defmodule Ferricstore.Flow.Transfer do
  @moduledoc """
  Copies a store's Flow data through a file.

  `export/3` writes every run of a store (its record, values, history, signal
  idempotency and named-value link rows), its standalone shared values and its
  type policies. `restore/3` loads such a file into a store that holds no Flow
  data, writing keys in that store's data format (see `Ferricstore.DataFormat`).
  A data format 1 cluster moves to format 2 this way: export from it, then
  restore into a new cluster.

  Restored runs keep their ids, types, states, versions, attempts, leases,
  timestamps, values and history. Value refs inside records and history are
  rewritten for the target format; the shard-local sequence numbers that
  identify a run (`incarnation`, `state_enter_seq`) are assigned afresh by the
  target shard; policy generations start over. Runs are restored oldest state
  entry first, so FIFO lanes keep their order.

  Plain (non-Flow) keys are not exported. An export refuses a store holding Flow
  data it cannot carry (for example governance state or a policy migration in
  progress) instead of leaving it behind. Stop writers first: a run changed
  while the export reads it can be captured mid-change.
  """

  alias Ferricstore.DataDir
  alias Ferricstore.DataFormat
  alias Ferricstore.Flow.{Codec, HistoryRead, Keys, LMDB, Policy, ValueStore}
  alias Ferricstore.Flow.HistoryProjector.KeyCodec
  alias Ferricstore.Flow.Transfer.Retag
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @magic "FERRICSTORE FLOW EXPORT\n"
  @version 1
  @record_batch 256
  @lmdb_page 1_000
  @max_frame_bytes 1_073_741_824
  @unsupported_samples 5
  @flush_timeout_ms 120_000

  # Run-scoped rows a restore rebuilds from the run's record.
  @derived_run_families ~w(s h rtg rtm tc svr i)
  # Global rows rebuilt from policies and restored runs, or progress markers.
  @derived_global_families ~w(td svc svo svb svbp pcb policy-attribute policy-attribute-member
                              policy-attribute-revision policy-attribute-repair)

  @type counts :: %{
          flows: non_neg_integer(),
          shared_values: non_neg_integer(),
          policies: non_neg_integer()
        }

  @spec export(FerricStore.Instance.t(), Path.t(), keyword()) ::
          {:ok, counts()} | {:error, term()}
  def export(ctx, path, _opts \\ []) do
    with :ok <- flush_lmdb(ctx),
         {:ok, scan} <- scan(ctx),
         :ok <- refuse_unsupported(scan) do
      partial = path <> ".partial"

      case File.open(partial, [:write, :binary], &write_export(ctx, &1, scan)) do
        {:ok, {:ok, counts}} ->
          :ok = Ferricstore.FS.rename(partial, path)
          {:ok, counts}

        {:ok, {:error, _reason} = error} ->
          _ = Ferricstore.FS.rm(partial)
          error

        {:error, reason} ->
          {:error, {:export_file, reason}}
      end
    end
  end

  @spec restore(FerricStore.Instance.t(), Path.t(), keyword()) ::
          {:ok, counts()} | {:error, term()}
  def restore(ctx, path, _opts \\ []) do
    with {:ok, header} <- verify(path),
         from = header.data_format,
         to = DataFormat.current(),
         :ok <- restorable?(from, to),
         :ok <- require_empty(ctx) do
      case File.open(path, [:read, :binary], &restore_frames(ctx, &1, from, to)) do
        {:ok, result} -> result
        {:error, reason} -> {:error, {:export_file, reason}}
      end
    end
  end

  ## Export

  defp flush_lmdb(ctx) do
    case Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, @flush_timeout_ms) do
      :ok -> :ok
      {:error, reason} -> {:error, {:lmdb_flush_failed, reason}}
    end
  end

  defp write_export(ctx, io, scan) do
    header = %{
      version: @version,
      data_format: DataFormat.current(),
      exported_at_ms: System.system_time(:millisecond)
    }

    with :ok <- IO.binwrite(io, @magic),
         {:ok, hash} <- write_frame(io, :crypto.hash_init(:sha256), {:header, header}),
         {:ok, hash, policies} <- write_policies(ctx, io, hash, scan.policies),
         {:ok, hash, shared} <- write_shared_values(ctx, io, hash, scan.shared),
         {:ok, order} <- run_order(ctx, scan.flows),
         {:ok, hash, flows} <- write_runs(ctx, io, hash, order, scan) do
      counts = %{flows: flows, shared_values: shared, policies: policies}
      trailer = Map.put(counts, :sha256, :crypto.hash_final(hash))

      {:ok, _hash} = write_frame(io, nil, {:trailer, trailer})
      {:ok, counts}
    end
  end

  defp write_policies(ctx, io, hash, types) do
    types
    |> Enum.sort()
    |> Enum.reduce_while({:ok, hash, 0}, fn type, {:ok, hash, count} ->
      case Policy.raw_entry(ctx, type) do
        # Runs of a type without a policy leave an absent-policy row behind.
        {:ok, {_generation, nil}} ->
          {:cont, {:ok, hash, count}}

        {:ok, {_generation, policy}} when is_map(policy) ->
          {:ok, hash} = write_frame(io, hash, {:policy, type, policy})
          {:cont, {:ok, hash, count + 1}}

        {:error, reason} ->
          {:halt, {:error, {:policy_export_failed, type, reason}}}
      end
    end)
  end

  defp write_shared_values(ctx, io, hash, shared) do
    keys = shared |> Map.keys() |> Enum.sort()

    keys
    |> Enum.chunk_every(@record_batch)
    |> Enum.reduce_while({:ok, hash, 0}, fn chunk, {:ok, hash, count} ->
      chunk
      |> Enum.zip(ValueStore.raw_mget_any(ctx, chunk))
      |> Enum.reduce_while({:ok, hash, count}, fn
        {_key, nil}, acc ->
          {:cont, acc}

        {key, value}, {:ok, hash, count} when is_binary(value) ->
          {:ok, hash} =
            write_frame(io, hash, {:shared_value, key, value, Map.fetch!(shared, key)})

          {:cont, {:ok, hash, count + 1}}
      end)
      |> case do
        {:ok, _hash, _count} = ok -> {:cont, ok}
        error -> {:halt, error}
      end
    end)
  end

  # Oldest state entry first: restore re-inserts FIFO lanes in this order (a
  # partition's runs share one shard in both stores).
  defp run_order(ctx, state_keys) do
    state_keys
    |> Enum.sort()
    |> Enum.chunk_every(@record_batch)
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc} ->
      case read_records(ctx, chunk) do
        {:ok, records} ->
          entries =
            for {state_key, record} <- records,
                do:
                  {Map.get(record, :state_enter_seq) || Map.get(record, :incarnation) || 0,
                   state_key}

          {:cont, {:ok, entries ++ acc}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, entries |> Enum.sort() |> Enum.map(&elem(&1, 1))}
      error -> error
    end
  end

  defp write_runs(ctx, io, hash, order, scan) do
    order
    |> Enum.chunk_every(@record_batch)
    |> Enum.reduce_while({:ok, hash, 0}, fn chunk, {:ok, hash, count} ->
      with {:ok, records} <- read_records(ctx, chunk) do
        Enum.reduce_while(records, {:ok, hash, count}, fn {state_key, record},
                                                          {:ok, hash, count} ->
          with {:ok, bundle} <- bundle(ctx, state_key, record, scan),
               {:ok, hash} <- write_frame(io, hash, {:flow, bundle}) do
            {:cont, {:ok, hash, count + 1}}
          else
            error -> {:halt, error}
          end
        end)
      end
      |> case do
        {:ok, _hash, _count} = ok -> {:cont, ok}
        error -> {:halt, error}
      end
    end)
  end

  # Runs that expired since the scan are skipped.
  defp read_records(ctx, state_keys) do
    ctx
    |> Router.flow_batch_get_state_keys_with_status(state_keys)
    |> Enum.zip(state_keys)
    |> Enum.reduce_while({:ok, []}, fn
      {nil, _state_key}, acc ->
        {:cont, acc}

      {value, state_key}, {:ok, acc} when is_binary(value) ->
        {:cont, {:ok, [{state_key, Codec.decode_record(value)} | acc]}}

      {other, state_key}, _acc ->
        {:halt, {:error, {:run_read_failed, state_key, other}}}
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      error -> error
    end
  end

  defp bundle(ctx, state_key, %{id: id} = record, scan) do
    partition_key = Map.get(record, :partition_key)

    with {:ok, raw_history} <-
           HistoryRead.raw_entries(ctx, id, partition_key, Keys.history_key(id, partition_key)),
         {:ok, history} <- decode_history(state_key, raw_history) do
      refs = owned_value_refs(record, history)
      aux = Map.get(scan.signals, state_key, []) ++ Map.get(scan.links, state_key, [])

      {:ok,
       %{
         state_key: state_key,
         record: record,
         values: present_rows(refs, ValueStore.raw_mget(ctx, refs)),
         history: history,
         aux:
           for(
             {{kind, key}, value} <-
               Enum.zip(aux, ValueStore.raw_mget_any(ctx, Enum.map(aux, &elem(&1, 1)))),
             is_binary(value),
             do: {kind, key, value}
           )
       }}
    else
      {:error, reason} -> {:error, {:run_export_failed, state_key, reason}}
    end
  end

  defp decode_history(state_key, raw_entries) do
    Enum.reduce_while(raw_entries, {:ok, []}, fn {event_id, value}, {:ok, acc} ->
      case Codec.decode_history_snapshot(value) do
        {:ok, snapshot} -> {:cont, {:ok, [{event_id, snapshot} | acc]}}
        :error -> {:halt, {:error, {:invalid_history_entry, state_key, event_id}}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  # Value rows the run owns: every payload/result/error/named value its record
  # or history refers to. Standalone shared values travel separately.
  defp owned_value_refs(record, history) do
    record_refs =
      Enum.map([:payload_ref, :result_ref, :error_ref], &Map.get(record, &1)) ++
        for({_name, %{ref: ref}} <- Map.get(record, :value_refs) || %{}, do: ref)

    history_refs =
      Enum.flat_map(history, fn {_event_id, snapshot} ->
        [elem(snapshot, 14), elem(snapshot, 18), elem(snapshot, 19)] ++
          history_meta_refs(elem(snapshot, 21))
      end)

    (record_refs ++ history_refs)
    |> Enum.filter(&owned_value_ref?/1)
    |> Enum.uniq()
  end

  defp history_meta_refs(pairs) do
    Enum.flat_map(pairs, fn
      {"value_refs", json} ->
        case Jason.decode(json) do
          {:ok, refs} when is_map(refs) -> for {_name, %{"ref" => ref}} <- refs, do: ref
          _invalid -> []
        end

      {_name, value} ->
        [value]
    end)
  end

  defp owned_value_ref?(ref) when is_binary(ref) do
    Keys.value_key?(ref) and
      match?([_tag, <<kind, ?:, _rest::binary>>] when kind != ?s, :binary.split(ref, "}:v:"))
  end

  defp owned_value_ref?(_ref), do: false

  defp present_rows(keys, values) do
    for {key, value} <- Enum.zip(keys, values), is_binary(value), do: {key, value}
  end

  ## Scan

  defp scan(ctx) do
    acc = %{
      flows: MapSet.new(),
      signals: %{},
      links: %{},
      shared: %{},
      policies: MapSet.new(),
      unsupported: []
    }

    now_ms = System.system_time(:millisecond)

    Enum.reduce_while(0..(ctx.shard_count - 1)//1, {:ok, acc}, fn shard, {:ok, acc} ->
      acc = scan_keydir(ctx, shard, acc, now_ms)

      case scan_lmdb(ctx, shard, acc, now_ms) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, %{acc | flows: MapSet.to_list(acc.flows)}}
      error -> error
    end
  end

  defp scan_keydir(ctx, shard, acc, now_ms) do
    :ets.foldl(
      fn row, acc ->
        case {elem(row, 0), elem(row, 2)} do
          {"f:" <> _rest = key, expire_at_ms} ->
            if live?(expire_at_ms, now_ms), do: classify(acc, key, expire_at_ms), else: acc

          _other ->
            acc
        end
      end,
      acc,
      elem(ctx.keydir_refs, shard)
    )
  end

  defp scan_lmdb(ctx, shard, acc, now_ms) do
    ctx.data_dir
    |> DataDir.shard_data_path(shard)
    |> LMDB.path()
    |> scan_lmdb_pages(nil, acc, now_ms)
  end

  defp scan_lmdb_pages(path, after_key, acc, now_ms) do
    page =
      if after_key,
        do: LMDB.prefix_entries_after(path, "f:", after_key, @lmdb_page),
        else: LMDB.prefix_entries(path, "f:", @lmdb_page)

    case page do
      {:ok, []} ->
        {:ok, acc}

      {:ok, entries} ->
        acc =
          Enum.reduce(entries, acc, fn {key, blob}, acc ->
            expire_at_ms = lmdb_expire_at(blob)
            if live?(expire_at_ms, now_ms), do: classify(acc, key, expire_at_ms), else: acc
          end)

        scan_lmdb_pages(path, elem(List.last(entries), 0), acc, now_ms)

      {:error, reason} ->
        {:error, {:lmdb_scan_failed, path, reason}}
    end
  end

  defp lmdb_expire_at(blob) do
    case TermCodec.decode(blob) do
      {:ok, {expire_at_ms, _value}} when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        expire_at_ms

      _other ->
        0
    end
  end

  defp live?(expire_at_ms, now_ms) when is_integer(expire_at_ms) and expire_at_ms > 0,
    do: expire_at_ms > now_ms

  defp live?(_expire_at_ms, _now_ms), do: true

  defp classify(acc, "f:{" <> rest = key, expire_at_ms) do
    case :binary.split(rest, "}:") do
      [tag, tail] -> classify(acc, tag_kind(tag), tag, tail, key, expire_at_ms)
      _other -> unsupported(acc, key)
    end
  end

  defp classify(acc, key, _expire_at_ms), do: unsupported(acc, key)

  defp classify(acc, :run, tag, tail, key, expire_at_ms) do
    case family(tail) do
      "r" ->
        case Keys.state_key_from_registry_key(key) do
          {:ok, state_key} -> %{acc | flows: MapSet.put(acc.flows, state_key)}
          :error -> unsupported(acc, key)
        end

      "sig" ->
        add_owned_row(acc, :signals, :signal, tag, tail, key)

      "svl" ->
        add_owned_row(acc, :links, :link, tag, tail, key)

      "v" ->
        if String.starts_with?(tail, "v:s:"),
          do: %{acc | shared: Map.put(acc.shared, key, expire_at_ms)},
          else: acc

      family when family in @derived_run_families ->
        acc

      _other ->
        unsupported(acc, key)
    end
  end

  defp classify(acc, :global, _tag, tail, key, _expire_at_ms) do
    case family(tail) do
      "policy" ->
        "policy:" <> type = tail
        %{acc | policies: MapSet.put(acc.policies, type)}

      family when family in @derived_global_families ->
        acc

      _other ->
        unsupported(acc, key)
    end
  end

  defp classify(acc, :server, _tag, _tail, _key, _expire_at_ms), do: acc
  defp classify(acc, :other, _tag, _tail, key, _expire_at_ms), do: unsupported(acc, key)

  defp tag_kind("f"), do: :global
  defp tag_kind("fa:" <> _bucket), do: :run
  defp tag_kind("f:" <> _digest), do: :run
  defp tag_kind("__server__"), do: :server
  defp tag_kind(_tag), do: :other

  defp family(tail) do
    case :binary.split(tail, ":") do
      [family, _rest] -> family
      [family] -> family
    end
  end

  # Signal and link rows name their run by an encoded id component.
  defp add_owned_row(acc, field, kind, tag, tail, key) do
    with [_family, rest] <- :binary.split(tail, ":"),
         [encoded_id, _rest] <- :binary.split(rest, ":"),
         {:ok, id} when id != "" <- Base.url_decode64(encoded_id, padding: false) do
      state_key = "f:{" <> tag <> "}:s:" <> id

      Map.update!(
        acc,
        field,
        &Map.update(&1, state_key, [{kind, key}], fn rows -> [{kind, key} | rows] end)
      )
    else
      _invalid -> unsupported(acc, key)
    end
  end

  defp unsupported(%{unsupported: keys} = acc, key) when length(keys) < @unsupported_samples,
    do: %{acc | unsupported: [key | keys]}

  defp unsupported(acc, _key), do: acc

  defp refuse_unsupported(%{unsupported: []}), do: :ok

  defp refuse_unsupported(%{unsupported: keys}),
    do: {:error, {:unsupported_flow_data, Enum.reverse(keys)}}

  ## Restore

  defp restorable?(format, format), do: :ok
  defp restorable?(1, 2), do: :ok
  defp restorable?(from, to), do: {:error, {:cannot_restore_data_format, from, to}}

  defp require_empty(ctx) do
    with :ok <- flush_lmdb(ctx),
         {:ok, scan} <- scan(ctx) do
      present =
        scan.flows ++
          Map.keys(scan.shared) ++
          Enum.map(MapSet.to_list(scan.policies), &Keys.policy_key/1) ++ scan.unsupported

      case present do
        [] -> :ok
        [key | _rest] -> {:error, {:target_not_empty, key}}
      end
    end
  end

  defp restore_frames(ctx, io, from, to) do
    with :ok <- read_magic(io),
         {:ok, {:header, _header}, _frame} <- read_frame(io) do
      restore_frames(ctx, io, from, to, %{flows: 0, shared_values: 0, policies: 0})
    end
  end

  defp restore_frames(ctx, io, from, to, counts) do
    case read_frame(io) do
      {:ok, {:trailer, _trailer}, _frame} ->
        {:ok, counts}

      {:ok, {:policy, type, policy}, _frame} ->
        with :ok <- restore_policy(ctx, type, policy),
             do: restore_frames(ctx, io, from, to, Map.update!(counts, :policies, &(&1 + 1)))

      {:ok, {:shared_value, key, value, expire_at_ms}, _frame} ->
        with {:ok, key} <- Retag.key(key, from, to),
             :ok <-
               Router.flow_restore(ctx, key, %{
                 restore: :shared_value,
                 key: key,
                 value: value,
                 expire_at_ms: expire_at_ms
               }),
             do: restore_frames(ctx, io, from, to, Map.update!(counts, :shared_values, &(&1 + 1)))

      {:ok, {:flow, bundle}, _frame} ->
        with :ok <- restore_run(ctx, bundle, from, to),
             do: restore_frames(ctx, io, from, to, Map.update!(counts, :flows, &(&1 + 1)))

      {:ok, other, _frame} ->
        {:error, {:unexpected_export_frame, other}}

      error ->
        error
    end
  end

  defp restore_policy(ctx, type, policy) do
    opts = policy |> Map.delete(:type) |> Map.to_list()

    case Policy.set(ctx, type, opts ++ [replace: true]) do
      {:ok, _policy} -> :ok
      {:error, reason} -> {:error, {:policy_restore_failed, type, reason}}
    end
  end

  defp restore_run(ctx, %{state_key: source_key, record: record} = bundle, from, to) do
    with {:ok, record} <- Retag.record(record, from, to),
         {:ok, state_key} <- Retag.key(source_key, from, to),
         :ok <- same_state_key(state_key, record),
         {:ok, values} <- retag_rows(bundle.values, from, to),
         {:ok, history} <- restore_history(bundle.history, from, to),
         {:ok, aux} <- retag_aux(bundle.aux, from, to) do
      case Router.flow_restore(ctx, state_key, %{
             restore: :flow,
             type: Map.fetch!(record, :type),
             partition_key: Map.get(record, :partition_key),
             record: record,
             values: values,
             history: history,
             aux: aux
           }) do
        :ok -> :ok
        {:error, reason} -> {:error, {:run_restore_failed, source_key, reason}}
        other -> {:error, {:run_restore_failed, source_key, other}}
      end
    end
  end

  defp same_state_key(state_key, %{id: id} = record) do
    if Keys.state_key(id, Map.get(record, :partition_key)) == state_key,
      do: :ok,
      else: {:error, {:state_key_mismatch, state_key}}
  end

  defp retag_rows(rows, from, to) do
    collect(rows, fn {key, value} ->
      with {:ok, key} <- Retag.key(key, from, to), do: {:ok, {key, value}}
    end)
  end

  defp retag_aux(rows, from, to) do
    collect(rows, fn
      {:link, key, ref} ->
        with {:ok, key} <- Retag.key(key, from, to),
             {:ok, ref} <- Retag.key(ref, from, to),
             do: {:ok, {:link, key, ref}}

      {kind, key, value} ->
        with {:ok, key} <- Retag.key(key, from, to), do: {:ok, {kind, key, value}}
    end)
  end

  defp restore_history(entries, from, to) do
    collect(entries, fn {event_id, snapshot} ->
      with {:ok, snapshot} <- Retag.history_snapshot(snapshot, from, to),
           {:ok, event_ms} <- KeyCodec.parse_event_ms(event_id),
           {:ok, version} <- KeyCodec.parse_event_version(event_id) do
        {:ok, {event_id, event_ms, version, Codec.encode_history_snapshot(snapshot)}}
      else
        {:error, _reason} = error -> error
        _invalid -> {:error, {:invalid_history_event_id, event_id}}
      end
    end)
  end

  defp collect(items, fun) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      case fun.(item) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  ## File

  defp write_frame(io, hash, term) do
    bin = TermCodec.encode(term)
    frame = <<byte_size(bin)::unsigned-big-32, bin::binary>>

    :ok = IO.binwrite(io, frame)
    {:ok, if(hash, do: :crypto.hash_update(hash, frame), else: nil)}
  end

  defp read_magic(io) do
    case IO.binread(io, byte_size(@magic)) do
      @magic -> :ok
      _other -> {:error, :not_a_flow_export}
    end
  end

  defp read_frame(io) do
    case IO.binread(io, 4) do
      <<size::unsigned-big-32>> when size <= @max_frame_bytes ->
        case IO.binread(io, size) do
          bin when is_binary(bin) and byte_size(bin) == size ->
            case TermCodec.decode(bin) do
              {:ok, term} -> {:ok, term, <<size::unsigned-big-32, bin::binary>>}
              _invalid -> {:error, :corrupt_flow_export}
            end

          _short ->
            {:error, :truncated_flow_export}
        end

      :eof ->
        {:error, :truncated_flow_export}

      _invalid ->
        {:error, :corrupt_flow_export}
    end
  end

  # Checks the whole file before a restore writes anything.
  defp verify(path) do
    case File.open(path, [:read, :binary], &verify_frames/1) do
      {:ok, result} -> result
      {:error, reason} -> {:error, {:export_file, reason}}
    end
  end

  defp verify_frames(io) do
    with :ok <- read_magic(io),
         {:ok, {:header, %{version: @version, data_format: format} = header}, frame}
         when format in [1, 2] <- read_frame(io) do
      verify_frames(io, header, :crypto.hash_update(:crypto.hash_init(:sha256), frame), %{})
    else
      {:ok, _other, _frame} -> {:error, :unsupported_flow_export}
      {:error, _reason} = error -> error
    end
  end

  defp verify_frames(io, header, hash, counts) do
    case read_frame(io) do
      {:ok, {:trailer, %{sha256: sha256} = trailer}, _frame} ->
        expected = Map.take(trailer, [:flows, :shared_values, :policies])

        cond do
          :crypto.hash_final(hash) != sha256 ->
            {:error, :corrupt_flow_export}

          Map.merge(%{flows: 0, shared_values: 0, policies: 0}, counts) != expected ->
            {:error, :corrupt_flow_export}

          IO.binread(io, 1) != :eof ->
            {:error, :corrupt_flow_export}

          true ->
            {:ok, header}
        end

      {:ok, term, frame} ->
        verify_frames(io, header, :crypto.hash_update(hash, frame), count_frame(counts, term))

      error ->
        error
    end
  end

  defp count_frame(counts, {:flow, _bundle}), do: Map.update(counts, :flows, 1, &(&1 + 1))

  defp count_frame(counts, {:shared_value, _key, _value, _expire_at_ms}),
    do: Map.update(counts, :shared_values, 1, &(&1 + 1))

  defp count_frame(counts, {:policy, _type, _policy}),
    do: Map.update(counts, :policies, 1, &(&1 + 1))

  defp count_frame(counts, _other), do: Map.update(counts, :unknown, 1, &(&1 + 1))
end
