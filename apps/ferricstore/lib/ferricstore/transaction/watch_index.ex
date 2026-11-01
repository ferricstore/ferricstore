defmodule Ferricstore.Transaction.WatchIndex do
  @moduledoc false

  # Replicated write versions for watched keys.
  #
  # Every replica applies a watched EXEC and must reach the leader's decision,
  # so a WATCH token may only depend on what the log wrote. Storage locations
  # and promoted-collection revisions do not qualify: a node relocates values
  # when it trims its log and restarts its revision counters, each on its own
  # schedule. From feature level 5, WATCH registers a row per watched key in
  # the shard's keydir, and each applied entry raises the rows of the keys it
  # writes. A row holds the key's version, 2 * index for the entry that
  # registered it and 2 * index + 1 for the last entry that wrote the key, so
  # an entry that both watches and writes a key always reads as a write.

  alias Ferricstore.Raft.WARaftSegmentReader.CommandValues
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.CompoundKey

  @prefix ServerCatalog.root_prefix() <> "watch:"
  @row_prefix @prefix <> "k:"
  @in_use_key @prefix <> "in-use"
  @row_ttl_ms 60 * 60 * 1_000
  @max_version 0xFFFF_FFFF_FFFF_FFFF

  @single_key_tags ~w(
    put put_blob_ref set set_blob_ref append append_blob_ref cas cas_blob_ref delete
    getdel getex getset getset_blob_ref incr incr_float setbit setrange setrange_blob_ref
    hincrby hincrbyfloat hset_single lpush_single rpush_single sadd_single srem_single
    spop list_op stream_mutate zadd_single zincrby zpop zrem_single lock unlock extend
    ratelimit_add fetch_or_compute_publish fetch_or_compute_publish_blob_ref
    fetch_or_compute_release stream_append stream_append_blob_ref stream_append_many_auto
    pfadd pfmerge
    prob_restore bloom_add bloom_create bloom_madd cms_create cms_incrby cms_merge
    cuckoo_add cuckoo_addnx cuckoo_create cuckoo_del topk_add topk_create topk_incrby
    compound_put compound_delete compound_put_blob_ref compound_batch_put
    compound_batch_delete compound_blob_batch_put compound_command compound_type_claim
    compound_delete_prefix
  )a

  @entry_list_tags ~w(put_batch put_blob_batch mset mset_blob_batch msetnx msetnx_blob_batch
                      zadd_many_single)a

  # Expiry deletes only keys already expired, which a WATCH sees through the
  # key's liveness instead; the others write no client key (`prob_file_repair`
  # only gives a node back its copy of a file as every replica has it).
  @no_client_key_tags ~w(
    expire_if_batch watch_token watch_tokens cross_shard_tx
    ferricstore_apply_context_barrier server_catalog_mutate server_catalog_replace
    prob_file_repair
  )a

  @every_key_tags ~w(flush_shard delete_prefix clear_key_locks)a

  @doc false
  def row_key(key) when is_binary(key), do: @row_prefix <> key

  @doc false
  def row_prefix, do: @row_prefix

  @doc false
  def in_use_key, do: @in_use_key

  @doc false
  def row_ttl_ms, do: @row_ttl_ms

  @doc false
  def registration_version(index) when is_integer(index) and index >= 0, do: index * 2

  @doc false
  def write_version(index) when is_integer(index) and index >= 0, do: index * 2 + 1

  @doc false
  def encode_version(version)
      when is_integer(version) and version >= 0 and version <= @max_version,
      do: <<version::unsigned-big-64>>

  @doc false
  def decode_version(<<version::unsigned-big-64>>), do: {:ok, version}
  def decode_version(_value), do: :error

  @doc false
  # Every apply command tag this module classifies; a test keeps it in step
  # with the state machine.
  def classified_tags do
    @single_key_tags ++
      @entry_list_tags ++
      @no_client_key_tags ++
      @every_key_tags ++
      [
        :batch,
        :delete_batch,
        :fetch_or_compute_fail,
        :fetch_or_compute_lock,
        :key_lifecycle,
        :list_op_lmove,
        :stream_append_grouped_auto,
        :tx_execute
      ]
  end

  @doc """
  The client keys a replicated command can write: `:none`, `{:keys, keys}`, or
  `:all` when the keys are only known as it applies.
  """
  def written(command), do: command |> CommandValues.decode_replay_command() |> classify()

  @doc """
  `written/1` for a command that applied with `result`: a command whose
  result shows it changed nothing (SET NX of an existing key, SADD of an
  existing member, a failed CAS) writes no key, as Redis leaves watchers
  alone then. Results that do not tell count as writes.
  """
  def written(command, result) do
    command
    |> CommandValues.decode_replay_command()
    |> classify_applied(result)
  end

  defp classify_applied({:batch, commands} = batch, result) when is_list(commands) do
    case batch_results(result) do
      results when is_list(results) and length(results) == length(commands) ->
        commands
        |> Enum.zip(results)
        |> Enum.reduce_while({:keys, []}, fn {command, result}, {:keys, acc} ->
          case written(command, result) do
            :none -> {:cont, {:keys, acc}}
            {:keys, keys} -> {:cont, {:keys, keys ++ acc}}
            :all -> {:halt, :all}
          end
        end)
        |> unique()

      _unaligned ->
        classify(batch)
    end
  end

  defp classify_applied(command, result) do
    if unchanged?(command, result), do: :none, else: classify(command)
  end

  defp batch_results({:ok, results}) when is_list(results), do: results
  defp batch_results({results, count}) when is_list(results) and is_integer(count), do: results
  defp batch_results(_result), do: nil

  # SET with NX or XX answers nil when it skipped the write, and with GET the
  # old value instead: present for a skipped NX, missing for a skipped XX.
  defp unchanged?({tag, _key, _value, _expire_at_ms, opts}, result)
       when tag in [:set, :set_blob_ref] and is_map(opts) do
    case {Map.get(opts, :nx, false), Map.get(opts, :xx, false), Map.get(opts, :get, false)} do
      {true, _xx, false} -> is_nil(result)
      {_nx, true, false} -> is_nil(result)
      {true, _xx, true} -> is_binary(result)
      {_nx, true, true} -> is_nil(result)
      _unconditional -> false
    end
  end

  defp unchanged?({tag, _key, _expected, _value, _ttl_ms}, result)
       when tag in [:cas, :cas_blob_ref],
       do: result in [0, nil]

  defp unchanged?({:getdel, _key}, nil), do: true

  defp unchanged?({tag, _key, _member}, 0) when tag in [:sadd_single, :srem_single, :zrem_single],
    do: true

  defp unchanged?(_command, _result), do: false

  defp classify(command) when is_tuple(command) and tuple_size(command) >= 1 do
    tag = elem(command, 0)

    cond do
      flow_tag?(tag) -> :none
      tag in @no_client_key_tags -> :none
      tag in @every_key_tags -> :all
      tag in @single_key_tags -> single_key(command)
      tag in @entry_list_tags -> entry_list(command)
      true -> classify_shape(command)
    end
  end

  defp classify(_command), do: :all

  defp classify_shape({:batch, commands}) when is_list(commands) do
    Enum.reduce_while(commands, {:keys, []}, fn command, {:keys, acc} ->
      case written(command) do
        :none -> {:cont, {:keys, acc}}
        {:keys, keys} -> {:cont, {:keys, keys ++ acc}}
        :all -> {:halt, :all}
      end
    end)
    |> unique()
  end

  defp classify_shape({:delete_batch, keys}) when is_list(keys), do: storage_keys(keys)

  defp classify_shape({:key_lifecycle, {:copy, _source, destination, _replace?}})
       when is_binary(destination),
       do: {:keys, [destination]}

  defp classify_shape({:key_lifecycle, {_operation, source, destination}})
       when is_binary(source) and is_binary(destination),
       do: {:keys, [source, destination]}

  defp classify_shape({:list_op_lmove, source, destination, _from, _to})
       when is_binary(source) and is_binary(destination),
       do: {:keys, [source, destination]}

  # The outcome key holds the computed value the lock guards.
  defp classify_shape({:fetch_or_compute_lock, key, outcome_key, _owner, _expire_at_ms})
       when is_binary(key) and is_binary(outcome_key),
       do: {:keys, Enum.uniq([key, outcome_key])}

  defp classify_shape({:fetch_or_compute_fail, key, outcome_key, _error, _expire_at_ms, _owner})
       when is_binary(key) and is_binary(outcome_key),
       do: {:keys, Enum.uniq([key, outcome_key])}

  defp classify_shape({:stream_append_grouped_auto, groups, _count}) when is_list(groups),
    do: entry_list({:stream_append_grouped_auto, groups})

  defp classify_shape({:tx_execute, queue, namespace}), do: queue_keys(queue, namespace)
  defp classify_shape({:tx_execute, queue, namespace, _watched}), do: queue_keys(queue, namespace)
  defp classify_shape(_command), do: :all

  defp single_key(command) do
    case elem_or_nil(command, 1) do
      key when is_binary(key) -> {:keys, [CompoundKey.extract_redis_key(key)]}
      _missing -> :all
    end
  end

  defp entry_list({_tag, entries}) when is_list(entries) do
    entries
    |> Enum.reduce_while([], fn
      entry, acc when is_tuple(entry) and tuple_size(entry) > 0 and is_binary(elem(entry, 0)) ->
        {:cont, [elem(entry, 0) | acc]}

      _invalid, _acc ->
        {:halt, :invalid}
    end)
    |> case do
      :invalid -> :all
      keys -> keys |> Enum.reverse() |> storage_keys()
    end
  end

  defp entry_list(_command), do: :all

  defp storage_keys(keys) do
    if Enum.all?(keys, &is_binary/1),
      do: {:keys, keys |> Enum.map(&CompoundKey.extract_redis_key/1) |> Enum.uniq()},
      else: :all
  end

  defp queue_keys(queue, namespace) when is_list(queue) do
    Enum.reduce_while(queue, {:keys, []}, fn
      %{write_keys: keys}, {:keys, acc} when is_list(keys) ->
        if Enum.all?(keys, &is_binary/1),
          do: {:cont, {:keys, Enum.map(keys, &namespaced(&1, namespace)) ++ acc}},
          else: {:halt, :all}

      _entry, _acc ->
        {:halt, :all}
    end)
    |> unique()
  end

  defp queue_keys(_queue, _namespace), do: :all

  defp namespaced(key, namespace) when is_binary(namespace), do: namespace <> key
  defp namespaced(key, _namespace), do: key

  defp unique({:keys, keys}), do: {:keys, Enum.uniq(keys)}
  defp unique(other), do: other

  defp flow_tag?(tag) when is_atom(tag),
    do: match?("flow_" <> _rest, Atom.to_string(tag))

  defp flow_tag?(_tag), do: false

  defp elem_or_nil(tuple, index) when tuple_size(tuple) > index, do: elem(tuple, index)
  defp elem_or_nil(_tuple, _index), do: nil
end
