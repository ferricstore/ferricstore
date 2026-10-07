defmodule FerricstoreBench.WriteTimelineProfiler do
  @moduledoc false
  use GenServer

  alias FerricStore.Impl
  alias Ferricstore.Raft.{CommandStamp, StateMachine, WARaftBackend}
  alias Ferricstore.Bitcask.NIF
  @client {Impl, :hset, 3}
  @submit {WARaftBackend, :commit_safely, 3}
  @wal {:ferricstore_waraft_spike_segment_log, :append, 4}
  @apply {StateMachine, :apply_waraft_segment_command, 4}
  @latches [
    {Ferricstore.Store.Promotion, :await_compaction_latch, 2},
    {Ferricstore.Store.Promotion, :acquire_compaction_latch_for_apply, 2},
    {Ferricstore.Store.Promotion, :acquire_compaction_latch, 2}
  ]
  @io [
        {NIF, :v2_append_record, 4},
        {NIF, :v2_append_batch, 2},
        {NIF, :v2_fsync, 1},
        {NIF, :v2_fsync_dir, 1},
        {NIF, :fs_atomic_replace_nofollow, 3},
        {:ferricstore_waraft_spike_segment_log, :sync_segment_file, 2}
      ] ++ @latches
  @overhead [
    {:ferricstore_waraft_spike_segment_log, :validate_segment_log_dir, 1},
    {:ferricstore_waraft_spike_segment_log, :register_offset_entries, 1},
    {:ferricstore_waraft_spike_segment_log, :update_latest_config_from_records, 2},
    {:ferricstore_waraft_spike_segment_log, :append_memory_stats, 3},
    {:ferricstore_waraft_spike_segment_log, :enforce_ets_memory_limit, 2},
    {:filelib, :ensure_dir, 1},
    {:file, :write, 2},
    {:file, :read_file_info, 1},
    {:file, :read_link_info, 1},
    {:file, :read_link_info, 2}
  ]
  @mfas [@client, @submit, @wal, @apply] ++ @io ++ @overhead
  @max_writes 100_000
  @max_io 80_000

  def start(ctx), do: GenServer.start(__MODULE__, ctx, name: __MODULE__)

  def finish do
    :erlang.trace(:all, false, [:call, :set_on_spawn])
    for mfa <- @mfas, do: :erlang.trace_pattern(mfa, false, [:local])
    # A final mailbox barrier follows disabling call tracing in this owned VM.
    Process.sleep(50)
    report = GenServer.call(__MODULE__, :snapshot, 30_000)
    GenServer.stop(__MODULE__)
    report
  end

  @impl true
  def init(ctx) do
    specs =
      [
        {@client, [:_, :"$1", :"$2"], {{:"$1", :"$2"}}},
        {@submit, [:_, :_, :"$1"], :"$1"},
        {@wal, [:_, :"$1", :_, :_], :"$1"},
        {@apply, [:"$1", :_, :_, :_], :"$1"}
      ] ++
        Enum.map(@io ++ @overhead, fn mfa ->
          {_, _, arity} = mfa
          {mfa, [:"$1" | List.duplicate(:_, arity - 1)], :"$1"}
        end)

    for {mfa, args, message} <- specs do
      :erlang.trace_pattern(mfa, [{args, [], [{:message, message}, {:return_trace}]}], [:local])
    end

    :erlang.trace(:all, true, [
      :call,
      :arity,
      :monotonic_timestamp,
      :set_on_spawn,
      {:tracer, self()}
    ])

    Process.send_after(self(), :sample, 10)

    {:ok,
     %{
       ctx: ctx,
       writes: %{},
       refs: %{},
       calls: %{},
       applying: %{},
       wal_io: %{},
       io: [],
       io_count: 0,
       dropped_writes: 0,
       dropped_io: 0,
       samples: []
     }}
  end

  @impl true
  def handle_info({:trace_ts, pid, :call, mfa, message, at}, state) do
    at = us(at)

    ids =
      case {mfa, message} do
        {@client, {key, fields}} when is_map(fields) ->
          for {field, value} <- fields, id = id(key, field, value), id != nil, do: id

        {@submit, {_ref, command}} ->
          ids(command)

        {@apply, command} ->
          ids(command)

        _ ->
          []
      end

    record = %{at: at, ids: ids, message: payload(mfa, message)}
    state = %{state | calls: Map.update(state.calls, {pid, mfa}, [record], &[record | &1])}

    state =
      case {mfa, message} do
        {@client, _} ->
          Enum.reduce(ids, state, fn id, acc ->
            if map_size(acc.writes) < @max_writes do
              %{
                acc
                | writes:
                    Map.put(acc.writes, id, %{
                      id: id,
                      client: inspect(pid),
                      start: at,
                      shard: Ferricstore.Store.Router.shard_for(acc.ctx, elem(id, 0)),
                      io: [],
                      wal_io: []
                    })
              }
            else
              %{acc | dropped_writes: acc.dropped_writes + 1}
            end
          end)

        {@submit, {ref, _}} ->
          state |> Map.put(:refs, Map.put(state.refs, ref, ids)) |> put_times(ids, :submit, at)

        {@apply, _} ->
          state
          |> Map.put(:applying, Map.put(state.applying, pid, ids))
          |> put_times(ids, :apply_start, at)

        {@wal, _} ->
          %{state | wal_io: Map.put(state.wal_io, pid, [])}

        _ ->
          state
      end

    {:noreply, state}
  end

  def handle_info({:trace_ts, pid, :return_from, mfa, _result, at}, state) do
    case Map.get(state.calls, {pid, mfa}, []) do
      [record | rest] ->
        state = %{state | calls: Map.put(state.calls, {pid, mfa}, rest)}
        finished = us(at)

        state =
          cond do
            mfa == @client ->
              put_times(state, record.ids, :finish, finished)

            mfa == @apply ->
              state
              |> put_times(record.ids, :apply_finish, finished)
              |> Map.put(:applying, Map.delete(state.applying, pid))

            mfa == @wal ->
              refs = for {_term, {ref, _command}} <- record.message, do: ref
              ids = Enum.flat_map(refs, &Map.get(state.refs, &1, []))

              state
              |> put_times(ids, :wal_start, record.at)
              |> put_times(ids, :wal_finish, finished)
              |> then(fn acc ->
                Enum.reduce(ids, acc, fn id, current ->
                  update_write(current, id, &Map.put(&1, :wal_io, Map.get(state.wal_io, pid, [])))
                end)
              end)
              |> Map.put(:wal_io, Map.delete(state.wal_io, pid))

            mfa in @io or mfa in @overhead ->
              event = %{
                mfa: inspect(mfa),
                pid: inspect(pid),
                start: record.at,
                finish: finished,
                duration_us: finished - record.at,
                path: record.message,
                kind:
                  cond do
                    mfa in @latches -> :latch
                    mfa in @overhead -> :overhead
                    true -> :native
                  end
              }

              state =
                Enum.reduce(Map.get(state.applying, pid, []), state, fn id, acc ->
                  update_write(acc, id, fn row -> %{row | io: [event | row.io]} end)
                end)

              state =
                if Map.has_key?(state.wal_io, pid),
                  do: %{state | wal_io: Map.update!(state.wal_io, pid, &[event | &1])},
                  else: state

              if event.duration_us < 1_000 do
                state
              else
                if state.io_count < @max_io do
                  %{state | io: [event | state.io], io_count: state.io_count + 1}
                else
                  %{state | dropped_io: state.dropped_io + 1}
                end
              end

            true ->
              state
          end

        {:noreply, state}

      [] ->
        {:noreply, state}
    end
  end

  def handle_info(:sample, state) do
    actors =
      for name <- Process.registered(),
          String.contains?(Atom.to_string(name), ["file_server", "raft_server_", "raft_storage_"]),
          pid = Process.whereis(name),
          is_pid(pid),
          info = Process.info(pid, [:current_stacktrace, :message_queue_len]),
          info != nil do
        %{
          name: inspect(name),
          queue: info[:message_queue_len],
          stack: Enum.take(info[:current_stacktrace], 5) |> Enum.map(&inspect/1)
        }
      end

    sample = %{
      at_us: System.monotonic_time(:microsecond),
      actors: actors,
      run_queues: :erlang.statistics(:run_queue_lengths_all)
    }

    Process.send_after(self(), :sample, 10)
    {:noreply, %{state | samples: Enum.take([sample | state.samples], 200)}}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    complete = state.writes |> Map.values() |> Enum.filter(&complete?/1)
    timings = Enum.map(complete, &breakdown/1)
    slow = timings |> Enum.sort_by(& &1.total_us, :desc) |> Enum.take(64)

    report = %{
      writes_seen: map_size(state.writes),
      complete_writes: length(complete),
      dropped_writes: state.dropped_writes,
      dropped_io: state.dropped_io,
      histograms: histograms(timings),
      slow_writes: slow,
      slow_io: state.io |> Enum.sort_by(& &1.duration_us, :desc) |> Enum.take(128),
      recent_samples: Enum.reverse(state.samples),
      command_wrapping: false,
      diagnostic_only: true
    }

    {:reply, report, state}
  end

  defp payload(@wal, entries), do: entries
  defp payload(mfa, path) when mfa in @io and is_binary(path), do: path
  defp payload(_, _), do: nil
  defp us(at), do: System.convert_time_unit(at, :native, :microsecond)

  defp id(key, "client-" <> _ = field, <<version::unsigned-64, _::binary>>),
    do: {key, field, version}

  defp id(_, _, _), do: nil

  defp ids({:ttb, binary}) do
    case CommandStamp.decode_ttb(binary) do
      {:ok, {command, _metadata}} -> ids(command)
      _ -> []
    end
  end

  defp ids({command, %{hlc_ts: _}}), do: ids(command)
  defp ids({:batch, commands}), do: Enum.flat_map(commands, &ids/1)
  defp ids({:hset_single, key, field, value}), do: List.wrap(id(key, field, value))
  defp ids(_), do: []

  defp put_times(state, ids, key, at),
    do: Enum.reduce(ids, state, &update_write(&2, &1, fn row -> Map.put(row, key, at) end))

  defp update_write(state, id, fun) do
    case Map.fetch(state.writes, id) do
      {:ok, row} -> %{state | writes: Map.put(state.writes, id, fun.(row))}
      :error -> state
    end
  end

  defp complete?(row),
    do:
      Enum.all?(
        [:start, :submit, :wal_start, :wal_finish, :apply_start, :apply_finish, :finish],
        &Map.has_key?(row, &1)
      )

  defp breakdown(row) do
    {key, field, version} = row.id
    latch = Enum.filter(row.io, &(&1.kind == :latch))
    native = Enum.filter(row.io, &(&1.kind == :native))

    %{
      key: key,
      field: field,
      version: version,
      shard: row.shard,
      start_us: row.start,
      total_us: row.finish - row.start,
      admission_us: row.submit - row.start,
      wal_queue_us: row.wal_start - row.submit,
      wal_wall_us: row.wal_finish - row.wal_start,
      apply_queue_us: row.apply_start - row.wal_finish,
      apply_wall_us: row.apply_finish - row.apply_start,
      acknowledgement_tail_us: row.finish - row.apply_finish,
      native_apply_us: interval_union(native),
      latch_us: interval_union(latch),
      wal_native_us: interval_union(Enum.filter(row.wal_io, &(&1.kind == :native))),
      wal_io: Enum.reverse(row.wal_io),
      io: Enum.reverse(row.io)
    }
  end

  defp interval_union(events) do
    {total, span} =
      events
      |> Enum.map(&{&1.start, &1.finish})
      |> Enum.sort()
      |> Enum.reduce({0, nil}, fn
        interval, {total, nil} -> {total, interval}
        {a, b}, {total, {start, finish}} when a <= finish -> {total, {start, max(b, finish)}}
        interval, {total, {start, finish}} -> {total + finish - start, interval}
      end)

    case span do
      nil -> total
      {a, b} -> total + b - a
    end
  end

  defp histograms(rows) do
    for key <- [
          :total_us,
          :admission_us,
          :wal_queue_us,
          :wal_wall_us,
          :wal_native_us,
          :apply_queue_us,
          :apply_wall_us,
          :acknowledgement_tail_us,
          :native_apply_us,
          :latch_us
        ],
        into: %{} do
      values = Enum.map(rows, &Map.fetch!(&1, key)) |> Enum.sort()

      {key,
       %{
         count: length(values),
         p50: percentile(values, 0.5),
         p95: percentile(values, 0.95),
         p99: percentile(values, 0.99),
         max: List.last(values)
       }}
    end
  end

  defp percentile([], _), do: nil
  defp percentile(values, q), do: Enum.at(values, ceil(length(values) * q) - 1)
end
