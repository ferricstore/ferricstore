%%% Copyright (c) FerricStore contributors.
%%%
%%% Local load driver for the isolated WARaft spike. Benchmarks should execute
%%% this on the elected leader node so the measured path is WARaft commit/apply,
%%% not per-batch cross-node RPC from the benchmark coordinator.

-module(ferricstore_waraft_spike_load).

-export([run/4, run/5, run_multi/5, run_multi/6, run_mixed/4, run_mixed/5]).

-define(TIMEOUT, 30000).

run(Total, Concurrency, Pipeline, DataSize) ->
    run(Total, Concurrency, Pipeline, DataSize, 0).

run(Total, Concurrency, Pipeline, DataSize, WarmupTotal)
    when Total >= 0,
         Concurrency > 0,
         Pipeline > 0,
         DataSize >= 0,
         WarmupTotal >= 0 ->
    Value = binary:copy(<<"x">>, DataSize),
    _ = run_workers(WarmupTotal, max(1, min(Concurrency, 32)), Pipeline, Value),
    StartedAt = erlang:monotonic_time(microsecond),
    case run_workers(Total, Concurrency, Pipeline, Value) of
        {ok, Ops} ->
            ElapsedUs = max(erlang:monotonic_time(microsecond) - StartedAt, 1),
            OpsPerSec = Ops * 1000000 / ElapsedUs,
            MbPerSec = OpsPerSec * DataSize / 1048576,
            {ok, #{
                ops => Ops,
                elapsed_us => ElapsedUs,
                ops_per_sec => OpsPerSec,
                mb_per_sec => MbPerSec
            }};
        Error ->
            Error
    end.

run_multi(Total, Concurrency, Pipeline, DataSize, PartitionCount) ->
    run_multi(Total, Concurrency, Pipeline, DataSize, 0, PartitionCount).

run_multi(Total, Concurrency, Pipeline, DataSize, WarmupTotal, PartitionCount)
    when Total >= 0,
         Concurrency > 0,
         Pipeline > 0,
         DataSize >= 0,
         WarmupTotal >= 0,
         PartitionCount > 0 ->
    Value = binary:copy(<<"x">>, DataSize),
    _ = run_multi_workers(WarmupTotal, max(1, min(Concurrency, 32)), Pipeline, Value, PartitionCount),
    StartedAt = erlang:monotonic_time(microsecond),
    case run_multi_workers(Total, Concurrency, Pipeline, Value, PartitionCount) of
        {ok, Ops} ->
            ElapsedUs = max(erlang:monotonic_time(microsecond) - StartedAt, 1),
            OpsPerSec = Ops * 1000000 / ElapsedUs,
            MbPerSec = OpsPerSec * DataSize / 1048576,
            {ok, #{
                ops => Ops,
                elapsed_us => ElapsedUs,
                ops_per_sec => OpsPerSec,
                mb_per_sec => MbPerSec
            }};
        Error ->
            Error
    end.

run_mixed(Total, Concurrency, Pipeline, DataSize) ->
    run_mixed(Total, Concurrency, Pipeline, DataSize, 0).

run_mixed(Total, Concurrency, Pipeline, DataSize, WarmupTotal)
    when Total >= 0,
         Concurrency > 0,
         Pipeline > 0,
         DataSize >= 0,
         WarmupTotal >= 0 ->
    Value = binary:copy(<<"x">>, DataSize),
    SeedTotal = max(Total, WarmupTotal),
    ok = seed_mixed_read_keys(SeedTotal, max(1, min(Concurrency, 32)), Pipeline, Value),
    _ = run_mixed_workers(WarmupTotal, max(1, min(Concurrency, 32)), Pipeline, Value),
    StartedAt = erlang:monotonic_time(microsecond),
    case run_mixed_workers(Total, Concurrency, Pipeline, Value) of
        {ok, Ops, Reads, Writes} ->
            ElapsedUs = max(erlang:monotonic_time(microsecond) - StartedAt, 1),
            OpsPerSec = Ops * 1000000 / ElapsedUs,
            MbPerSec = Writes * DataSize * 1000000 / ElapsedUs / 1048576,
            {ok, #{
                ops => Ops,
                reads => Reads,
                writes => Writes,
                elapsed_us => ElapsedUs,
                ops_per_sec => OpsPerSec,
                mb_per_sec => MbPerSec
            }};
        Error ->
            Error
    end.

run_workers(0, _Concurrency, _Pipeline, _Value) ->
    {ok, 0};
run_workers(Total, Concurrency, Pipeline, Value) ->
    Parent = self(),
    Counter = atomics:new(1, []),
    Monitors = [
        spawn_monitor(fun() ->
            Result =
                try
                    {ok, worker(Worker, Counter, Total, Pipeline, Value, 0)}
                catch
                    Class:Reason:Stack ->
                        {error, {Class, Reason, Stack}}
                end,
            Parent ! {self(), Result}
        end)
     || Worker <- lists:seq(1, Concurrency)],
    collect(length(Monitors), 0).

worker(Worker, Counter, Total, Pipeline, Value, Done) ->
    Start = atomics:add_get(Counter, 1, Pipeline) - Pipeline + 1,
    case Start > Total of
        true ->
            Done;
        false ->
            Count = min(Pipeline, Total - Start + 1),
            Tag = {Worker, Start},
            Entries = entries(Start, Count, Value),
            ok = ferricstore_waraft_spike:put_many_async(Tag, Entries),
            receive
                {Tag, ok} ->
                    worker(Worker, Counter, Total, Pipeline, Value, Done + Count);
                {Tag, Other} ->
                    error({unexpected_waraft_reply, Other})
            after ?TIMEOUT ->
                error({waraft_reply_timeout, Tag})
            end
    end.

entries(Start, Count, Value) ->
    [
        {<<"bench:k", (integer_to_binary(Start + Offset))/binary>>, Value}
     || Offset <- lists:seq(0, Count - 1)].

collect(0, Ops) ->
    {ok, Ops};
collect(Remaining, Ops) ->
    receive
        {_Pid, {ok, WorkerOps}} ->
            collect(Remaining - 1, Ops + WorkerOps);
        {_Pid, {error, Reason}} ->
            {error, Reason};
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            collect(Remaining, Ops)
    after ?TIMEOUT ->
        {error, load_driver_timeout}
    end.

run_multi_workers(0, _Concurrency, _Pipeline, _Value, _PartitionCount) ->
    {ok, 0};
run_multi_workers(Total, Concurrency, Pipeline, Value, PartitionCount) ->
    Parent = self(),
    Counter = atomics:new(1, []),
    Monitors = [
        spawn_monitor(fun() ->
            Result =
                try
                    {ok, multi_worker(Worker, Counter, Total, Pipeline, Value, PartitionCount, 0)}
                catch
                    Class:Reason:Stack ->
                        {error, {Class, Reason, Stack}}
                end,
            Parent ! {self(), Result}
        end)
     || Worker <- lists:seq(1, Concurrency)],
    collect(length(Monitors), 0).

multi_worker(Worker, Counter, Total, Pipeline, Value, PartitionCount, Done) ->
    Start = atomics:add_get(Counter, 1, Pipeline) - Pipeline + 1,
    case Start > Total of
        true ->
            Done;
        false ->
            Count = min(Pipeline, Total - Start + 1),
            Batches = multi_entries_by_partition(Start, Count, Value, PartitionCount),
            ok = submit_multi_batches(Worker, Start, Batches),
            ok = await_multi_batches(Worker, Start, Batches),
            multi_worker(Worker, Counter, Total, Pipeline, Value, PartitionCount, Done + Count)
    end.

multi_entries_by_partition(Start, Count, Value, PartitionCount) ->
    Groups =
        lists:foldl(
            fun(Index, Acc) ->
                Partition = partition_for_index(Index, PartitionCount),
                Entry = {multi_key(Partition, Index), Value},
                maps:update_with(Partition, fun(Entries) -> [Entry | Entries] end, [Entry], Acc)
            end,
            #{},
            lists:seq(Start, Start + Count - 1)
        ),
    [{Partition, lists:reverse(Entries)} || {Partition, Entries} <- lists:sort(maps:to_list(Groups))].

submit_multi_batches(_Worker, _Start, []) ->
    ok;
submit_multi_batches(Worker, Start, [{Partition, Entries} | Rest]) ->
    Tag = {multi, Worker, Start, Partition},
    ok = ferricstore_waraft_spike:put_many_async_on(Tag, Partition, Entries),
    submit_multi_batches(Worker, Start, Rest).

await_multi_batches(_Worker, _Start, []) ->
    ok;
await_multi_batches(Worker, Start, [{Partition, _Entries} | Rest]) ->
    Tag = {multi, Worker, Start, Partition},
    receive
        {Tag, ok} ->
            await_multi_batches(Worker, Start, Rest);
        {Tag, Other} ->
            error({unexpected_waraft_reply, Other})
    after ?TIMEOUT ->
        error({waraft_reply_timeout, Tag})
    end.

partition_for_index(Index, PartitionCount) ->
    ((Index - 1) rem PartitionCount) + 1.

multi_key(Partition, Index) ->
    <<"bench:p", (integer_to_binary(Partition))/binary, ":k", (integer_to_binary(Index))/binary>>.

seed_mixed_read_keys(0, _Concurrency, _Pipeline, _Value) ->
    ok;
seed_mixed_read_keys(Total, Concurrency, Pipeline, Value) ->
    case run_seed_workers(Total, Concurrency, Pipeline, Value) of
        {ok, _Ops} -> ok;
        {error, Reason} -> error({seed_mixed_read_keys_failed, Reason})
    end.

run_seed_workers(Total, Concurrency, Pipeline, Value) ->
    Parent = self(),
    Counter = atomics:new(1, []),
    Monitors = [
        spawn_monitor(fun() ->
            Result =
                try
                    {ok, seed_worker(Worker, Counter, Total, Pipeline, Value, 0)}
                catch
                    Class:Reason:Stack ->
                        {error, {Class, Reason, Stack}}
                end,
            Parent ! {self(), Result}
        end)
     || Worker <- lists:seq(1, Concurrency)],
    collect(length(Monitors), 0).

seed_worker(Worker, Counter, Total, Pipeline, Value, Done) ->
    Start = atomics:add_get(Counter, 1, Pipeline) - Pipeline + 1,
    case Start > Total of
        true ->
            Done;
        false ->
            Count = min(Pipeline, Total - Start + 1),
            Tag = {seed, Worker, Start},
            Entries = mixed_read_seed_entries(Start, Count, Value),
            ok = ferricstore_waraft_spike:put_many_async(Tag, Entries),
            receive
                {Tag, ok} ->
                    seed_worker(Worker, Counter, Total, Pipeline, Value, Done + Count);
                {Tag, Other} ->
                    error({unexpected_waraft_reply, Other})
            after ?TIMEOUT ->
                error({waraft_reply_timeout, Tag})
            end
    end.

mixed_read_seed_entries(Start, Count, Value) ->
    [
        {mixed_read_key(Start + Offset), Value}
     || Offset <- lists:seq(0, Count - 1)].

run_mixed_workers(0, _Concurrency, _Pipeline, _Value) ->
    {ok, 0, 0, 0};
run_mixed_workers(Total, Concurrency, Pipeline, Value) ->
    Parent = self(),
    Counter = atomics:new(1, []),
    Monitors = [
        spawn_monitor(fun() ->
            Result =
                try
                    {ok, mixed_worker(Worker, Counter, Total, Pipeline, Value, 0, 0, 0)}
                catch
                    Class:Reason:Stack ->
                        {error, {Class, Reason, Stack}}
                end,
            Parent ! {self(), Result}
        end)
     || Worker <- lists:seq(1, Concurrency)],
    collect_mixed(length(Monitors), 0, 0, 0).

mixed_worker(Worker, Counter, Total, Pipeline, Value, Done, Reads, Writes) ->
    Start = atomics:add_get(Counter, 1, Pipeline) - Pipeline + 1,
    case Start > Total of
        true ->
            {Done, Reads, Writes};
        false ->
            Count = min(Pipeline, Total - Start + 1),
            {WriteEntries, ReadKeys} = mixed_batch(Start, Count, Value),
            Tag = {mixed, Worker, Start},
            ok = maybe_put_many_async(Tag, WriteEntries),
            ok = read_mixed_keys(ReadKeys, Value),
            ok = maybe_await_put_many(Tag, WriteEntries),
            mixed_worker(
                Worker,
                Counter,
                Total,
                Pipeline,
                Value,
                Done + Count,
                Reads + length(ReadKeys),
                Writes + length(WriteEntries)
            )
    end.

mixed_batch(Start, Count, Value) ->
    lists:foldr(
        fun(Index, {Writes, Reads}) ->
            case Index rem 2 of
                0 -> {[{mixed_write_key(Index), Value} | Writes], Reads};
                1 -> {Writes, [mixed_read_key(Index) | Reads]}
            end
        end,
        {[], []},
        lists:seq(Start, Start + Count - 1)
    ).

maybe_put_many_async(_Tag, []) ->
    ok;
maybe_put_many_async(Tag, Entries) ->
    ferricstore_waraft_spike:put_many_async(Tag, Entries).

maybe_await_put_many(_Tag, []) ->
    ok;
maybe_await_put_many(Tag, _Entries) ->
    receive
        {Tag, ok} ->
            ok;
        {Tag, Other} ->
            error({unexpected_waraft_reply, Other})
    after ?TIMEOUT ->
        error({waraft_reply_timeout, Tag})
    end.

read_mixed_keys([], _Expected) ->
    ok;
read_mixed_keys([Key | Rest], Expected) ->
    %% FerricStore hot GET reads local state after apply; WARaft strong reads
    %% have their own queue and are not representative for this workload.
    case ferricstore_waraft_spike:storage_get(Key) of
        {ok, Expected} ->
            read_mixed_keys(Rest, Expected);
        Other ->
            error({unexpected_waraft_read_reply, Key, Other})
    end.

mixed_read_key(Index) ->
    <<"mixed:get:k", (integer_to_binary(Index))/binary>>.

mixed_write_key(Index) ->
    <<"mixed:set:k", (integer_to_binary(Index))/binary>>.

collect_mixed(0, Ops, Reads, Writes) ->
    {ok, Ops, Reads, Writes};
collect_mixed(Remaining, Ops, Reads, Writes) ->
    receive
        {_Pid, {ok, {WorkerOps, WorkerReads, WorkerWrites}}} ->
            collect_mixed(
                Remaining - 1,
                Ops + WorkerOps,
                Reads + WorkerReads,
                Writes + WorkerWrites
            );
        {_Pid, {error, Reason}} ->
            {error, Reason};
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            collect_mixed(Remaining, Ops, Reads, Writes)
    after ?TIMEOUT ->
        {error, load_driver_timeout}
    end.
