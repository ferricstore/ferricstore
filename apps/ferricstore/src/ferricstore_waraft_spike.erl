%%% Copyright (c) FerricStore contributors.
%%%
%%% WARaft spike adapter. This is intentionally isolated from the production
%%% Ra path so we can evaluate WARaft API/performance without migrating the
%%% product surface.

-module(ferricstore_waraft_spike).

-export([
    start/1,
    start_volatile/1,
    start_multi_volatile/2,
    start_multi_volatile_segment_log/2,
    start_segment_log/1,
    start_volatile_segment_log/1,
    start_cluster_member/1,
    start_cluster_member_segment_log/1,
    bootstrap_cluster/1,
    trigger_election/0,
    stop/0,
    put/2,
    put_many/1,
    put_on/3,
    put_async/3,
    put_async_on/4,
    put_many_async/2,
    put_many_async_on/3,
    get/1,
    get_on/2,
    storage_get/1,
    storage_get_on/2,
    create_snapshot/0,
    install_snapshot/2,
    adjust_membership/2,
    membership/0,
    status/0
]).

-include_lib("wa_raft/include/wa_raft.hrl").

-define(APP, ferricstore).
-define(TABLE, ferricstore_waraft_spike).
-define(PARTITION, 1).
-define(SUP_ID, ferricstore_waraft_spike_sup).
-define(TIMEOUT, 5000).

start(DataDir) ->
    start(DataDir, ferricstore_waraft_spike_storage).

start_volatile(DataDir) ->
    start(DataDir, ferricstore_waraft_spike_volatile_storage).

start_multi_volatile(DataDir, PartitionCount) when PartitionCount > 0 ->
    start_multi(DataDir, ferricstore_waraft_spike_volatile_storage, PartitionCount, ferricstore_waraft_spike_segment_log).

start_multi_volatile_segment_log(DataDir, PartitionCount) when PartitionCount > 0 ->
    start_multi(DataDir, ferricstore_waraft_spike_volatile_storage, PartitionCount, ferricstore_waraft_spike_segment_log).

start_segment_log(DataDir) ->
    start(DataDir, ferricstore_waraft_spike_storage, auto_bootstrap, ferricstore_waraft_spike_segment_log).

start_volatile_segment_log(DataDir) ->
    start(DataDir, ferricstore_waraft_spike_volatile_storage, auto_bootstrap, ferricstore_waraft_spike_segment_log).

start_cluster_member(DataDir) ->
    start(DataDir, ferricstore_waraft_spike_volatile_storage, cluster_member).

start_cluster_member_segment_log(DataDir) ->
    start(DataDir, ferricstore_waraft_spike_volatile_storage, cluster_member, ferricstore_waraft_spike_segment_log).

start(DataDir, StorageModule) ->
    start(DataDir, StorageModule, auto_bootstrap).

start(DataDir, StorageModule, BootstrapMode) ->
    start(DataDir, StorageModule, BootstrapMode, ferricstore_waraft_spike_segment_log).

start(DataDir, StorageModule, BootstrapMode, LogModule) ->
    ok = ensure_started(),
    _ = stop(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    configure_for_spike(),
    application:set_env(?APP, raft_database, DataDir),
    Spec0 = wa_raft_sup:child_spec(?APP, [
        #{
            table => ?TABLE,
            partition => ?PARTITION,
            log_module => LogModule,
            storage_module => StorageModule
        }
    ]),
    Spec = Spec0#{id => ?SUP_ID},
    case supervisor:start_child(kernel_sup, Spec) of
        {ok, _Pid} -> finish_start_or_cleanup(BootstrapMode);
        {ok, _Pid, _Info} -> finish_start_or_cleanup(BootstrapMode);
        {error, {already_started, _Pid}} -> finish_start_or_cleanup(BootstrapMode);
        {error, already_present} ->
            ok = supervisor:delete_child(kernel_sup, ?SUP_ID),
            start(DataDir, StorageModule, BootstrapMode, LogModule);
        {error, Reason} ->
            _ = stop(),
            {error, Reason}
    end.

start_multi(DataDir, StorageModule, PartitionCount, LogModule) ->
    ok = ensure_started(),
    _ = stop(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    configure_for_spike(),
    application:set_env(?APP, raft_database, DataDir),
    Partitions = lists:seq(1, PartitionCount),
    Specs = [
        #{
            table => ?TABLE,
            partition => Partition,
            log_module => LogModule,
            storage_module => StorageModule
        }
     || Partition <- Partitions],
    Spec0 = wa_raft_sup:child_spec(?APP, Specs),
    Spec = Spec0#{id => ?SUP_ID},
    case supervisor:start_child(kernel_sup, Spec) of
        {ok, _Pid} -> finish_start_multi_or_cleanup(Partitions);
        {ok, _Pid, _Info} -> finish_start_multi_or_cleanup(Partitions);
        {error, {already_started, _Pid}} -> finish_start_multi_or_cleanup(Partitions);
        {error, already_present} ->
            ok = supervisor:delete_child(kernel_sup, ?SUP_ID),
            start_multi(DataDir, StorageModule, PartitionCount, LogModule);
        {error, Reason} ->
            _ = stop(),
            {error, Reason}
    end.

bootstrap_cluster(Nodes) ->
    Server = ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
    Members = [#raft_identity{name = Server, node = Node} || Node <- Nodes],
    Config = wa_raft_server:make_config(Members),
    case wa_raft_server:bootstrap(Server, #raft_log_pos{index = 1, term = 1}, Config, #{}) of
        ok ->
            ok;
        {error, already_bootstrapped} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

trigger_election() ->
    wa_raft_server:trigger_election(?RAFT_SERVER_NAME(?TABLE, ?PARTITION)).

stop() ->
    _ = supervisor:terminate_child(kernel_sup, ?SUP_ID),
    _ = supervisor:delete_child(kernel_sup, ?SUP_ID),
    _ = stop_orphaned_waraft_sup(),
    wait_down(registered_names(), 100).

put(Key, Value) ->
    put_on(?PARTITION, Key, Value).

put_on(Partition, Key, Value) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, Partition),
    wa_raft_acceptor:commit(Acceptor, {make_ref(), {write, ?TABLE, Key, Value}}, ?TIMEOUT, low).

put_many(Entries) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, ?PARTITION),
    wa_raft_acceptor:commit(Acceptor, {make_ref(), {write_many, ?TABLE, Entries}}, ?TIMEOUT, low).

put_async(Tag, Key, Value) ->
    put_async_on(Tag, ?PARTITION, Key, Value).

put_async_on(Tag, Partition, Key, Value) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, Partition),
    wa_raft_acceptor:commit_async(
        Acceptor,
        {self(), Tag},
        {make_ref(), {write, ?TABLE, Key, Value}},
        low
    ).

put_many_async(Tag, Entries) ->
    put_many_async_on(Tag, ?PARTITION, Entries).

put_many_async_on(Tag, Partition, Entries) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, Partition),
    wa_raft_acceptor:commit_async(
        Acceptor,
        {self(), Tag},
        {make_ref(), {write_many, ?TABLE, Entries}},
        low
    ).

get(Key) ->
    get_on(?PARTITION, Key).

get_on(Partition, Key) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, Partition),
    wa_raft_acceptor:read(Acceptor, {read, ?TABLE, Key}, ?TIMEOUT).

storage_get(Key) ->
    storage_get_on(?PARTITION, Key).

storage_get_on(Partition, Key) ->
    Storage = ?RAFT_STORAGE_NAME(?TABLE, Partition),
    wa_raft_storage:read(Storage, {read, ?TABLE, Key}).

create_snapshot() ->
    Storage = ?RAFT_STORAGE_NAME(?TABLE, ?PARTITION),
    wa_raft_storage:create_snapshot(Storage).

install_snapshot(SnapshotPath, Position) ->
    Server = ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
    wa_raft_server:snapshot_available(Server, SnapshotPath, Position).

adjust_membership(Action, Peer) ->
    Server = ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
    wa_raft_server:adjust_membership(Server, Action, Peer).

membership() ->
    Server = ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
    wa_raft_server:membership(Server).

status() ->
    wa_raft_server:status(?RAFT_SERVER_NAME(?TABLE, ?PARTITION)).

finish_start(cluster_member) ->
    Server = ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
    case wait_status(Server, 100) of
        {ok, _Status} -> ok;
        {error, Reason} -> {error, Reason}
    end;
finish_start(auto_bootstrap) ->
    Server = ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
    case wait_status(Server, 100) of
        {ok, Status} ->
            case proplists:get_value(state, Status) of
                stalled ->
                    ok = bootstrap(Server),
                    wait_leader(Server, 100);
                leader ->
                    ok;
                _Other ->
                    _ = promote_local_leader(Server),
                    wait_leader(Server, 100)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

finish_start_or_cleanup(BootstrapMode) ->
    case finish_start(BootstrapMode) of
        ok ->
            ok;
        {error, _Reason} = Error ->
            _ = stop(),
            Error
    end.

bootstrap(Server) ->
    Config = wa_raft_server:make_config([
        #raft_identity{name = Server, node = node()}
    ]),
    wa_raft_server:bootstrap(Server, #raft_log_pos{index = 1, term = 1}, Config, #{}).

wait_leader(Server, Attempts) ->
    wait_leader(Server, Attempts, undefined).

wait_leader(_Server, 0, LastStatus) ->
    {error, {not_leader, proplists:get_value(state, LastStatus), LastStatus}};
wait_leader(Server, Attempts, _LastStatus) ->
    case wait_status(Server, 1) of
        {ok, Status} ->
            case proplists:get_value(state, Status) of
                leader ->
                    ok;
                _Other ->
                    timer:sleep(10),
                    wait_leader(Server, Attempts - 1, Status)
            end;
        Error ->
            Error
    end.

finish_start_multi(Partitions) ->
    lists:foldl(
        fun
            (_Partition, {error, _Reason} = Error) ->
                Error;
            (Partition, ok) ->
                finish_start_partition(Partition)
        end,
        ok,
        Partitions
    ).

finish_start_multi_or_cleanup(Partitions) ->
    case finish_start_multi(Partitions) of
        ok ->
            ok;
        {error, _Reason} = Error ->
            _ = stop(),
            Error
    end.

finish_start_partition(Partition) ->
    Server = ?RAFT_SERVER_NAME(?TABLE, Partition),
    case wait_status(Server, 100) of
        {ok, Status} ->
            case proplists:get_value(state, Status) of
                stalled ->
                    ok = bootstrap(Server),
                    wait_leader(Server, 100);
                leader ->
                    ok;
                _Other ->
                    _ = promote_local_leader(Server),
                    wait_leader(Server, 100)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

wait_status(_Server, 0) ->
    {error, status_timeout};
wait_status(Server, Attempts) ->
    try wa_raft_server:status(Server) of
        Status -> {ok, Status}
    catch
        _:_ ->
            timer:sleep(10),
            wait_status(Server, Attempts - 1)
    end.

promote_local_leader(Server) ->
    %% Auto-bootstrap mode is only used by single-node spike partitions. Force a
    %% deterministic local promotion on restart; cluster-member mode still uses
    %% explicit bootstrap/election so this cannot mask multi-node safety issues.
    case wa_raft_server:promote(Server, next, true) of
        ok -> ok;
        {error, _Reason} -> wa_raft_server:trigger_election(Server)
    end.

wait_down(_Names, 0) ->
    ok;
wait_down(Names, Attempts) ->
    case [Name || Name <- Names, whereis(Name) =/= undefined] of
        [] ->
            ok;
        _ ->
            timer:sleep(10),
            wait_down(Names, Attempts - 1)
    end.

registered_names() ->
    [
        wa_raft_sup:default_name(?APP),
        ?SUP_ID,
        ?RAFT_SUPERVISOR_NAME(?TABLE, ?PARTITION),
        ?RAFT_ACCEPTOR_NAME(?TABLE, ?PARTITION),
        ?RAFT_LOG_NAME(?TABLE, ?PARTITION),
        wa_raft_queue:default_read_queue_name(?TABLE, ?PARTITION),
        ?RAFT_SERVER_NAME(?TABLE, ?PARTITION),
        ?RAFT_STORAGE_NAME(?TABLE, ?PARTITION),
        wa_raft_transport_cleanup:default_name(?TABLE, ?PARTITION)
    ].

stop_orphaned_waraft_sup() ->
    Name = wa_raft_sup:default_name(?APP),
    case whereis(Name) of
        undefined ->
            ok;
        _Pid ->
            _ = catch supervisor:stop(Name, shutdown, infinity),
            kill_orphaned_waraft_sup(Name)
    end.

kill_orphaned_waraft_sup(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            %% wa_raft_sup can be orphaned if partition startup crashes inside
            %% start_link/3. It may not be linked to kernel_sup, so force the
            %% spike cleanup path to remove it before the next start attempt.
            exit(Pid, kill),
            ok
    end.

ensure_started() ->
    case application:ensure_all_started(wa_raft) of
        {ok, _} -> ok;
        {error, {already_started, wa_raft}} -> ok;
        {error, Reason} -> error({wa_raft_start_failed, Reason})
    end.

configure_for_spike() ->
    application:set_env(?APP, raft_max_pending_low_priority_commits, 100000),
    application:set_env(?APP, raft_max_pending_high_priority_commits, 100000),
    application:set_env(?APP, raft_max_pending_reads, 100000),
    application:set_env(?APP, raft_max_pending_applies, 100000),
    application:set_env(?APP, raft_apply_queue_max_size, 100000),
    application:set_env(?APP, raft_commit_batch_interval_ms, 'Elixir.Ferricstore.Raft.WARaftBackend':default_commit_batch_interval_ms()),
    application:set_env(?APP, raft_commit_batch_max, 'Elixir.Ferricstore.Raft.WARaftBackend':default_commit_batch_max()),
    application:set_env(?APP, raft_max_log_entries_per_heartbeat, 1024),
    application:set_env(?APP, raft_max_heartbeat_size, 16 * 1024 * 1024),
    application:set_env(?APP, raft_apply_log_batch_size, 1024),
    application:set_env(?APP, raft_apply_batch_max_bytes, 16 * 1024 * 1024),
    ok.
