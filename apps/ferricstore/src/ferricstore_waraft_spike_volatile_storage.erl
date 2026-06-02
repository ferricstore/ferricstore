%%% Copyright (c) FerricStore contributors.
%%%
%%% Volatile ETS storage for the WARaft spike. This is intentionally not
%%% durable; it exists to measure WARaft core command/apply overhead with the
%%% same batched write shape FerricStore uses on hot paths.

-module(ferricstore_waraft_spike_volatile_storage).

-behaviour(wa_raft_storage).

-export([
    storage_open/2,
    storage_close/1,
    storage_label/1,
    storage_position/1,
    storage_config/1,
    storage_apply/3,
    storage_apply/4,
    storage_apply_config/3,
    storage_read/3,
    storage_create_snapshot/2,
    storage_create_witness_snapshot/2,
    storage_open_snapshot/3,
    storage_make_empty_snapshot/5
]).

-include_lib("wa_raft/include/wa_raft.hrl").

-define(OPTIONS, [set, public, {read_concurrency, true}, {write_concurrency, true}]).
-define(SNAPSHOT_FILENAME, "data").
-define(METADATA_TAG, '$metadata').
-define(LABEL_TAG, '$label').
-define(POSITION_TAG, '$position').
-define(INCOMPLETE_TAG, '$incomplete').

-record(state, {
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    self :: #raft_identity{},
    storage :: ets:table()
}).

storage_open(#raft_options{table = Table, partition = Partition, self = Self, storage_name = Name}, _RootDir) ->
    Storage = ets:new(Name, ?OPTIONS),
    #state{name = Name, table = Table, partition = Partition, self = Self, storage = Storage}.

storage_close(#state{storage = Storage}) ->
    true = ets:delete(Storage),
    ok.

storage_position(#state{storage = Storage}) ->
    ets:lookup_element(Storage, ?POSITION_TAG, 2, #raft_log_pos{}).

storage_label(#state{storage = Storage}) ->
    case ets:lookup(Storage, ?LABEL_TAG) of
        [{_, Label}] -> {ok, Label};
        [] -> {ok, undefined}
    end.

storage_config(#state{storage = Storage}) ->
    case ets:lookup(Storage, {?METADATA_TAG, config}) of
        [{_, {Version, Value}}] -> {ok, Version, Value};
        [] -> undefined
    end.

storage_apply(Command, Position, Label, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, {?LABEL_TAG, Label}),
    storage_apply(Command, Position, State).

storage_apply(noop, Position, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, {?POSITION_TAG, Position}),
    {ok, State};
storage_apply(noop_omitted, Position, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, [{?INCOMPLETE_TAG, true}, {?POSITION_TAG, Position}]),
    {ok, State};
storage_apply({write, _Table, Key, Value}, Position, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, [{Key, Value}, {?POSITION_TAG, Position}]),
    {ok, State};
storage_apply({write_many, _Table, Entries}, Position, #state{storage = Storage} = State) ->
    Records = [{Key, Value} || {Key, Value} <- Entries],
    true = ets:insert(Storage, [{?POSITION_TAG, Position} | Records]),
    {ok, State};
storage_apply({delete, _Table, Key}, Position, #state{storage = Storage} = State) ->
    true = ets:delete(Storage, Key),
    true = ets:insert(Storage, {?POSITION_TAG, Position}),
    {ok, State}.

storage_apply_config(Config, LogPos, State) ->
    storage_apply_config(Config, LogPos, LogPos, State).

storage_apply_config(Config, ConfigPos, LogPos, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, [{{?METADATA_TAG, config}, {ConfigPos, Config}}, {?POSITION_TAG, LogPos}]),
    {ok, State}.

storage_read(noop, _Position, #state{}) ->
    ok;
storage_read({read, _Table, Key}, _Position, #state{storage = Storage}) ->
    case ets:lookup(Storage, Key) of
        [{_, Value}] -> {ok, Value};
        [] -> not_found
    end.

storage_create_snapshot(SnapshotPath, #state{storage = Storage}) ->
    case filelib:ensure_path(SnapshotPath) of
        ok -> ets:tab2file(Storage, filename:join(SnapshotPath, ?SNAPSHOT_FILENAME));
        {error, Reason} -> {error, Reason}
    end.

storage_create_witness_snapshot(SnapshotPath, #state{name = Name, table = Table, partition = Partition, self = Self} = State) ->
    {ok, ConfigPosition, Config} = storage_config(State),
    SnapshotPosition = storage_position(State),
    storage_make_empty_snapshot(Name, Table, Partition, Self, SnapshotPath, SnapshotPosition, Config, ConfigPosition, #{}).

storage_open_snapshot(SnapshotPath, SnapshotPosition, #state{storage = OldStorage} = State) ->
    SnapshotData = filename:join(SnapshotPath, ?SNAPSHOT_FILENAME),
    case ets:file2tab(SnapshotData) of
        {ok, NewStorage} ->
            case ets:lookup_element(NewStorage, ?POSITION_TAG, 2, #raft_log_pos{}) of
                SnapshotPosition ->
                    try ets:delete(OldStorage) catch _:_ -> ok end,
                    {ok, State#state{storage = NewStorage}};
                _IncorrectPosition ->
                    try ets:delete(NewStorage) catch _:_ -> ok end,
                    {error, bad_position}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

storage_make_empty_snapshot(#raft_options{table = Table, partition = Partition, self = Self, storage_name = Name}, SnapshotPath, SnapshotPosition, Config, Data) ->
    storage_make_empty_snapshot(Name, Table, Partition, Self, SnapshotPath, SnapshotPosition, Config, SnapshotPosition, Data).

storage_make_empty_snapshot(Name, Table, Partition, Self, SnapshotPath, SnapshotPosition, Config, ConfigPosition, _Data) ->
    Storage = ets:new(Name, ?OPTIONS),
    State = #state{
        name = Name,
        table = Table,
        partition = Partition,
        self = Self,
        storage = Storage
    },
    {ok, State1} = storage_apply_config(Config, ConfigPosition, SnapshotPosition, State),
    storage_create_snapshot(SnapshotPath, State1).
