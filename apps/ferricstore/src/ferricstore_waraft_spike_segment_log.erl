%%% Copyright (c) FerricStore contributors.
%%%
%%% Durable segmented WARaft log provider for the migration spike.
%%% Each append batch is written as CRC-framed records and data-synced before
%%% the Raft append is acknowledged. Same-segment batches pay one data sync for
%%% the group, new segments can reserve disk through keep-size preallocation,
%%% and recovery fails closed on corrupt or ambiguous records.

-module(ferricstore_waraft_spike_segment_log).

-behaviour(wa_raft_log).

-export([
    first_index/1,
    last_index/1,
    fold/6,
    fold_binary/6,
    fold_terms/5,
    get/2,
    term/2,
    config/1,
    fold_disk/3,
    location_for_index/2,
    read_disk/2,
    read_disk_at/4,
    write_projection/3,
    write_projection_batch/3
]).

-export([
    append/4
]).

-export([
    init/1,
    open/1,
    close/2,
    reset/3,
    truncate/3,
    trim/3,
    flush/1
]).

-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("kernel/include/file.hrl").

-define(DEFAULT_SEGMENT_RECORDS, 65536).
-define(SEGMENT_EXT, ".seg").
-define(SEGMENT_CONFIG_FILE, "segment_config.term").
-define(RECORD_HEADER_SIZE, 8).
-define(MAX_RECORD_BYTES, 1073741824).
-define(MAX_SEGMENT_METADATA_BYTES, 1048576).
-define(REWRITE_MARKER_EXT, ".rewrite.term").
-define(REWRITE_STAGING_PREFIX, ".rewrite.staging.").
-define(REWRITE_BACKUP_PREFIX, ".rewrite.backup.").
-define(APPEND_FAILURE_MARKER, "segment_log.append_failed.term").
-define(DEFAULT_PREALLOCATE_BYTES, 0).
-define(WRITER_REGISTRY, ferricstore_waraft_segment_writer_registry).
-define(OFFSET_REGISTRY, ferricstore_waraft_segment_offset_registry).
-define(DEFAULT_WAL_NIF_SYNC_TIMEOUT_MS, 30000).
-define(DEFAULT_WAL_NIF_MAX_BUFFER_BYTES, 67108864).

first_index(#raft_log{name = Name}) ->
    case ets:first(Name) of
        '$end_of_table' -> undefined;
        Key -> Key
    end.

last_index(#raft_log{name = Name}) ->
    case ets:last(Name) of
        '$end_of_table' -> undefined;
        Key -> Key
    end.

fold(Log, Start, End, SizeLimit, Func, Acc) ->
    fold_impl(Log, Start, End, 0, SizeLimit, Func, Acc).

fold_binary(Log, Start, End, SizeLimit, Func, Acc) ->
    fold_binary_impl(Log, Start, End, 0, SizeLimit, Func, Acc).

fold_terms(Log, Start, End, Func, Acc) ->
    fold_terms_impl(Log, Start, End, Func, Acc).

get(#raft_log{name = Name}, Index) ->
    case ets:lookup(Name, Index) of
        [{Index, Entry}] -> {ok, Entry};
        [] -> not_found
    end.

term(Log, Index) ->
    case get(Log, Index) of
        {ok, {Term, _Op}} -> {ok, Term};
        not_found -> not_found
    end.

config(#raft_log{name = Name}) ->
    config_from_index(Name, ets:last(Name)).

config_from_index(_Name, '$end_of_table') ->
    not_found;
config_from_index(Name, Index) ->
    case ets:lookup(Name, Index) of
        [{Index, Entry}] ->
            case config_from_entry(Entry) of
                {ok, Config} -> {ok, Index, Config};
                not_found -> config_from_index(Name, ets:prev(Name, Index))
            end;
        [] ->
            config_from_index(Name, ets:prev(Name, Index))
    end.

config_from_entry({_Term, {_Key, {config, Config}}}) ->
    {ok, Config};
config_from_entry({_Term, {_Key, _Label, {config, Config}}}) ->
    {ok, Config};
config_from_entry(_Entry) ->
    not_found.

fold_disk(RootDir, Fun, Acc) when is_function(Fun, 3) ->
    Dir = fold_disk_segment_dir(RootDir),
    Tid = ets:new(?MODULE, [ordered_set]),
    try
        case validate_segment_log_dir(Dir) of
            ok ->
                case recover_rewrite(Dir) of
                    ok ->
                        case validate_segment_log_dir(Dir) of
                            ok ->
                                case preload_segment_config(Dir) of
                                    ok ->
                                        case load_segments(Dir, Tid) of
                                            ok -> {ok, fold_loaded_disk_records(Tid, ets:first(Tid), Fun, Acc)};
                                            {error, _Reason} = Error -> Error
                                        end;
                                    {error, enoent} ->
                                        {ok, Acc};
                                    {error, _Reason} = Error ->
                                        Error
                                end;
                            {error, _Reason} = Error ->
                                Error
                        end;
                    {error, _Reason} = Error ->
                        Error
                end;
            {error, enoent} ->
                {ok, Acc};
            {error, _Reason} = Error ->
                Error
        end
    after
        ets:delete(Tid)
    end.

read_disk(RootDir, Index) when is_integer(Index), Index >= 0 ->
    Dir = fold_disk_segment_dir(RootDir),
    case validate_segment_log_dir(Dir) of
        ok ->
            case recover_rewrite(Dir) of
                ok ->
                    case validate_segment_log_dir(Dir) of
                        ok ->
                            case existing_records_per_segment(Dir) of
                                {ok, RecordsPerSegment} ->
                                    read_disk_record(Dir, Index, RecordsPerSegment);
                                not_found ->
                                    not_found;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, enoent} ->
                            not_found;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, enoent} ->
            not_found;
        {error, _Reason} = Error ->
            Error
    end;
read_disk(_RootDir, _Index) ->
    {error, bad_index}.

location_for_index(RootDir, Index) when is_integer(Index), Index >= 0 ->
    Dir = fold_disk_segment_dir(RootDir),
    case lookup_offset(Dir, Index) of
        {ok, Location} ->
            {ok, Location};
        not_found ->
            case rebuild_offset_registry(Dir) of
                ok -> lookup_offset(Dir, Index);
                {error, enoent} -> not_found;
                {error, _Reason} = Error -> Error
            end
    end;
location_for_index(_RootDir, _Index) ->
    {error, bad_index}.

read_disk_at(RootDir, Index, Offset, EncodedSize)
  when is_integer(Index), Index >= 0,
       is_integer(Offset), Offset >= 0,
       is_integer(EncodedSize), EncodedSize >= ?RECORD_HEADER_SIZE ->
    Dir = fold_disk_segment_dir(RootDir),
    case validate_segment_log_dir(Dir) of
        ok ->
            case recover_rewrite(Dir) of
                ok ->
                    case validate_segment_log_dir(Dir) of
                        ok ->
                            case existing_records_per_segment(Dir) of
                                {ok, RecordsPerSegment} ->
                                    read_disk_record_at(Dir, Index, Offset, EncodedSize, RecordsPerSegment);
                                not_found ->
                                    not_found;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, enoent} ->
                            not_found;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, enoent} ->
            not_found;
        {error, _Reason} = Error ->
            Error
    end;
read_disk_at(_RootDir, _Index, _Offset, _EncodedSize) ->
    {error, bad_location}.

fold_loaded_disk_records(_Tid, '$end_of_table', _Fun, Acc) ->
    Acc;
fold_loaded_disk_records(Tid, Index, Fun, Acc) ->
    case ets:lookup(Tid, Index) of
        [{Index, Entry}] ->
            fold_loaded_disk_records(Tid, ets:next(Tid, Index), Fun, Fun(Index, Entry, Acc));
        [] ->
            fold_loaded_disk_records(Tid, ets:next(Tid, Index), Fun, Acc)
    end.

fold_disk_segment_dir(RootDir) ->
    Path = unicode:characters_to_list(RootDir),
    case filename:basename(Path) of
        "segment_log" -> Path;
        _Other -> filename:join(Path, "segment_log")
    end.

write_projection(RootDir, Position, Entries) when is_list(Entries) ->
    Dir = fold_disk_segment_dir(RootDir),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            rewrite_records(Dir, projection_records(Position, Entries));
        {error, Reason} ->
            {error, {ensure_projection_dir, Reason}}
    end.

write_projection_batch(RootDir, Position, Entries) when is_list(Entries) ->
    case projection_batch_index(Position) of
        {ok, Index} ->
            Dir = fold_disk_segment_dir(RootDir),
            case filelib:ensure_dir(filename:join(Dir, "dummy")) of
                ok ->
                    Record = {Index, {0, {ferricstore_segment_apply_projection_batch, Position, Entries}}},
                    write_records(Dir, [Record]);
                {error, Reason} ->
                    {error, {ensure_projection_batch_dir, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

projection_records(Position, Entries) ->
    Header = {0, {0, {ferricstore_segment_projection_header, Position, length(Entries)}}},
    {Records, _NextIndex} =
        lists:foldl(
            fun({Key, Value, ExpireAtMs}, {Acc, Index}) ->
                Record = {Index, {0, {ferricstore_segment_projection_entry, Key, Value, ExpireAtMs}}},
                {[Record | Acc], Index + 1}
            end,
            {[Header], 1},
            Entries
        ),
    lists:reverse(Records).

projection_batch_index({raft_log_pos, Index, _Term}) when is_integer(Index), Index > 0 ->
    {ok, Index};
projection_batch_index(_Position) ->
    {error, bad_projection_batch_position}.

append(View, Entries, _Mode, _Priority) ->
    Log = wa_raft_log:log(View),
    Name = wa_raft_log:log_name(View),
    Last = wa_raft_log:last_index(View),
    case append_decode(Last + 1, Entries) of
        {ok, Records} ->
            Dir = log_dir(Log),
            case write_records(Dir, Records) of
                ok ->
                    true = ets:insert(Name, Records),
                    ok;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

init(#raft_log{name = Name}) ->
    case ets:info(Name) of
        undefined -> ok;
        _ -> true = ets:delete(Name)
    end,
    ets:new(Name, [ordered_set, public, named_table, {read_concurrency, true}]),
    ok.

open(#raft_log{name = Name} = Log) ->
    Dir = log_dir(Log),
    case filelib:ensure_dir(filename:join(filename:dirname(Dir), "dummy")) of
        ok ->
            case validate_segment_log_dir(Dir) of
                ok ->
                    case check_append_failure_marker(Dir) of
                        ok ->
                            case recover_rewrite(Dir) of
                                ok ->
                                    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
                                        ok ->
                                            case validate_segment_log_dir(Dir) of
                                                ok ->
                                                    case preload_segment_config(Dir) of
                                                        ok ->
                                                            true = ets:delete_all_objects(Name),
                                                            _ = ensure_offset_registry(),
                                                            _ = clear_offset_registry_for_dir(Dir),
                                                            case load_segments(Dir, Name) of
                                                                ok -> {ok, #{dir => Dir}};
                                                                {error, _Reason} = Error ->
                                                                    true = ets:delete_all_objects(Name),
                                                                    Error
                                                            end;
                                                        {error, _Reason} = Error ->
                                                            Error
                                                    end;
                                                {error, _Reason} = Error ->
                                                    Error
                                            end;
                                        {error, Reason} ->
                                            {error, {ensure_dir, Reason}}
                                    end;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {ensure_dir, Reason}}
    end.

close(Log, _State) ->
    _ = close_writers_for_dir(log_dir(Log)),
    _ = persistent_term:erase(segment_config_cache_key(log_dir(Log))),
    ok.

reset(#raft_log{name = Name} = Log, #raft_log_pos{index = Index, term = Term}, State) ->
    Record = {Index, {Term, undefined}},
    true = ets:delete_all_objects(Name),
    Dir = log_dir(Log),
    case check_append_failure_marker(Dir) of
        ok ->
            case close_writers_for_dir(Dir) of
                ok ->
                    case rewrite_records(Dir, [Record]) of
                        ok ->
                            true = ets:insert(Name, Record),
                            {ok, State};
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

truncate(_Log, '$end_of_table', State) ->
    {ok, State};
truncate(#raft_log{name = Name} = Log, Index, State) ->
    Dir = log_dir(Log),
    case check_append_failure_marker(Dir) of
        ok ->
            case close_writers_for_dir(Dir) of
                ok ->
                    case rewrite_ets_records_before(Dir, Name, Index) of
                        ok ->
                            delete_from(Name, Index),
                            {ok, State};
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

trim(#raft_log{name = Name} = Log, Index, State) ->
    %% WARaft advances the in-memory log view after this asynchronous callback.
    %% The provider must trim persisted records too, otherwise restart reloads
    %% compacted entries and the segment log grows without bound.
    %% In segment_keydir mode the Raft segment itself is the Bitcask payload
    %% location. Before trimming those records, the storage layer must persist
    %% a projection checkpoint so cold keydir rows can still be rebuilt/read.
    Dir = log_dir(Log),
    case prepare_segment_projection_for_trim(Dir, Index) of
        ok ->
            case check_append_failure_marker(Dir) of
                ok ->
                    case close_writers_for_dir(Dir) of
                        ok ->
                            case rewrite_ets_records_at_or_after(Dir, Name, Index) of
                                ok ->
                                    delete_before(Name, Index),
                                    {ok, State};
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

prepare_segment_projection_for_trim(Dir, Index) ->
    RootDir = unicode:characters_to_binary(filename:dirname(Dir)),
    try 'Elixir.Ferricstore.Raft.WARaftStorage':prepare_segment_projection_for_trim(RootDir, Index) of
        ok -> ok;
        {error, _Reason} = Error -> Error;
        Other -> {error, {prepare_segment_projection_for_trim, Other}}
    catch
        Class:Reason ->
            {error, {prepare_segment_projection_for_trim, Class, Reason}}
    end.

flush(_Log) ->
    ok.

fold_impl(_Log, '$end_of_table', _End, _Size, _SizeLimit, _Func, Acc) ->
    {ok, Acc};
fold_impl(_Log, Start, End, _Size, _SizeLimit, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_impl(_Log, _Start, _End, Size, SizeLimit, _Func, Acc) when Size >= SizeLimit ->
    {ok, Acc};
fold_impl(#raft_log{name = Name} = Log, Start, End, Size, SizeLimit, Func, Acc) ->
    case ets:lookup(Name, Start) of
        [{Start, Entry}] ->
            EntrySize = erlang:external_size(Entry),
            fold_impl(Log, ets:next(Name, Start), End, Size + EntrySize, SizeLimit, Func, Func(Start, EntrySize, Entry, Acc));
        [] ->
            fold_impl(Log, ets:next(Name, Start), End, Size, SizeLimit, Func, Acc)
    end.

fold_binary_impl(_Log, '$end_of_table', _End, _Size, _SizeLimit, _Func, Acc) ->
    {ok, Acc};
fold_binary_impl(_Log, Start, End, _Size, _SizeLimit, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_binary_impl(_Log, _Start, _End, Size, SizeLimit, _Func, Acc) when Size >= SizeLimit ->
    {ok, Acc};
fold_binary_impl(#raft_log{name = Name} = Log, Start, End, Size, SizeLimit, Func, Acc) ->
    case ets:lookup(Name, Start) of
        [{Start, Entry}] ->
            Binary = term_to_binary(Entry),
            EntrySize = byte_size(Binary),
            fold_binary_impl(Log, ets:next(Name, Start), End, Size + EntrySize, SizeLimit, Func, Func(Start, Binary, Acc));
        [] ->
            fold_binary_impl(Log, ets:next(Name, Start), End, Size, SizeLimit, Func, Acc)
    end.

fold_terms_impl(_Log, '$end_of_table', _End, _Func, Acc) ->
    {ok, Acc};
fold_terms_impl(_Log, Start, End, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_terms_impl(#raft_log{name = Name} = Log, Start, End, Func, Acc) ->
    case ets:lookup(Name, Start) of
        [{Start, {Term, _Op}}] ->
            fold_terms_impl(Log, ets:next(Name, Start), End, Func, Func(Start, Term, Acc));
        [] ->
            fold_terms_impl(Log, ets:next(Name, Start), End, Func, Acc)
    end.

append_decode(Index, Entries) ->
    append_decode(Index, Entries, []).

append_decode(_Index, [], Acc) ->
    {ok, lists:reverse(Acc)};
append_decode(Index, [Entry | Entries], Acc) ->
    case decode_append_entry(Entry) of
        {ok, Decoded} ->
            append_decode(Index + 1, Entries, [{Index, Decoded} | Acc]);
        {error, _Reason} = Error ->
            Error
    end.

decode_append_entry(Entry) when is_binary(Entry) ->
    try binary_to_term(Entry, [safe]) of
        Decoded -> {ok, Decoded}
    catch
        _:Reason -> {error, {bad_entry_term, Reason}}
    end;
decode_append_entry(Entry) ->
    {ok, Entry}.

delete_from(_Name, '$end_of_table') ->
    ok;
delete_from(Name, Index) ->
    true = ets:delete(Name, Index),
    delete_from(Name, ets:next(Name, Index)).

delete_before(Name, Index) ->
    case ets:first(Name) of
        '$end_of_table' ->
            ok;
        Key when Key < Index ->
            true = ets:delete(Name, Key),
            delete_before(Name, Index);
        _Key ->
            ok
    end.

write_records(_Dir, []) ->
    ok;
write_records(Dir, Records) ->
    case validate_segment_log_dir(Dir) of
        ok ->
            case check_append_failure_marker(Dir) of
                ok ->
                    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
                        ok ->
                            case validate_segment_log_dir(Dir) of
                                ok ->
                                    case records_per_segment(Dir) of
                                        {ok, RecordsPerSegment} ->
                                            write_records_with_segment_size(Dir, Records, RecordsPerSegment);
                                        {error, _Reason} = Error ->
                                            Error
                                    end;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, Reason} ->
                            {error, {ensure_dir, Reason}}
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

write_records_with_segment_size(_Dir, [], _RecordsPerSegment) ->
    ok;
write_records_with_segment_size(Dir, Records, RecordsPerSegment) ->
    case group_records(Records, RecordsPerSegment) of
        {ok, Groups} -> write_record_group_list(Dir, Groups, []);
        {error, _Reason} = Error -> Error
    end.

group_records(Records, RecordsPerSegment) ->
    group_records_start(Records, RecordsPerSegment).

group_records_start([], _RecordsPerSegment) ->
    {ok, []};
group_records_start([{Index, _Entry} = Record | Rest], RecordsPerSegment) ->
    Ordinal = segment_ordinal(Index, RecordsPerSegment),
    Segment = segment_file_from_ordinal(Ordinal),
    group_records_loop(Rest, RecordsPerSegment, Ordinal, Segment, [Record], []).

group_records_loop([], _RecordsPerSegment, Ordinal, Segment, RecordsRev, Acc) ->
    {ok, lists:reverse([{{Ordinal, Segment}, RecordsRev} | Acc])};
group_records_loop([{Index, _Entry} = Record | Rest], RecordsPerSegment, Ordinal, Segment, RecordsRev, Acc) ->
    NextOrdinal = segment_ordinal(Index, RecordsPerSegment),
    case NextOrdinal of
        Ordinal ->
            group_records_loop(Rest, RecordsPerSegment, Ordinal, Segment, [Record | RecordsRev], Acc);
        _ when NextOrdinal > Ordinal ->
            NextSegment = segment_file_from_ordinal(NextOrdinal),
            group_records_loop(
                Rest,
                RecordsPerSegment,
                NextOrdinal,
                NextSegment,
                [Record],
                [{{Ordinal, Segment}, RecordsRev} | Acc]
            );
        _ ->
            {error, {non_monotonic_segment_group, Index, Ordinal, NextOrdinal}}
    end.

write_record_group_list(Dir, Groups, Rollbacks) ->
    write_record_group_list(Dir, Groups, Rollbacks, []).

write_record_group_list(_Dir, [], _Rollbacks, OffsetAcc) ->
    register_offset_entries(lists:append(lists:reverse(OffsetAcc)));
write_record_group_list(Dir, [{{_Ordinal, Segment}, RecordsRev} | Rest], Rollbacks, OffsetAcc) ->
    case write_record_group_once(Dir, Segment, RecordsRev) of
        {ok, Rollback, Offsets} ->
            write_record_group_list(Dir, Rest, [Rollback | Rollbacks], [Offsets | OffsetAcc]);
        {error, Reason} ->
            case rollback_written_groups(Rollbacks) of
                ok -> maybe_poison_after_append_rollback_failure(Dir, Reason);
                {error, RollbackReason} -> poison_segment_log(Dir, {rollback_failed, Reason, RollbackReason})
            end
    end.

write_record_group_once(Dir, Segment, RecordsRev) ->
    Path = filename:join(Dir, Segment),
    case validate_segment_file_for_append(Path) of
        ok ->
            open_record_group_once(Dir, Path, RecordsRev);
        {error, _Reason} = Error ->
            Error
    end.

open_record_group_once(Dir, Path, RecordsRev) ->
    case segment_io_mode() of
        {ok, file} ->
            open_record_group_once_file(Dir, Path, RecordsRev);
        {ok, wal_nif} ->
            open_record_group_once_wal_nif(Dir, Path, RecordsRev);
        {error, _Reason} = Error ->
            Error
    end.

open_record_group_once_file(Dir, Path, RecordsRev) ->
    case file_writer_mode() of
        persistent ->
            open_record_group_once_file_persistent(Dir, Path, RecordsRev);
        process ->
            open_record_group_once_file_process(Dir, Path, RecordsRev);
        direct ->
            open_record_group_once_file_direct(Dir, Path, RecordsRev);
        {error, _Reason} = Error ->
            Error
    end.

open_record_group_once_file_persistent(Dir, Path, RecordsRev) ->
    case segment_file_fd_handle(Path) of
        {ok, Fd, OldSize} ->
            Records = lists:reverse(RecordsRev),
            Writes = [encode_record(Record) || Record <- Records],
            case write_record_group_file_persistent(Dir, Path, Fd, OldSize, Writes, OldSize =:= 0) of
                ok ->
                    NewSize = OldSize + iolist_size(Writes),
                    case update_segment_writer_position(Path, NewSize) of
                        ok ->
                            {ok, {Path, OldSize}, offset_entries_for_records(Dir, Path, Records, OldSize, Writes)};
                        {error, Reason} ->
                            _ = close_writer_for_path(Path),
                            {error, {update_writer_position, Reason}}
                    end;
                {error, Reason} ->
                    _ = close_writer_for_path(Path),
                    {error, Reason}
            end;
        {error, _Reason} = Error ->
            Error
    end.

open_record_group_once_file_direct(Dir, Path, RecordsRev) ->
    case close_writer_for_path(Path) of
        ok ->
            case file:open(Path, [append, raw, binary]) of
                {ok, Fd} ->
                    case file:position(Fd, eof) of
                        {ok, OldSize} ->
                            case maybe_preallocate_new_segment(Path, OldSize =:= 0) of
                                ok ->
                                    Records = lists:reverse(RecordsRev),
                                    Writes = [encode_record(Record) || Record <- Records],
                                    case write_record_group_file_direct(Dir, Path, Fd, OldSize, Writes, OldSize =:= 0) of
                                        ok ->
                                            {ok, {Path, OldSize}, offset_entries_for_records(Dir, Path, Records, OldSize, Writes)};
                                        {error, Reason} ->
                                            {error, Reason}
                                    end;
                                {error, Reason} ->
                                    _ = file:close(Fd),
                                    {error, Reason}
                            end;
                        {error, Reason} ->
                            _ = file:close(Fd),
                            {error, {position, Reason}}
                    end;
                {error, Reason} ->
                    {error, {open, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

open_record_group_once_file_process(Dir, Path, RecordsRev) ->
    case segment_file_writer_handle(Path) of
        {ok, WriterPid} ->
            Records = lists:reverse(RecordsRev),
            Writes = [encode_record(Record) || Record <- Records],
            case write_record_group_file(Dir, Path, WriterPid, Writes) of
                {ok, OldSize} ->
                    {ok, {Path, OldSize}, offset_entries_for_records(Dir, Path, Records, OldSize, Writes)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _Reason} = Error ->
            Error
    end.

open_record_group_once_wal_nif(Dir, Path, RecordsRev) ->
    case segment_file_size(Path) of
        {ok, OldSize} ->
            case maybe_preallocate_new_segment(Path, OldSize =:= 0) of
                ok ->
                    Records = lists:reverse(RecordsRev),
                    Writes = [encode_record(Record) || Record <- Records],
                    case write_record_group_wal_nif(Dir, Path, OldSize, Writes, OldSize =:= 0) of
                        ok ->
                            {ok, {Path, OldSize}, offset_entries_for_records(Dir, Path, Records, OldSize, Writes)};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

maybe_preallocate_new_segment(_Path, false) ->
    ok;
maybe_preallocate_new_segment(Path, true) ->
    case segment_preallocate_bytes() of
        {ok, 0} ->
            ok;
        {ok, Bytes} ->
            BinaryPath = unicode:characters_to_binary(Path),
            case maybe_run_preallocate_hook(BinaryPath, Bytes) of
                ok -> preallocate_keep_size(BinaryPath, Bytes);
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

segment_preallocate_bytes() ->
    case application:get_env(ferricstore, waraft_segment_log_preallocate_bytes, ?DEFAULT_PREALLOCATE_BYTES) of
        Bytes when is_integer(Bytes), Bytes >= 0 -> {ok, Bytes};
        Other -> {error, {bad_segment_preallocate_bytes, Other}}
    end.

preallocate_keep_size(BinaryPath, Bytes) ->
    try ferricstore_wal_nif:preallocate_keep_size(BinaryPath, Bytes) of
        ok -> ok;
        {error, Reason} -> {error, {preallocate, BinaryPath, Reason}};
        Other -> {error, {preallocate, BinaryPath, Other}}
    catch
        error:nif_not_loaded ->
            preallocate_keep_size_unavailable(BinaryPath);
        Class:Reason -> {error, {preallocate_exception, BinaryPath, Class, Reason}}
    end.

preallocate_keep_size_unavailable(_BinaryPath) ->
    case os:type() of
        {unix, darwin} -> ok;
        _Other -> {error, {preallocate_unavailable, nif_not_loaded}}
    end.

validate_segment_file_for_append(Path) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular}} ->
            ok;
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_path, Path, Type}};
        {error, enoent} ->
            ok;
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

write_record_group_file_persistent(Dir, Path, Fd, OldSize, Writes, NewSegment) ->
    StartedAt = erlang:monotonic_time(),
    Count = length(Writes),
    Bytes = iolist_size(Writes),
    WriteResult =
        case file:write(Fd, Writes) of
            ok ->
                case maybe_run_append_hook(after_write) of
                    ok -> sync_segment_file(Path, Fd);
                    {error, _Reason} = Error -> Error
                end;
            {error, WriteError} ->
                {error, {write, WriteError}}
        end,

    Result =
        case WriteResult of
            ok ->
                maybe_sync_new_segment_dir(Dir, Path, OldSize, NewSegment);
            {error, AppendError} ->
                case rollback_append_fd(Path, Fd, OldSize) of
                    ok -> {error, AppendError};
                    {error, RollbackReason} -> {error, {rollback_failed, AppendError, RollbackReason}}
                end
        end,
    emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment),
    Result.

write_record_group_file_direct(Dir, Path, Fd, OldSize, Writes, NewSegment) ->
    StartedAt = erlang:monotonic_time(),
    Count = length(Writes),
    Bytes = iolist_size(Writes),
    WriteResult =
        case file:write(Fd, Writes) of
            ok ->
                case maybe_run_append_hook(after_write) of
                    ok -> sync_segment_file(Path, Fd);
                    {error, _Reason} = Error -> Error
                end;
            {error, WriteError} ->
                {error, {write, WriteError}}
        end,

    Result = case WriteResult of
        ok ->
            case file:close(Fd) of
                ok ->
                    maybe_sync_new_segment_dir(Dir, Path, OldSize, NewSegment);
                {error, CloseReason} ->
                    rollback_append_path(Path, OldSize, {close, CloseReason})
            end;
        {error, AppendError} ->
            RollbackResult = rollback_append_fd(Path, Fd, OldSize),
            CloseResult = file:close(Fd),
            case {RollbackResult, CloseResult} of
                {ok, _} -> {error, AppendError};
                {{error, RollbackReason}, _} -> {error, {rollback_failed, AppendError, RollbackReason}};
                {_, {error, CloseReason}} -> {error, {rollback_failed, AppendError, {close, CloseReason}}}
            end
    end,
    emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment),
    Result.

write_record_group_file(Dir, Path, WriterPid, Writes) ->
    StartedAt = erlang:monotonic_time(),
    Count = length(Writes),
    Bytes = iolist_size(Writes),
    {Result, NewSegment} = case file_writer_append(WriterPid, Dir, Path, Writes) of
        {ok, OldSize, NewPosition, WasNewSegment} ->
            case update_segment_writer_position(Path, NewPosition) of
                ok ->
                    {{ok, OldSize}, WasNewSegment};
                {error, UpdateReason} ->
                    {{error, {update_writer_position, UpdateReason}}, WasNewSegment}
            end;
        {error, _Reason} = Error ->
            _ = close_writer_for_path(Path),
            {Error, false}
    end,
    emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment),
    Result.

write_record_group_wal_nif(Dir, Path, OldSize, Writes, NewSegment) ->
    StartedAt = erlang:monotonic_time(),
    Count = length(Writes),
    Bytes = iolist_size(Writes),
    WriteResult = write_record_group_wal_nif_once(Dir, Path, OldSize, Writes, Bytes),
    Result =
        case WriteResult of
            ok ->
                maybe_sync_new_segment_dir(Dir, Path, OldSize, NewSegment);
            {error, AppendError} ->
                case close_writer_for_path(Path) of
                    ok ->
                        case rollback_append_path(Path, OldSize, AppendError) of
                            {error, AppendError} -> {error, AppendError};
                            {error, RollbackReason} -> {error, RollbackReason}
                        end;
                    {error, CloseReason} ->
                        {error, {rollback_failed, AppendError, CloseReason}}
                end
        end,
    emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment),
    Result.

write_record_group_wal_nif_once(Dir, Path, OldSize, Writes, Bytes) ->
    case close_inactive_writers_for_dir(Dir, Path) of
        ok ->
            case segment_writer_handle(Path, OldSize) of
                {ok, Handle} ->
                    case wal_nif_write(Handle, Writes) of
                        ok ->
                            case maybe_run_append_hook(after_write) of
                                ok ->
                                    case sync_segment_wal_nif(Path, Handle) of
                                        ok -> update_segment_writer_position(Path, OldSize + Bytes);
                                        {error, _Reason} = Error -> Error
                                    end;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

maybe_sync_new_segment_dir(_Dir, _Path, _OldSize, false) ->
    ok;
maybe_sync_new_segment_dir(Dir, Path, OldSize, true) ->
    case sync_dir(Dir) of
        ok -> ok;
        {error, Reason} -> rollback_append_path(Path, OldSize, {sync_new_segment_dir, Reason})
    end.

rollback_written_groups([]) ->
    ok;
rollback_written_groups([{Path, OldSize} | Rest]) ->
    case rollback_append_path(Path, OldSize, previous_group) of
        {error, previous_group} ->
            rollback_written_groups(Rest);
        {error, Reason} ->
            {error, Reason}
    end.

rollback_append_path(Path, OldSize, OriginalReason) ->
    case validate_existing_segment_file(Path) of
        ok ->
            case file:open(Path, [read, write, raw, binary]) of
                {ok, Fd} ->
                    Result = rollback_append_fd(Path, Fd, OldSize),
                    CloseResult = file:close(Fd),
                    case {Result, CloseResult} of
                        {ok, ok} -> {error, OriginalReason};
                        {{error, RollbackReason}, _} -> {error, {rollback_failed, OriginalReason, RollbackReason}};
                        {_, {error, CloseReason}} -> {error, {rollback_failed, OriginalReason, {close, CloseReason}}}
                    end;
                {error, Reason} ->
                    {error, {rollback_failed, OriginalReason, {open, Reason}}}
            end;
        {error, Reason} ->
            {error, {rollback_failed, OriginalReason, Reason}}
    end.

rollback_append_fd(Path, Fd, OldSize) ->
    BinaryPath = unicode:characters_to_binary(Path),
    case maybe_run_rollback_hook(BinaryPath) of
        ok ->
            case file:position(Fd, OldSize) of
                {ok, _} ->
                    case file:truncate(Fd) of
                        ok -> file:sync(Fd);
                        {error, Reason} -> {error, {truncate, Reason}}
                    end;
                {error, Reason} ->
                    {error, {position, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

segment_file_size(Path) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = Size}} ->
            {ok, Size};
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_path, Path, Type}};
        {error, enoent} ->
            {ok, 0};
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

maybe_poison_after_append_rollback_failure(Dir, {rollback_failed, _OriginalReason, _RollbackReason} = Reason) ->
    poison_segment_log(Dir, Reason);
maybe_poison_after_append_rollback_failure(_Dir, Reason) ->
    {error, Reason}.

poison_segment_log(Dir, Reason) ->
    case write_append_failure_marker(Dir, Reason) of
        ok -> {error, Reason};
        {error, MarkerReason} -> {error, {append_failure_marker_failed, Reason, MarkerReason}}
    end.

append_failure_marker_path(Dir) ->
    filename:join(Dir, ?APPEND_FAILURE_MARKER).

check_append_failure_marker(Dir) ->
    Path = append_failure_marker_path(Dir),
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = Size}} when Size =< ?MAX_SEGMENT_METADATA_BYTES ->
            read_append_failure_marker(Path);
        {ok, #file_info{type = regular, size = Size}} ->
            {error, {append_failure_marker_file_too_large, Size, ?MAX_SEGMENT_METADATA_BYTES}};
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_append_failure_marker, Path, Type}};
        {error, enoent} ->
            ok;
        {error, Reason} ->
            {error, {read_append_failure_marker_info, Path, Reason}}
    end.

read_append_failure_marker(Path) ->
    case file:read_file(Path) of
        {ok, Binary} when byte_size(Binary) =< ?MAX_SEGMENT_METADATA_BYTES ->
            try binary_to_term(Binary, [safe]) of
                Metadata -> {error, {segment_log_poisoned, Metadata}}
            catch
                _:Reason -> {error, {segment_log_poisoned, #{path => Path, decode_error => Reason}}}
            end;
        {ok, Binary} ->
            {error, {segment_log_poisoned, #{path => Path, marker_bytes => byte_size(Binary)}}};
        {error, Reason} ->
            {error, {read_append_failure_marker, Path, Reason}}
    end.

write_append_failure_marker(Dir, Reason) ->
    MarkerPath = append_failure_marker_path(Dir),
    TmpPath = MarkerPath ++ ".tmp." ++ unique_suffix(),
    Marker = #{
        version => 1,
        reason => Reason
    },
    case filelib:ensure_dir(MarkerPath) of
        ok ->
            case write_file_sync(TmpPath, term_to_binary(Marker)) of
                ok ->
                    case rename_path(TmpPath, MarkerPath) of
                        ok -> sync_dir(Dir);
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    _ = delete_file_if_exists(TmpPath),
                    Error
            end;
        {error, EnsureReason} ->
            {error, {ensure_append_failure_marker_dir, EnsureReason}}
    end.

rewrite_records(Dir, Records) ->
    case validate_segment_log_dir(Dir) of
        ok ->
            case recover_rewrite(Dir) of
                ok ->
                    case validate_segment_log_dir(Dir) of
                        ok ->
                            case records_per_segment(Dir) of
                                {ok, RecordsPerSegment} ->
                                    rewrite_records_atomic(Dir, Records, RecordsPerSegment);
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error -> Error
    end.

rewrite_ets_records_before(Dir, Name, Index) ->
    rewrite_ets_records(Dir, Name, ets:first(Name), fun(Key) -> Key < Index end).

rewrite_ets_records_at_or_after(Dir, Name, Index) ->
    StartKey =
        case ets:lookup(Name, Index) of
            [{Index, _Entry}] -> Index;
            [] -> ets:next(Name, Index)
        end,
    rewrite_ets_records(Dir, Name, StartKey, fun(_Key) -> true end).

rewrite_ets_records(Dir, Name, StartKey, KeepFun) ->
    case validate_segment_log_dir(Dir) of
        ok ->
            case recover_rewrite(Dir) of
                ok ->
                    case validate_segment_log_dir(Dir) of
                        ok ->
                            case records_per_segment(Dir) of
                                {ok, RecordsPerSegment} ->
                                    rewrite_ets_records_atomic(Dir, Name, StartKey, KeepFun, RecordsPerSegment);
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error -> Error
    end.

rewrite_records_atomic(Dir, Records, RecordsPerSegment) ->
    Paths = rewrite_paths(Dir),
    Staging = maps:get(staging, Paths),
    Backup = maps:get(backup, Paths),
    case prepare_rewrite_stage(Staging, Records, RecordsPerSegment) of
        ok ->
            case write_rewrite_marker(Dir, Paths) of
                ok ->
                    case swap_rewrite_dirs(Dir, Staging, Backup) of
                        ok ->
                            case finish_rewrite(Dir, Backup) of
                                ok ->
                                    rebuild_offset_registry(Dir);
                                {error, _Reason} = Error ->
                                    _ = rollback_rewrite(Dir, Paths),
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            _ = rollback_rewrite(Dir, Paths),
                            Error
                    end;
                {error, _Reason} = Error ->
                    _ = remove_tree(Staging),
                    Error
            end;
        {error, _Reason} = Error ->
            _ = remove_tree(Staging),
            Error
    end.

rewrite_ets_records_atomic(Dir, Name, StartKey, KeepFun, RecordsPerSegment) ->
    Paths = rewrite_paths(Dir),
    Staging = maps:get(staging, Paths),
    Backup = maps:get(backup, Paths),
    case prepare_rewrite_stage_from_ets(Staging, Name, StartKey, KeepFun, RecordsPerSegment) of
        ok ->
            case write_rewrite_marker(Dir, Paths) of
                ok ->
                    case swap_rewrite_dirs(Dir, Staging, Backup) of
                        ok ->
                            case finish_rewrite(Dir, Backup) of
                                ok ->
                                    rebuild_offset_registry(Dir);
                                {error, _Reason} = Error ->
                                    _ = rollback_rewrite(Dir, Paths),
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            _ = rollback_rewrite(Dir, Paths),
                            Error
                    end;
                {error, _Reason} = Error ->
                    _ = remove_tree(Staging),
                    Error
            end;
        {error, _Reason} = Error ->
            _ = remove_tree(Staging),
            Error
    end.

prepare_rewrite_stage(Staging, Records, RecordsPerSegment) ->
    case remove_tree(Staging) of
        ok ->
            case filelib:ensure_dir(filename:join(Staging, "dummy")) of
                ok ->
                    case write_segment_config(Staging, RecordsPerSegment) of
                        ok ->
                            case write_records_with_segment_size(Staging, Records, RecordsPerSegment) of
                                ok ->
                                    case close_writers_for_dir(Staging) of
                                        ok -> sync_dir(Staging);
                                        {error, _Reason} = Error -> Error
                                    end;
                                {error, _Reason} = Error -> Error
                            end;
                        {error, _Reason} = Error -> Error
                    end;
                {error, Reason} ->
                    {error, {ensure_staging_dir, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

prepare_rewrite_stage_from_ets(Staging, Name, StartKey, KeepFun, RecordsPerSegment) ->
    case remove_tree(Staging) of
        ok ->
            case filelib:ensure_dir(filename:join(Staging, "dummy")) of
                ok ->
                    case write_segment_config(Staging, RecordsPerSegment) of
                        ok ->
                            case write_ets_records_with_segment_size(
                                Staging,
                                Name,
                                StartKey,
                                KeepFun,
                                RecordsPerSegment
                            ) of
                                ok ->
                                    case close_writers_for_dir(Staging) of
                                        ok -> sync_dir(Staging);
                                        {error, _Reason} = Error -> Error
                                    end;
                                {error, _Reason} = Error -> Error
                            end;
                        {error, _Reason} = Error -> Error
                    end;
                {error, Reason} ->
                    {error, {ensure_staging_dir, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

write_ets_records_with_segment_size(Dir, Name, StartKey, KeepFun, RecordsPerSegment) ->
    case write_ets_records_loop(Dir, Name, StartKey, KeepFun, RecordsPerSegment, undefined, [], []) of
        {ok, _Rollbacks} -> ok;
        {error, _Reason} = Error -> Error
    end.

write_ets_records_loop(Dir, _Name, '$end_of_table', _KeepFun, _RecordsPerSegment, Ordinal, RecordsRev, Rollbacks) ->
    finish_streamed_record_group(Dir, Ordinal, RecordsRev, Rollbacks);
write_ets_records_loop(Dir, Name, Key, KeepFun, RecordsPerSegment, Ordinal, RecordsRev, Rollbacks) ->
    case KeepFun(Key) of
        true ->
            NextKey = ets:next(Name, Key),
            case ets:lookup(Name, Key) of
                [{Key, Entry}] ->
                    RecordOrdinal = segment_ordinal(Key, RecordsPerSegment),
                    case Ordinal of
                        undefined ->
                            write_ets_records_loop(
                                Dir,
                                Name,
                                NextKey,
                                KeepFun,
                                RecordsPerSegment,
                                RecordOrdinal,
                                [{Key, Entry}],
                                Rollbacks
                            );
                        RecordOrdinal ->
                            write_ets_records_loop(
                                Dir,
                                Name,
                                NextKey,
                                KeepFun,
                                RecordsPerSegment,
                                Ordinal,
                                [{Key, Entry} | RecordsRev],
                                Rollbacks
                            );
                        _OtherOrdinal ->
                            case finish_streamed_record_group(Dir, Ordinal, RecordsRev, Rollbacks) of
                                {ok, NextRollbacks} ->
                                    write_ets_records_loop(
                                        Dir,
                                        Name,
                                        NextKey,
                                        KeepFun,
                                        RecordsPerSegment,
                                        RecordOrdinal,
                                        [{Key, Entry}],
                                        NextRollbacks
                                    );
                                {error, _Reason} = Error ->
                                    Error
                            end
                    end;
                [] ->
                    write_ets_records_loop(
                        Dir,
                        Name,
                        NextKey,
                        KeepFun,
                        RecordsPerSegment,
                        Ordinal,
                        RecordsRev,
                        Rollbacks
                    )
            end;
        false ->
            finish_streamed_record_group(Dir, Ordinal, RecordsRev, Rollbacks)
    end.

finish_streamed_record_group(_Dir, undefined, [], Rollbacks) ->
    {ok, Rollbacks};
finish_streamed_record_group(_Dir, _Ordinal, [], Rollbacks) ->
    {ok, Rollbacks};
finish_streamed_record_group(Dir, Ordinal, RecordsRev, Rollbacks) ->
    Segment = segment_file_from_ordinal(Ordinal),
    case write_record_group_once(Dir, Segment, RecordsRev) of
        {ok, Rollback, _Offsets} ->
            {ok, [Rollback | Rollbacks]};
        {error, Reason} ->
            _ = rollback_written_groups(Rollbacks),
            {error, Reason}
    end.

swap_rewrite_dirs(Dir, Staging, Backup) ->
    case move_live_to_backup(Dir, Backup) of
        ok ->
            case maybe_run_rewrite_hook(after_live_backup) of
                ok ->
                    case rename_path(Staging, Dir) of
                        ok -> sync_dir(filename:dirname(Dir));
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

move_live_to_backup(Dir, Backup) ->
    case path_exists(Dir) of
        true ->
            case remove_tree(Backup) of
                ok ->
                    case rename_path(Dir, Backup) of
                        ok -> sync_dir(filename:dirname(Dir));
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        false ->
            ok;
        {error, _Reason} = Error ->
            Error
    end.

finish_rewrite(Dir, Backup) ->
    case delete_file_if_exists(rewrite_marker_path(Dir)) of
        ok ->
            case sync_dir(filename:dirname(Dir)) of
                ok ->
                    case remove_tree(Backup) of
                        ok -> sync_dir(filename:dirname(Dir));
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

recover_rewrite(Dir) ->
    MarkerPath = rewrite_marker_path(Dir),
    case read_segment_metadata_file(MarkerPath, rewrite_marker_file_too_large) of
        {ok, Binary} ->
            try binary_to_term(Binary, [safe]) of
                #{version := 1, dir := Dir, staging := Staging, backup := Backup} = Marker
                  when is_list(Staging), is_list(Backup) ->
                    case validate_rewrite_marker_paths(Dir, Staging, Backup) of
                        ok -> rollback_rewrite(Dir, Marker);
                        {error, _Reason} = Error -> Error
                    end;
                Other ->
                    {error, {bad_rewrite_marker, Other}}
            catch
                _:Reason ->
                    {error, {bad_rewrite_marker, Reason}}
            end;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            {error, {read_rewrite_marker, Reason}}
    end.

rollback_rewrite(Dir, #{staging := Staging, backup := Backup}) ->
    case path_exists(Backup) of
        true ->
            case remove_tree(Dir) of
                ok ->
                    case rename_path(Backup, Dir) of
                        ok -> cleanup_rewrite_marker(Dir, Staging);
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        false ->
            recover_without_backup(Dir, Staging);
        {error, _Reason} = Error ->
            Error
    end.

recover_without_backup(Dir, Staging) ->
    case path_exists(Dir) of
        true ->
            cleanup_rewrite_marker(Dir, Staging);
        false ->
            case path_exists(Staging) of
                true ->
                    case rename_path(Staging, Dir) of
                        ok -> cleanup_rewrite_marker(Dir, Staging);
                        {error, _Reason} = Error -> Error
                    end;
                false ->
                    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
                        ok -> cleanup_rewrite_marker(Dir, Staging);
                        {error, Reason} -> {error, {ensure_recovered_dir, Reason}}
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

validate_rewrite_marker_paths(Dir, Staging, Backup) ->
    case validate_rewrite_marker_path(Dir, Staging, ?REWRITE_STAGING_PREFIX) of
        ok -> validate_rewrite_marker_path(Dir, Backup, ?REWRITE_BACKUP_PREFIX);
        {error, _Reason} = Error -> Error
    end.

validate_rewrite_marker_path(Dir, Path, Prefix) when is_list(Path) ->
    AbsDir = filename:absname(Dir),
    Parent = filename:dirname(AbsDir),
    Base = filename:basename(AbsDir),
    AbsPath = filename:absname(Path),
    IsValid = filename:dirname(AbsPath) =:= Parent andalso
        lists:prefix(Base ++ Prefix, filename:basename(AbsPath)),
    case IsValid of
        true -> ok;
        false -> {error, {bad_rewrite_marker_path, Path}}
    end;
validate_rewrite_marker_path(_Dir, Path, _Prefix) ->
    {error, {bad_rewrite_marker_path, Path}}.

cleanup_rewrite_marker(Dir, Staging) ->
    _ = remove_tree(Staging),
    case delete_file_if_exists(rewrite_marker_path(Dir)) of
        ok -> sync_dir(filename:dirname(Dir));
        {error, _Reason} = Error -> Error
    end.

write_rewrite_marker(Dir, Paths) ->
    MarkerPath = rewrite_marker_path(Dir),
    TmpPath = MarkerPath ++ ".tmp." ++ unique_suffix(),
    Marker = Paths#{version => 1, dir => Dir},
    case write_file_sync(TmpPath, term_to_binary(Marker)) of
        ok ->
            case rename_path(TmpPath, MarkerPath) of
                ok -> sync_dir(filename:dirname(Dir));
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error ->
            _ = delete_file_if_exists(TmpPath),
            Error
    end.

rewrite_paths(Dir) ->
    Parent = filename:dirname(Dir),
    Base = filename:basename(Dir),
    Suffix = unique_suffix(),
    #{
        staging => filename:join(Parent, Base ++ ?REWRITE_STAGING_PREFIX ++ Suffix),
        backup => filename:join(Parent, Base ++ ?REWRITE_BACKUP_PREFIX ++ Suffix)
    }.

load_segments(Dir, Name) ->
    case records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            case segment_paths(Dir) of
                {ok, Paths} ->
                    case load_segment_paths(Paths, Name, undefined, RecordsPerSegment) of
                        {ok, _LastIndex} -> ok;
                        {error, _Reason} = Error -> Error
                    end;
                {error, enoent} -> ok;
                {error, Reason} = Error ->
                    emit_corrupt_segment(Dir, Reason),
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

existing_records_per_segment(Dir) ->
    CacheKey = segment_config_cache_key(Dir),
    case persistent_term:get(CacheKey, undefined) of
        Value when is_integer(Value), Value > 0 ->
            {ok, Value};
        undefined ->
            ConfigPath = segment_config_path(Dir),
            case read_segment_metadata_file(ConfigPath, segment_config_file_too_large) of
                {ok, Binary} ->
                    case decode_segment_config(Binary) of
                        {ok, Value} ->
                            persistent_term:put(CacheKey, Value),
                            {ok, Value};
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, enoent} ->
                    case segment_paths(Dir) of
                        {ok, []} -> not_found;
                        {ok, _ExistingSegments} -> {error, {missing_segment_config, Dir}};
                        {error, enoent} -> not_found;
                        {error, _Reason} = Error -> Error
                    end;
                {error, Reason} ->
                    {error, {read_segment_config, Reason}}
            end
    end.

read_disk_record(Dir, Index, RecordsPerSegment) ->
    Ordinal = segment_ordinal(Index, RecordsPerSegment),
    Path = filename:join(Dir, segment_file_from_ordinal(Ordinal)),
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    Result =
                        try read_disk_record_fd(Fd, Path, Index, 0, FileBytes, Ordinal, RecordsPerSegment) of
                            ReadResult -> ReadResult
                        after
                            _ = file:close(Fd)
                        end,
                    case Result of
                        {error, Reason} ->
                            emit_corrupt_segment(Path, Reason),
                            Result;
                        _Other ->
                            Result
                    end;
                {error, Reason} ->
                    {error, {open_segment, Reason}}
            end;
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_path, Path, Type}};
        {error, enoent} ->
            not_found;
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

read_disk_record_at(Dir, Index, Offset, EncodedSize, RecordsPerSegment) ->
    Ordinal = segment_ordinal(Index, RecordsPerSegment),
    Path = filename:join(Dir, segment_file_from_ordinal(Ordinal)),
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    Result =
                        try read_disk_record_at_fd(Fd, Path, Index, Offset, EncodedSize, FileBytes, Ordinal, RecordsPerSegment) of
                            ReadResult -> ReadResult
                        after
                            _ = file:close(Fd)
                        end,
                    case Result of
                        {error, Reason} ->
                            emit_corrupt_segment(Path, Reason),
                            Result;
                        _Other ->
                            Result
                    end;
                {error, Reason} ->
                    {error, {open_segment, Reason}}
            end;
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_path, Path, Type}};
        {error, enoent} ->
            not_found;
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

read_disk_record_at_fd(Fd, Path, WantedIndex, Offset, EncodedSize, FileBytes, Ordinal, RecordsPerSegment) ->
    case Offset + EncodedSize > FileBytes of
        true ->
            not_found;
        false ->
            case file:pread(Fd, Offset, ?RECORD_HEADER_SIZE) of
                {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
                    ExpectedSize = ?RECORD_HEADER_SIZE + Len,
                    case {Len > ?MAX_RECORD_BYTES, ExpectedSize =:= EncodedSize} of
                        {true, _} ->
                            {error, {record_too_large, Offset, Len}};
                        {_, false} ->
                            {error, {record_size_mismatch, Offset, EncodedSize, ExpectedSize}};
                        {false, true} ->
                            read_disk_record_at_payload(
                                Fd,
                                Path,
                                WantedIndex,
                                Offset,
                                Ordinal,
                                RecordsPerSegment,
                                Len,
                                Crc
                            )
                    end;
                {ok, Header} ->
                    {error, {short_record_header, Offset, byte_size(Header)}};
                eof ->
                    not_found;
                {error, Reason} ->
                    {error, {read_record_header, Offset, Reason}}
            end
    end.

read_disk_record_at_payload(Fd, Path, WantedIndex, Offset, Ordinal, RecordsPerSegment, Len, Crc) ->
    case file:pread(Fd, Offset + ?RECORD_HEADER_SIZE, Len) of
        {ok, Payload} when byte_size(Payload) =:= Len ->
            case erlang:crc32(Payload) of
                Crc ->
                    try binary_to_term(Payload, [safe]) of
                        {Index, {_Term, _Op} = Entry} when is_integer(Index), Index >= 0 ->
                            case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                                ok when Index =:= WantedIndex ->
                                    {ok, Entry};
                                ok ->
                                    {error, {record_index_mismatch, Offset, WantedIndex, Index}};
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        Other ->
                            {error, {bad_record, Other}}
                    catch
                        _:Reason ->
                            {error, {bad_term, Reason}}
                    end;
                _Mismatch ->
                    {error, {crc_mismatch, Offset}}
            end;
        {ok, Payload} ->
            {error, {short_record_read, Offset, Len, byte_size(Payload)}};
        eof ->
            not_found;
        {error, Reason} ->
            {error, {read_record_payload, Offset, Reason}}
    end.

read_disk_record_fd(Fd, Path, WantedIndex, Offset, FileBytes, Ordinal, RecordsPerSegment) ->
    case file:read(Fd, ?RECORD_HEADER_SIZE) of
        eof ->
            not_found;
        {ok, Header} when byte_size(Header) < ?RECORD_HEADER_SIZE ->
            not_found;
        {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
            case Len > ?MAX_RECORD_BYTES of
                true ->
                    {error, {record_too_large, Offset, Len}};
                false ->
                    case Offset + ?RECORD_HEADER_SIZE + Len > FileBytes of
                        true ->
                            not_found;
                        false ->
                            case file:read(Fd, Len) of
                                {ok, Payload} when byte_size(Payload) =:= Len ->
                                    read_disk_record_payload(
                                        Fd,
                                        Path,
                                        WantedIndex,
                                        Offset,
                                        FileBytes,
                                        Ordinal,
                                        RecordsPerSegment,
                                        Len,
                                        Crc,
                                        Payload
                                    );
                                {ok, Payload} ->
                                    {error, {short_record_read, Offset, Len, byte_size(Payload)}};
                                eof ->
                                    not_found;
                                {error, Reason} ->
                                    {error, {read_record_payload, Offset, Reason}}
                            end
                    end
            end;
        {error, Reason} ->
            {error, {read_record_header, Offset, Reason}}
    end.

read_disk_record_payload(Fd, Path, WantedIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, Len, Crc, Payload) ->
    case erlang:crc32(Payload) of
        Crc ->
            try binary_to_term(Payload, [safe]) of
                {Index, {_Term, _Op} = Entry} when is_integer(Index), Index >= 0 ->
                    case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                        ok when Index =:= WantedIndex ->
                            {ok, Entry};
                        ok when Index < WantedIndex ->
                            read_disk_record_fd(
                                Fd,
                                Path,
                                WantedIndex,
                                Offset + ?RECORD_HEADER_SIZE + Len,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment
                            );
                        ok ->
                            not_found;
                        {error, _Reason} = Error ->
                            Error
                    end;
                Other ->
                    {error, {bad_record, Other}}
            catch
                _:Reason ->
                    {error, {bad_term, Reason}}
            end;
        _Mismatch ->
            {error, {crc_mismatch, Offset}}
    end.

load_segment_paths([], _Name, LastIndex, _RecordsPerSegment) ->
    {ok, LastIndex};
load_segment_paths([{Ordinal, Path} | Rest], Name, PreviousIndex, RecordsPerSegment) ->
    case load_segment(Ordinal, Path, Name, PreviousIndex, RecordsPerSegment) of
        {ok, LastIndex} -> load_segment_paths(Rest, Name, LastIndex, RecordsPerSegment);
        {error, _Reason} = Error -> Error
    end.

load_segment(Ordinal, Path, Name, PreviousIndex, RecordsPerSegment) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    try load_segment_fd(Fd, Path, Name, PreviousIndex, 0, FileBytes, Ordinal, RecordsPerSegment) of
                        {ok, LastIndex, ValidBytes} ->
                            case maybe_truncate(Path, ValidBytes, FileBytes) of
                                ok -> {ok, LastIndex};
                                {error, _Reason} = Error -> Error
                            end;
                        {error, Reason} = Error ->
                            emit_corrupt_segment(Path, Reason),
                            Error
                    after
                        _ = file:close(Fd)
                    end;
                {error, Reason} ->
                    {error, {open_segment, Reason}}
            end;
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_path, Path, Type}};
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

load_segment_fd(Fd, Path, Name, PreviousIndex, Offset, FileBytes, Ordinal, RecordsPerSegment) ->
    case file:read(Fd, ?RECORD_HEADER_SIZE) of
        eof ->
            {ok, PreviousIndex, Offset};
        {ok, Header} when byte_size(Header) < ?RECORD_HEADER_SIZE ->
            {ok, PreviousIndex, Offset};
        {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
            case Len > ?MAX_RECORD_BYTES andalso PreviousIndex =:= undefined of
                true ->
                    {error, {record_too_large, Offset, Len}};
                false ->
                    case Offset + ?RECORD_HEADER_SIZE + Len > FileBytes of
                        true ->
                            {ok, PreviousIndex, Offset};
                        false ->
                            case file:read(Fd, Len) of
                                {ok, Payload} when byte_size(Payload) =:= Len ->
                                    load_segment_payload(
                                        Fd,
                                        Path,
                                        Name,
                                        PreviousIndex,
                                        Offset,
                                        FileBytes,
                                        Ordinal,
                                        RecordsPerSegment,
                                        Len,
                                        Crc,
                                        Payload
                                    );
                                {ok, Payload} ->
                                    {error, {short_record_read, Offset, Len, byte_size(Payload)}};
                                eof ->
                                    {ok, PreviousIndex, Offset};
                                {error, Reason} ->
                                    {error, {read_record_payload, Offset, Reason}}
                            end
                    end
            end;
        {error, Reason} ->
            {error, {read_record_header, Offset, Reason}}
    end.

load_segment_payload(Fd, Path, Name, PreviousIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, Len, Crc, Payload) ->
    case erlang:crc32(Payload) of
        Crc ->
            try binary_to_term(Payload, [safe]) of
                {Index, {_Term, _Op} = Entry} when is_integer(Index), Index >= 0 ->
                    case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                        ok ->
                            case register_record_offset(filename:dirname(Path), Index, Ordinal, Offset, ?RECORD_HEADER_SIZE + Len) of
                                ok ->
                                    case insert_recovered_record(Name, {Index, Entry}, PreviousIndex) of
                                        {ok, LastIndex} ->
                                            load_segment_fd(
                                                Fd,
                                                Path,
                                                Name,
                                                LastIndex,
                                                Offset + ?RECORD_HEADER_SIZE + Len,
                                                FileBytes,
                                                Ordinal,
                                                RecordsPerSegment
                                            );
                                        {error, _Reason} = Error ->
                                            Error
                                    end;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                Other ->
                    {error, {bad_record, Other}}
            catch
                _:Reason ->
                    {error, {bad_term, Reason}}
            end;
        _Mismatch ->
            {error, {crc_mismatch, Offset}}
    end.

validate_record_segment_ordinal(Path, Index, ExpectedOrdinal, RecordsPerSegment) ->
    ActualOrdinal = segment_ordinal(Index, RecordsPerSegment),
    case ActualOrdinal of
        ExpectedOrdinal ->
            ok;
        _Other ->
            {error, {segment_ordinal_mismatch, Path, Index, ExpectedOrdinal, ActualOrdinal}}
    end.

insert_recovered_record(Name, {Index, _Entry} = Record, PreviousIndex) ->
    case ets:lookup(Name, Index) of
        [] ->
            case recovered_index_contiguous(PreviousIndex, Index) of
                ok ->
                    true = ets:insert(Name, Record),
                    {ok, Index};
                {error, _Reason} = Error ->
                    Error
            end;
        [_Existing] ->
            {error, {duplicate_record_index, Index}}
    end.

recovered_index_contiguous(undefined, _Index) ->
    ok;
recovered_index_contiguous(PreviousIndex, Index) when Index =:= PreviousIndex + 1 ->
    ok;
recovered_index_contiguous(PreviousIndex, Index) ->
    {error, {non_contiguous_record_index, PreviousIndex, Index}}.

maybe_truncate(Path, ValidBytes, FileBytes) when ValidBytes < FileBytes ->
    case validate_existing_segment_file(Path) of
        ok ->
            case file:open(Path, [read, write, raw, binary]) of
                {ok, Fd} ->
                    Result = file:position(Fd, ValidBytes),
                    TruncResult =
                        case Result of
                            {ok, ValidBytes} -> file:truncate(Fd);
                            {error, Reason} -> {error, {position, Reason}}
                        end,
                    SyncResult =
                        case TruncResult of
                            ok -> file:sync(Fd);
                            {error, _Reason} = Error -> Error
                        end,
                    CloseResult = file:close(Fd),
                    case {SyncResult, CloseResult} of
                        {ok, ok} -> ok;
                        {{error, TruncReason}, _} -> {error, TruncReason};
                        {_, {error, CloseReason}} -> {error, {close, CloseReason}}
                    end;
                {error, Reason} ->
                    {error, {open_truncate, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end;
maybe_truncate(_Path, _ValidBytes, _FileBytes) ->
    ok.

validate_existing_segment_file(Path) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular}} ->
            ok;
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_path, Path, Type}};
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

validate_segment_log_dir(Dir) ->
    case file:read_link_info(Dir) of
        {ok, #file_info{type = directory}} ->
            ok;
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_log_dir, Dir, Type}};
        {error, enoent} ->
            ok;
        {error, Reason} ->
            {error, {read_segment_log_dir, Dir, Reason}}
    end.

encode_record({Index, Entry}) ->
    Payload = term_to_binary({Index, Entry}),
    Len = byte_size(Payload),
    Crc = erlang:crc32(Payload),
    <<Len:32/unsigned-big, Crc:32/unsigned-big, Payload/binary>>.

offset_entries_for_records(Dir, Path, Records, OldSize, Writes) ->
    {ok, Ordinal} = parse_segment_ordinal(filename:basename(Path, ?SEGMENT_EXT)),
    DirKey = offset_dir_key(Dir),
    offset_entries_for_records(DirKey, Ordinal, Records, Writes, OldSize, []).

offset_entries_for_records(_DirKey, _Ordinal, [], [], _Offset, Acc) ->
    lists:reverse(Acc);
offset_entries_for_records(DirKey, Ordinal, [{Index, _Entry} | Records], [Write | Writes], Offset, Acc) ->
    EncodedSize = iolist_size(Write),
    Entry = offset_entry(DirKey, Index, Ordinal, Offset, EncodedSize),
    offset_entries_for_records(DirKey, Ordinal, Records, Writes, Offset + EncodedSize, [Entry | Acc]).

offset_entry(DirKey, Index, Ordinal, Offset, EncodedSize) ->
    {{DirKey, Index}, Ordinal, Offset, EncodedSize}.

register_record_offset(Dir, Index, Ordinal, Offset, EncodedSize) ->
    register_offset_entries([offset_entry(offset_dir_key(Dir), Index, Ordinal, Offset, EncodedSize)]).

register_offset_entries([]) ->
    ok;
register_offset_entries(Entries) ->
    case ensure_offset_registry() of
        ok ->
            true = ets:insert(?OFFSET_REGISTRY, Entries),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

lookup_offset(Dir, Index) ->
    case ensure_offset_registry() of
        ok ->
            case ets:lookup(?OFFSET_REGISTRY, {offset_dir_key(Dir), Index}) of
                [{{_DirKey, Index}, Ordinal, Offset, EncodedSize}] ->
                    {ok, {Ordinal, Offset, EncodedSize}};
                [] ->
                    not_found
            end;
        {error, _Reason} = Error ->
            Error
    end.

rebuild_offset_registry(Dir) ->
    case ensure_offset_registry() of
        ok ->
            clear_offset_registry_for_dir(Dir),
            Tid = ets:new(?MODULE, [ordered_set]),
            try
                case existing_records_per_segment(Dir) of
                    {ok, RecordsPerSegment} ->
                        case segment_paths(Dir) of
                            {ok, Paths} ->
                                case load_segment_paths(Paths, Tid, undefined, RecordsPerSegment) of
                                    {ok, _LastIndex} -> ok;
                                    {error, _Reason} = Error -> Error
                                end;
                            {error, enoent} -> ok;
                            {error, _Reason} = Error -> Error
                        end;
                    not_found ->
                        ok;
                    {error, _Reason} = Error ->
                        Error
                end
            after
                ets:delete(Tid)
            end;
        {error, _Reason} = Error ->
            Error
    end.

clear_offset_registry_for_dir(Dir) ->
    DirKey = offset_dir_key(Dir),
    ets:match_delete(?OFFSET_REGISTRY, {{DirKey, '_'}, '_', '_', '_'}),
    ok.

offset_dir_key(Dir) ->
    unicode:characters_to_binary(filename:absname(Dir)).

ensure_offset_registry() ->
    case ets:info(?OFFSET_REGISTRY) of
        undefined ->
            try
                ets:new(?OFFSET_REGISTRY, [
                    named_table,
                    public,
                    set,
                    {read_concurrency, true},
                    {write_concurrency, true}
                ]),
                ok
            catch
                error:badarg ->
                    case ets:info(?OFFSET_REGISTRY) of
                        undefined -> {error, offset_registry_unavailable};
                        _Info -> ok
                    end
            end;
        _Info ->
            ok
    end.

segment_paths(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            case segment_file_ordinals(Files, []) of
                {ok, Segments} ->
                    Paths =
                        [{Ordinal, filename:join(Dir, File)} || {Ordinal, File} <- lists:keysort(1, Segments)],
                    {ok, Paths};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

segment_file_ordinals(Files, Acc) ->
    segment_file_ordinals(Files, Acc, #{}).

segment_file_ordinals([], Acc, _Seen) ->
    {ok, Acc};
segment_file_ordinals([File | Rest], Acc, Seen) ->
    case filename:extension(File) of
        ?SEGMENT_EXT ->
            case parse_segment_ordinal(filename:basename(File, ?SEGMENT_EXT)) of
                {ok, Ordinal} ->
                    case maps:find(Ordinal, Seen) of
                        {ok, ExistingFile} ->
                            {error, {duplicate_segment_ordinal, Ordinal, ExistingFile, File}};
                        error ->
                            segment_file_ordinals(Rest, [{Ordinal, File} | Acc], Seen#{Ordinal => File})
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        _Other ->
            segment_file_ordinals(Rest, Acc, Seen)
    end.

parse_segment_ordinal(Base) ->
    case string:to_integer(Base) of
        {Ordinal, []} when is_integer(Ordinal), Ordinal >= 0 ->
            Canonical = integer_to_list(Ordinal),
            case Base of
                Canonical -> {ok, Ordinal};
                _Other -> {error, {noncanonical_segment_filename, Base ++ ?SEGMENT_EXT, Canonical ++ ?SEGMENT_EXT}}
            end;
        _Other ->
            {error, {bad_segment_filename, Base ++ ?SEGMENT_EXT}}
    end.

records_per_segment(Dir) ->
    CacheKey = segment_config_cache_key(Dir),
    case persistent_term:get(CacheKey, undefined) of
        Value when is_integer(Value), Value > 0 ->
            {ok, Value};
        undefined ->
            case load_or_create_segment_config(Dir) of
                {ok, Value} ->
                    persistent_term:put(CacheKey, Value),
                    {ok, Value};
                {error, _Reason} = Error ->
                    Error
            end
    end.

load_or_create_segment_config(Dir) ->
    ConfigPath = segment_config_path(Dir),
    case read_segment_metadata_file(ConfigPath, segment_config_file_too_large) of
        {ok, Binary} ->
            decode_segment_config(Binary);
        {error, enoent} ->
            Value = configured_records_per_segment(),
            case write_segment_config(Dir, Value) of
                ok -> {ok, Value};
                {error, _Reason} = Error -> Error
            end;
        {error, Reason} ->
            {error, {read_segment_config, Reason}}
    end.

preload_segment_config(Dir) ->
    CacheKey = segment_config_cache_key(Dir),
    ConfigPath = segment_config_path(Dir),
    case read_segment_metadata_file(ConfigPath, segment_config_file_too_large) of
        {ok, Binary} ->
            case decode_segment_config(Binary) of
                {ok, Value} ->
                    persistent_term:put(CacheKey, Value),
                    ok;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, enoent} ->
            case segment_paths(Dir) of
                {ok, []} ->
                    _ = persistent_term:erase(CacheKey),
                    ok;
                {ok, _ExistingSegments} ->
                    {error, {missing_segment_config, Dir}};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {read_segment_config, Reason}}
    end.

read_segment_metadata_file(Path, TooLargeReason) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = Size}} when Size =< ?MAX_SEGMENT_METADATA_BYTES ->
            file:read_file(Path);
        {ok, #file_info{type = regular, size = Size}} ->
            {error, {TooLargeReason, Size, ?MAX_SEGMENT_METADATA_BYTES}};
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_segment_metadata_path, Path, Type}};
        {error, Reason} ->
            {error, Reason}
    end.

decode_segment_config(Binary) ->
    try binary_to_term(Binary, [safe]) of
        #{version := 1, records_per_segment := Value}
          when is_integer(Value), Value > 0 ->
            {ok, Value};
        Other ->
            {error, {bad_segment_config, Other}}
    catch
        _:Reason ->
            {error, {bad_segment_config, Reason}}
    end.

configured_records_per_segment() ->
    case application:get_env(ferricstore, waraft_segment_log_records_per_segment) of
        {ok, Value} when is_integer(Value), Value > 0 ->
            Value;
        _Other ->
            ?DEFAULT_SEGMENT_RECORDS
    end.

write_segment_config(Dir, Value) ->
    Path = segment_config_path(Dir),
    TmpPath = Path ++ ".tmp." ++ unique_suffix(),
    Config = #{version => 1, records_per_segment => Value},
    case write_file_sync(TmpPath, term_to_binary(Config)) of
        ok ->
            case rename_path(TmpPath, Path) of
                ok -> sync_dir(Dir);
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error ->
            _ = delete_file_if_exists(TmpPath),
            Error
    end.

segment_config_path(Dir) ->
    filename:join(Dir, ?SEGMENT_CONFIG_FILE).

segment_config_cache_key(Dir) ->
    {?MODULE, records_per_segment, filename:absname(Dir)}.

segment_ordinal(Index, RecordsPerSegment) ->
    Index div RecordsPerSegment.

segment_file_from_ordinal(Ordinal) ->
    integer_to_list(Ordinal) ++ ?SEGMENT_EXT.

log_dir(#raft_log{table = Table, partition = Partition}) ->
    filename:join(wa_raft_part_sup:registered_partition_path(Table, Partition), "segment_log").

rewrite_marker_path(Dir) ->
    filename:join(filename:dirname(Dir), filename:basename(Dir) ++ ?REWRITE_MARKER_EXT).

path_exists(Path) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = directory}} -> true;
        {ok, #file_info{type = Type}} -> {error, {unsafe_rewrite_path, Path, Type}};
        {error, enoent} -> false;
        {error, Reason} -> {error, {read_link_info, Path, Reason}}
    end.

remove_tree(Path) ->
    case path_exists(Path) of
        true ->
            case file:del_dir_r(Path) of
                ok -> ok;
                {error, Reason} -> {error, {remove_tree, Path, Reason}}
            end;
        false ->
            ok;
        {error, _Reason} = Error ->
            Error
    end.

rename_path(From, To) ->
    case file:rename(From, To) of
        ok -> ok;
        {error, Reason} -> {error, {rename, From, To, Reason}}
    end.

delete_file_if_exists(Path) ->
    case file:delete(Path) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> {error, {delete_file, Path, Reason}}
    end.

write_file_sync(Path, Binary) ->
    case file:open(Path, [write, raw, binary]) of
        {ok, Fd} ->
            WriteResult =
                case file:write(Fd, Binary) of
                    ok -> file:sync(Fd);
                    {error, Reason} -> {error, {write, Reason}}
                end,
            CloseResult = file:close(Fd),
            case {WriteResult, CloseResult} of
                {ok, ok} -> ok;
                {{error, WriteReason}, _} -> {error, WriteReason};
                {_, {error, CloseReason}} -> {error, {close, CloseReason}}
            end;
        {error, Reason} ->
            {error, {open_write, Reason}}
    end.

sync_dir(Path) ->
    BinaryPath = unicode:characters_to_binary(Path),
    case maybe_run_sync_dir_hook(BinaryPath) of
        ok ->
            try 'Elixir.Ferricstore.Bitcask.NIF':v2_fsync_dir(BinaryPath) of
                ok -> ok;
                {error, Reason} -> {error, {fsync_dir, Path, Reason}};
                Other -> {error, {fsync_dir, Path, Other}}
            catch
                Class:Reason -> {error, {fsync_dir_exception, Path, Class, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

sync_segment_file(Path, Fd) ->
    BinaryPath = unicode:characters_to_binary(Path),
    case segment_sync_method() of
        {ok, Method} ->
            case maybe_run_file_sync_hook(BinaryPath, Method) of
                ok -> sync_segment_fd(Method, Fd);
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

segment_sync_method() ->
    case application:get_env(ferricstore, waraft_segment_log_sync_method, datasync) of
        sync -> {ok, sync};
        datasync -> {ok, datasync};
        auto -> {ok, datasync};
        Other -> {error, {bad_segment_sync_method, Other}}
    end.

sync_segment_fd(sync, Fd) ->
    file:sync(Fd);
sync_segment_fd(datasync, Fd) ->
    file:datasync(Fd).

segment_io_mode() ->
    case application:get_env(ferricstore, waraft_segment_log_io_mode, file) of
        file -> {ok, file};
        wal_nif -> {ok, wal_nif};
        Other -> {error, {bad_segment_io_mode, Other}}
    end.

file_writer_mode() ->
    %% Experimental group-sync path. Keep direct as the default because local
    %% Flow profiles showed the process hop did not beat direct append+sync.
    case application:get_env(ferricstore, waraft_segment_log_file_writer_mode, direct) of
        direct -> direct;
        persistent -> persistent;
        process -> process;
        Other -> {error, {bad_segment_file_writer_mode, Other}}
    end.

segment_file_fd_handle(Path) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    case ets:lookup(Registry, Key) of
        [{Key, _Dir, file_fd, Fd, Position}] ->
            {ok, Fd, Position};
        [{Key, _Dir, _OtherKind, _Handle, _Position}] ->
            case close_writer_for_path(Path) of
                ok -> open_file_fd_writer(Path);
                {error, _Reason} = Error -> Error
            end;
        [{Key, _Dir, _LegacyWalHandle, _LegacyPosition}] ->
            case close_writer_for_path(Path) of
                ok -> open_file_fd_writer(Path);
                {error, _Reason} = Error -> Error
            end;
        [] ->
            open_file_fd_writer(Path)
    end.

open_file_fd_writer(Path) ->
    %% This persistent mode is caller-side and must be closeable from the log
    %% process during shutdown/truncate. A raw fd is tied to its controlling
    %% process, so raw persistent ownership belongs in the file_writer process
    %% mode instead.
    case file:open(Path, [append, binary]) of
        {ok, Fd} ->
            case file:position(Fd, eof) of
                {ok, Position} ->
                    case maybe_preallocate_new_segment(Path, Position =:= 0) of
                        ok ->
                            Registry = ensure_writer_registry(),
                            true = ets:insert(Registry, {writer_key(Path), writer_dir(Path), file_fd, Fd, Position}),
                            {ok, Fd, Position};
                        {error, Reason} ->
                            _ = file:close(Fd),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    _ = file:close(Fd),
                    {error, {position, Reason}}
            end;
        {error, Reason} ->
            {error, {open, Reason}}
    end.

segment_file_writer_handle(Path) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    case ets:lookup(Registry, Key) of
        [{Key, _Dir, file_writer, Pid, _Position}] when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Pid};
                false ->
                    true = ets:delete(Registry, Key),
                    open_file_segment_writer(Path)
            end;
        [{Key, _Dir, _OtherKind, _Handle, _Position}] ->
            case close_writer_for_path(Path) of
                ok -> open_file_segment_writer(Path);
                {error, _Reason} = Error -> Error
            end;
        [{Key, _Dir, _LegacyWalHandle, _LegacyPosition}] ->
            case close_writer_for_path(Path) of
                ok -> open_file_segment_writer(Path);
                {error, _Reason} = Error -> Error
            end;
        [] ->
            open_file_segment_writer(Path)
    end.

open_file_segment_writer(Path) ->
    Ref = make_ref(),
    Parent = self(),
    Pid = spawn(fun() -> file_writer_init(Parent, Ref, Path) end),
    receive
        {Ref, {ok, Position}} ->
            Registry = ensure_writer_registry(),
            true = ets:insert(Registry, {writer_key(Path), writer_dir(Path), file_writer, Pid, Position}),
            {ok, Pid};
        {Ref, {error, _Reason} = Error} ->
            Error
    after file_writer_call_timeout_ms() ->
        {error, {file_writer_open_timeout, Path, file_writer_call_timeout_ms()}}
    end.

file_writer_init(Parent, Ref, Path) ->
    case file:open(Path, [append, raw, binary]) of
        {ok, Fd} ->
            case file:position(Fd, eof) of
                {ok, Position} ->
                    case maybe_preallocate_new_segment(Path, Position =:= 0) of
                        ok ->
                            Parent ! {Ref, {ok, Position}},
                            file_writer_loop(Path, Fd, Position);
                        {error, Reason} ->
                            _ = file:close(Fd),
                            Parent ! {Ref, {error, Reason}}
                    end;
                {error, Reason} ->
                    _ = file:close(Fd),
                    Parent ! {Ref, {error, {position, Reason}}}
            end;
        {error, Reason} ->
            Parent ! {Ref, {error, {open, Reason}}}
    end.

file_writer_append(Pid, Dir, Path, Writes) ->
    Ref = make_ref(),
    Pid ! {append, self(), Ref, Dir, Path, Writes},
    receive
        {Ref, Reply} -> Reply
    after file_writer_call_timeout_ms() ->
        {error, {file_writer_timeout, Path, file_writer_call_timeout_ms()}}
    end.

file_writer_loop(Path, Fd, Position) ->
    receive
        {append, From, Ref, Dir, Path, Writes} ->
            Requests = collect_file_writer_requests([{From, Ref, Dir, Writes}], Path),
            case file_writer_flush(Path, Fd, Position, Requests) of
                {ok, NewPosition} ->
                    file_writer_loop(Path, Fd, NewPosition);
                {error, _Reason} ->
                    _ = file:close(Fd),
                    ok
            end;
        {close, From, Ref} ->
            From ! {Ref, file:close(Fd)},
            ok
    end.

collect_file_writer_requests(Acc, Path) ->
    receive
        {append, From, Ref, Dir, Path, Writes} ->
            collect_file_writer_requests([{From, Ref, Dir, Writes} | Acc], Path)
    after file_writer_group_delay_ms() ->
        lists:reverse(Acc)
    end.

file_writer_flush(Path, Fd, Position, Requests) ->
    {Writes, _EndPosition, RepliesRev, AnyNewSegment} =
        lists:foldl(
            fun({From, Ref, Dir, RequestWrites}, {WriteAcc, Pos, ReplyAcc, NewAcc}) ->
                Bytes = iolist_size(RequestWrites),
                Reply = {From, Ref, Pos, Pos + Bytes, Pos =:= 0, Dir},
                {[RequestWrites | WriteAcc], Pos + Bytes, [Reply | ReplyAcc], NewAcc orelse Pos =:= 0}
            end,
            {[], Position, [], false},
            Requests
        ),
    Replies = lists:reverse(RepliesRev),
    WriteResult =
        case file:write(Fd, lists:reverse(Writes)) of
            ok ->
                case maybe_run_append_hook(after_write) of
                    ok -> sync_segment_file(Path, Fd);
                    {error, _HookReason} = HookError -> HookError
                end;
            {error, WriteError} ->
                {error, {write, WriteError}}
        end,
    Result =
        case WriteResult of
            ok ->
                maybe_sync_new_segment_dirs(Path, Fd, Position, Replies, AnyNewSegment);
            {error, AppendError} ->
                case rollback_append_fd(Path, Fd, Position) of
                    ok -> {error, AppendError};
                    {error, RollbackReason} -> {error, {rollback_failed, AppendError, RollbackReason}}
                end
        end,
    reply_file_writer_requests(Replies, Result),
    case Result of
        {ok, NewPosition} -> {ok, NewPosition};
        {error, _FlushReason} = FlushError -> FlushError
    end.

maybe_sync_new_segment_dirs(_Path, _Fd, _Position, Replies, false) ->
    {ok, file_writer_new_position(Replies)};
maybe_sync_new_segment_dirs(Path, Fd, Position, Replies, true) ->
    Dir = file_writer_first_new_segment_dir(Replies),
    case maybe_sync_new_segment_dir(Dir, Path, Position, true) of
        ok ->
            {ok, file_writer_new_position(Replies)};
        {error, Reason} ->
            case rollback_append_fd(Path, Fd, Position) of
                ok -> {error, Reason};
                {error, RollbackReason} -> {error, {rollback_failed, Reason, RollbackReason}}
            end
    end.

file_writer_first_new_segment_dir([{_From, _Ref, _Old, _New, true, Dir} | _Rest]) ->
    Dir;
file_writer_first_new_segment_dir([_Other | Rest]) ->
    file_writer_first_new_segment_dir(Rest).

file_writer_new_position(Replies) ->
    {_From, _Ref, _Old, New, _NewSegment, _Dir} = lists:last(Replies),
    New.

reply_file_writer_requests(Replies, {ok, _FinalPosition}) ->
    lists:foreach(
        fun({From, Ref, Old, New, NewSegment, _Dir}) ->
            From ! {Ref, {ok, Old, New, NewSegment}}
        end,
        Replies
    );
reply_file_writer_requests(Replies, {error, _Reason} = Error) ->
    lists:foreach(
        fun({From, Ref, _Old, _New, _NewSegment, _Dir}) ->
            From ! {Ref, Error}
        end,
        Replies
    ).

segment_writer_handle(Path, OldSize) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    case ets:lookup(Registry, Key) of
        [{Key, _Dir, wal_nif, Handle, OldSize}] ->
            {ok, Handle};
        [{Key, _Dir, _Kind, _Handle, _Position}] ->
            case close_writer_for_path(Path) of
                ok -> open_segment_writer(Path, OldSize);
                {error, _Reason} = Error -> Error
            end;
        [{Key, _Dir, Handle, OldSize}] ->
            {ok, Handle};
        [{Key, _Dir, _Handle, _Position}] ->
            case close_writer_for_path(Path) of
                ok -> open_segment_writer(Path, OldSize);
                {error, _Reason} = Error -> Error
            end;
        [] ->
            open_segment_writer(Path, OldSize)
    end.

open_segment_writer(Path, OldSize) ->
    case wal_nif_default_commit_delay_us() of
        {ok, DelayUs} ->
            case wal_nif_max_buffer_bytes() of
                {ok, MaxBufferBytes} ->
                    BinaryPath = unicode:characters_to_binary(Path),
                    case wal_nif_open_raw_append(BinaryPath, DelayUs, MaxBufferBytes, OldSize) of
                        {ok, Handle} ->
                            Registry = ensure_writer_registry(),
                            true = ets:insert(Registry, {writer_key(Path), writer_dir(Path), wal_nif, Handle, OldSize}),
                            {ok, Handle};
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

wal_nif_open_raw_append(BinaryPath, DelayUs, MaxBufferBytes, OldSize) ->
    try ferricstore_wal_nif:open_raw_append(BinaryPath, DelayUs, MaxBufferBytes, OldSize) of
        {ok, Handle} -> {ok, Handle};
        {error, Reason} -> {error, {wal_nif_open_raw_append, BinaryPath, Reason}};
        Other -> {error, {wal_nif_open_raw_append, BinaryPath, Other}}
    catch
        error:nif_not_loaded -> {error, {wal_nif_open_raw_append, BinaryPath, nif_not_loaded}};
        Class:Reason -> {error, {wal_nif_open_raw_append, BinaryPath, Class, Reason}}
    end.

wal_nif_write(Handle, Writes) ->
    try ferricstore_wal_nif:write(Handle, Writes) of
        ok -> ok;
        {error, Reason} -> {error, {wal_nif_write, Reason}};
        Other -> {error, {wal_nif_write, Other}}
    catch
        Class:Reason -> {error, {wal_nif_write, Class, Reason}}
    end.

sync_segment_wal_nif(Path, Handle) ->
    BinaryPath = unicode:characters_to_binary(Path),
    case wal_nif_sync_delay_us() of
        {ok, DelayUs} ->
            case wal_nif_sync_timeout_ms() of
                {ok, TimeoutMs} ->
                    case maybe_run_wal_nif_sync_hook(BinaryPath, DelayUs) of
                        ok ->
                            Ref = make_ref(),
                            case wal_nif_request_sync(Handle, self(), Ref, DelayUs) of
                                ok ->
                                    receive
                                        {wal_sync_complete, Ref, _SyncedPosition} ->
                                            ok;
                                        {wal_sync_error, Ref, Reason} ->
                                            {error, {wal_nif_sync, BinaryPath, Reason}}
                                    after TimeoutMs ->
                                        {error, {wal_nif_sync_timeout, BinaryPath, TimeoutMs}}
                                    end;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

wal_nif_request_sync(Handle, Pid, Ref, DelayUs) ->
    try ferricstore_wal_nif:sync_with_delay(Handle, Pid, Ref, DelayUs) of
        ok -> ok;
        {error, Reason} -> {error, {wal_nif_sync_request, Reason}};
        Other -> {error, {wal_nif_sync_request, Other}}
    catch
        Class:Reason -> {error, {wal_nif_sync_request, Class, Reason}}
    end.

update_segment_writer_position(Path, Position) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    case ets:lookup(Registry, Key) of
        [{Key, Dir, Kind, Handle, _OldPosition}] ->
            true = ets:insert(Registry, {Key, Dir, Kind, Handle, Position}),
            ok;
        [{Key, Dir, Handle, _OldPosition}] ->
            true = ets:insert(Registry, {Key, Dir, Handle, Position}),
            ok;
        [] ->
            {error, {segment_writer_missing, Path}}
    end.

close_inactive_writers_for_dir(Dir, ActivePath) ->
    Registry = ensure_writer_registry(),
    ActiveKey = writer_key(ActivePath),
    WriterDir = writer_dir_from_dir(Dir),
    maybe_run_writer_registry_hook(before_tab2list, Registry),
    close_writer_entries(entries_to_close(writer_registry_entries(Registry), WriterDir, ActiveKey)).

close_writers_for_dir(Dir) ->
    Registry = ensure_writer_registry(),
    WriterDir = writer_dir_from_dir(Dir),
    maybe_run_writer_registry_hook(before_tab2list, Registry),
    close_writer_entries(entries_to_close(writer_registry_entries(Registry), WriterDir, undefined)).

close_writer_for_path(Path) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    case ets:lookup(Registry, Key) of
        [{Key, _Dir, Kind, Handle, _Position}] -> close_writer_entry(Key, Kind, Handle);
        [{Key, _Dir, Handle, _Position}] -> close_writer_entry(Key, wal_nif, Handle);
        [] -> ok
    end.

entries_to_close(Entries, WriterDir, ActiveKey) ->
    [Spec || Entry <- Entries,
             {ok, Spec} <- [entry_to_close_spec(Entry, WriterDir, ActiveKey)]].

entry_to_close_spec({Key, WriterDir, Kind, Handle, _Position}, WriterDir, ActiveKey)
  when Key =/= ActiveKey ->
    {ok, {Key, Kind, Handle}};
entry_to_close_spec({Key, WriterDir, Handle, _Position}, WriterDir, ActiveKey)
  when Key =/= ActiveKey ->
    {ok, {Key, wal_nif, Handle}};
entry_to_close_spec(_Entry, _WriterDir, _ActiveKey) ->
    skip.

close_writer_entries([]) ->
    ok;
close_writer_entries([{Key, Kind, Handle} | Rest]) ->
    case close_writer_entry(Key, Kind, Handle) of
        ok -> close_writer_entries(Rest);
        {error, _Reason} = Error -> Error
    end.

close_writer_entry(Key, file_writer, Pid) when is_pid(Pid) ->
    Registry = ensure_writer_registry(),
    Ref = make_ref(),
    Pid ! {close, self(), Ref},
    Result =
        receive
            {Ref, ok} ->
                ok;
            {Ref, {error, Reason}} ->
                {error, {file_writer_close, Key, Reason}}
        after file_writer_call_timeout_ms() ->
            case is_process_alive(Pid) of
                false -> ok;
                true -> {error, {file_writer_close_timeout, Key, file_writer_call_timeout_ms()}}
            end
        end,
    case Result of
        ok ->
            ok = delete_writer_entry(Registry, Key),
            ok;
        {error, _Reason} = Error ->
            Error
    end;
close_writer_entry(Key, file_fd, Fd) ->
    Registry = ensure_writer_registry(),
    Result =
        case file:close(Fd) of
            ok -> ok;
            {error, Reason} -> {error, {file_fd_close, Key, Reason}}
        end,
    case Result of
        ok ->
            ok = delete_writer_entry(Registry, Key),
            ok;
        {error, _Reason} = Error ->
            Error
    end;
close_writer_entry(Key, wal_nif, Handle) ->
    Registry = ensure_writer_registry(),
    Result =
        try ferricstore_wal_nif:close(Handle) of
            ok -> ok;
            {error, Reason} -> {error, {wal_nif_close, Key, Reason}};
            Other -> {error, {wal_nif_close, Key, Other}}
        catch
            Class:Reason -> {error, {wal_nif_close, Key, Class, Reason}}
        end,
    case Result of
        ok ->
            ok = delete_writer_entry(Registry, Key),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

delete_writer_entry(Registry, Key) ->
    try ets:delete(Registry, Key) of
        true -> ok
    catch
        %% WARaft shuts down partitions one-for-all, and another partition may
        %% have owned and lost the named ETS table while this close path was
        %% already iterating. At shutdown the handle is already closed; missing
        %% registry cleanup must not crash the log process.
        error:badarg -> ok
    end.

ensure_writer_registry() ->
    case ets:info(?WRITER_REGISTRY) of
        undefined ->
            try ets:new(?WRITER_REGISTRY, [named_table, public, set, {read_concurrency, true}]) of
                Tid -> Tid
            catch
                error:badarg -> ?WRITER_REGISTRY
            end;
        _ ->
            ?WRITER_REGISTRY
    end.

writer_registry_entries(Registry) ->
    try ets:tab2list(Registry) of
        Entries -> Entries
    catch
        %% The registry is a process-owned ETS table. During WARaft one-for-all
        %% shutdown another log process may lose the table after ensure but
        %% before traversal; close is best-effort in that already-cleaned state.
        error:badarg -> []
    end.

writer_key(Path) ->
    filename:absname(Path).

writer_dir(Path) ->
    writer_dir_from_dir(filename:dirname(Path)).

writer_dir_from_dir(Dir) ->
    filename:absname(Dir).

wal_nif_default_commit_delay_us() ->
    non_neg_int_env(wal_commit_delay_us, 6000, bad_wal_commit_delay_us).

wal_nif_sync_delay_us() ->
    case application:get_env(ferricstore, waraft_segment_log_sync_delay_us) of
        {ok, Value} when is_integer(Value), Value >= 0 ->
            {ok, Value};
        {ok, Other} ->
            {error, {bad_segment_sync_delay_us, Other}};
        undefined ->
            %% WARaft already uses wal_commit_delay_us to size its commit batch window.
            %% Do not apply the same delay again at the segment fdatasync boundary.
            {ok, 0}
    end.

wal_nif_sync_timeout_ms() ->
    non_neg_int_env(
        waraft_segment_log_wal_nif_sync_timeout_ms,
        ?DEFAULT_WAL_NIF_SYNC_TIMEOUT_MS,
        bad_segment_wal_nif_sync_timeout_ms
    ).

wal_nif_max_buffer_bytes() ->
    non_neg_int_env(wal_max_buffer_bytes, ?DEFAULT_WAL_NIF_MAX_BUFFER_BYTES, bad_wal_max_buffer_bytes).

file_writer_call_timeout_ms() ->
    case application:get_env(ferricstore, waraft_segment_log_file_writer_timeout_ms, 30000) of
        Value when is_integer(Value), Value >= 0 -> Value;
        _Other -> 30000
    end.

file_writer_group_delay_ms() ->
    case application:get_env(ferricstore, waraft_segment_log_file_writer_group_delay_ms, 0) of
        Value when is_integer(Value), Value >= 0 -> Value;
        _Other -> 0
    end.

non_neg_int_env(Key, Default, ErrorTag) ->
    case application:get_env(ferricstore, Key, Default) of
        Value when is_integer(Value), Value >= 0 -> {ok, Value};
        Other -> {error, {ErrorTag, Other}}
    end.

maybe_run_rewrite_hook(Phase) ->
    case application:get_env(ferricstore, waraft_segment_log_rewrite_hook) of
        {ok, {fail_once_after_live_backup, Notify}} when Phase =:= after_live_backup ->
            application:unset_env(ferricstore, waraft_segment_log_rewrite_hook),
            Notify ! {waraft_segment_log_rewrite_hook, Phase},
            {error, {rewrite_hook, Phase}};
        _ ->
            ok
    end.

maybe_run_append_hook(Phase) ->
    case application:get_env(ferricstore, waraft_segment_log_append_hook) of
        {ok, {fail_once_after_write, Notify}} when Phase =:= after_write ->
            application:unset_env(ferricstore, waraft_segment_log_append_hook),
            Notify ! {waraft_segment_log_append_hook, Phase},
            {error, {append_hook, Phase}};
        {ok, {fail_after_write_count, Target, Notify}} when Phase =:= after_write, is_integer(Target), Target > 0 ->
            maybe_fail_append_after_write_count(Target, Notify, 1);
        {ok, {fail_after_write_count, Target, Notify, Count0}} when Phase =:= after_write, is_integer(Target), Target > 0, is_integer(Count0) ->
            maybe_fail_append_after_write_count(Target, Notify, Count0 + 1);
        _ ->
            ok
    end.

maybe_fail_append_after_write_count(Target, Notify, Count) ->
    Notify ! {waraft_segment_log_append_hook, after_write, Count},
    case Count >= Target of
        true ->
            application:unset_env(ferricstore, waraft_segment_log_append_hook),
            {error, {append_hook, after_write, Count}};
        false ->
            application:set_env(ferricstore, waraft_segment_log_append_hook, {fail_after_write_count, Target, Notify, Count}),
            ok
    end.

maybe_run_rollback_hook(BinaryPath) ->
    case application:get_env(ferricstore, waraft_segment_log_rollback_hook) of
        {ok, {notify, Notify}} ->
            Notify ! {waraft_segment_log_rollback_hook, BinaryPath},
            ok;
        {ok, {fail_once, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_rollback_hook),
            Notify ! {waraft_segment_log_rollback_hook, BinaryPath},
            {error, {rollback_hook, BinaryPath}};
        _ ->
            ok
    end.

maybe_run_preallocate_hook(BinaryPath, Bytes) ->
    case application:get_env(ferricstore, waraft_segment_log_preallocate_hook) of
        {ok, {notify, Notify}} ->
            Notify ! {waraft_segment_log_preallocate, BinaryPath, Bytes},
            ok;
        {ok, {fail_once, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_preallocate_hook),
            Notify ! {waraft_segment_log_preallocate, BinaryPath, Bytes},
            {error, {preallocate_hook, BinaryPath, Bytes}};
        _ ->
            ok
    end.

maybe_run_file_sync_hook(BinaryPath, Method) ->
    case application:get_env(ferricstore, waraft_segment_log_file_sync_hook) of
        {ok, {block, Notify}} ->
            Ref = make_ref(),
            Notify ! {waraft_segment_log_file_sync_blocked, BinaryPath, Method, self(), Ref},
            receive
                {Ref, continue} -> ok;
                {Ref, {error, Reason}} -> {error, {file_sync_hook, BinaryPath, Reason}}
            after file_sync_hook_timeout_ms() ->
                {error, {file_sync_hook_timeout, BinaryPath, file_sync_hook_timeout_ms()}}
            end;
        {ok, {notify, Notify}} ->
            Notify ! {waraft_segment_log_file_sync, BinaryPath},
            ok;
        {ok, {notify_with_method, Notify}} ->
            Notify ! {waraft_segment_log_file_sync, BinaryPath, Method},
            ok;
        {ok, {fail_once, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_file_sync_hook),
            Notify ! {waraft_segment_log_file_sync, BinaryPath},
            {error, {file_sync_hook, BinaryPath}};
        {ok, {fail_on_count, Target, Notify}} when is_integer(Target), Target > 0 ->
            maybe_fail_file_sync_count(BinaryPath, Target, Notify, 1);
        {ok, {fail_on_count, Target, Notify, Count0}}
            when is_integer(Target), Target > 0, is_integer(Count0) ->
            maybe_fail_file_sync_count(BinaryPath, Target, Notify, Count0 + 1);
        _ ->
            ok
    end.

file_sync_hook_timeout_ms() ->
    case application:get_env(ferricstore, waraft_segment_log_file_sync_hook_timeout_ms, 5000) of
        Value when is_integer(Value), Value >= 0 -> Value;
        _Other -> 5000
    end.

maybe_run_wal_nif_sync_hook(BinaryPath, DelayUs) ->
    case application:get_env(ferricstore, waraft_segment_log_wal_nif_sync_hook) of
        {ok, {notify, Notify}} ->
            Notify ! {waraft_segment_log_wal_nif_sync, BinaryPath, DelayUs},
            ok;
        {ok, {fail_once, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_wal_nif_sync_hook),
            Notify ! {waraft_segment_log_wal_nif_sync, BinaryPath, DelayUs},
            {error, {wal_nif_sync_hook, BinaryPath, DelayUs}};
        _ ->
            ok
    end.

maybe_fail_file_sync_count(BinaryPath, Target, Notify, Count) ->
    Notify ! {waraft_segment_log_file_sync, BinaryPath, Count},
    case Count >= Target of
        true ->
            application:unset_env(ferricstore, waraft_segment_log_file_sync_hook),
            {error, {file_sync_hook, BinaryPath, Count}};
        false ->
            application:set_env(ferricstore, waraft_segment_log_file_sync_hook, {fail_on_count, Target, Notify, Count}),
            ok
    end.

maybe_run_sync_dir_hook(BinaryPath) ->
    case application:get_env(ferricstore, waraft_segment_log_sync_dir_hook) of
        {ok, {notify, Notify}} ->
            Notify ! {waraft_segment_log_sync_dir, BinaryPath},
            ok;
        {ok, {fail_once, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_sync_dir_hook),
            Notify ! {waraft_segment_log_sync_dir, BinaryPath},
            {error, {sync_dir_hook, BinaryPath}};
        {ok, {fail_on_count, Target, Notify}} when is_integer(Target), Target > 0 ->
            maybe_fail_sync_dir_count(BinaryPath, Target, Notify, 1);
        {ok, {fail_on_count, Target, Notify, Count0}}
            when is_integer(Target), Target > 0, is_integer(Count0) ->
            maybe_fail_sync_dir_count(BinaryPath, Target, Notify, Count0 + 1);
        _ ->
            ok
    end.

maybe_fail_sync_dir_count(BinaryPath, Target, Notify, Count) ->
    Notify ! {waraft_segment_log_sync_dir, BinaryPath, Count},
    case Count >= Target of
        true ->
            application:unset_env(ferricstore, waraft_segment_log_sync_dir_hook),
            {error, {sync_dir_hook, BinaryPath, Count}};
        false ->
            application:set_env(ferricstore, waraft_segment_log_sync_dir_hook, {fail_on_count, Target, Notify, Count}),
            ok
    end.

maybe_run_writer_registry_hook(Phase, Registry) ->
    case application:get_env(ferricstore, waraft_segment_log_writer_registry_hook) of
        {ok, {delete_once, Phase, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_writer_registry_hook),
            Notify ! {waraft_segment_log_writer_registry_hook, Phase},
            catch ets:delete(Registry),
            ok;
        _ ->
            ok
    end.

unique_suffix() ->
    integer_to_list(erlang:unique_integer([positive, monotonic])).

emit_corrupt_segment(Path, Reason) ->
    Metadata = #{
        path => unicode:characters_to_binary(Path),
        reason => Reason
    },
    emit_telemetry([ferricstore, waraft, segment_log_corrupt], #{count => 1}, Metadata).

emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment) ->
    Duration = erlang:monotonic_time() - StartedAt,
    Metadata0 = #{
        path => unicode:characters_to_binary(Path),
        result => segment_append_result(Result),
        new_segment => NewSegment
    },
    Metadata =
        case Result of
            ok -> Metadata0;
            {ok, _Value} -> Metadata0;
            {error, Reason} -> Metadata0#{reason => Reason};
            Other -> Metadata0#{reason => Other}
        end,
    Measurements = #{
        count => Count,
        bytes => Bytes,
        duration => Duration
    },
    emit_telemetry([ferricstore, waraft, segment_log, append], Measurements, Metadata).

emit_telemetry(Event, Measurements, Metadata) ->
    %% telemetry:execute/3 logs a warning when the telemetry application has not
    %% booted yet. The segment append path is hot, so skip cleanly in that case.
    case persistent_term:get(telemetry, undefined) of
        undefined ->
            ok;
        _ ->
            try telemetry:execute(Event, Measurements, Metadata) of
                _ -> ok
            catch
                _:_ -> ok
            end
    end.

segment_append_result(ok) ->
    ok;
segment_append_result({ok, _Value}) ->
    ok;
segment_append_result({error, _Reason}) ->
    error;
segment_append_result(_Other) ->
    unknown.
