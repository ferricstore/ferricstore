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
    reset_disk_to_position/2,
    memory_status/1,
    ensure_segment_config/1,
    write_projection/3,
    write_projection_batch/3,
    write_projection_batches/2,
    write_projection_batches_sync/2,
    close_process_writers/1
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
-define(TRIM_FLOOR_FILE, "trim_floor.term").
-define(RECORD_HEADER_SIZE, 8).
-define(MAX_RECORD_BYTES, 1073741824).
-define(MAX_SEGMENT_METADATA_BYTES, 1048576).
-define(REWRITE_MARKER_EXT, ".rewrite.term").
-define(REWRITE_STAGING_PREFIX, ".rewrite.staging.").
-define(REWRITE_BACKUP_PREFIX, ".rewrite.backup.").
-define(REWRITE_GROUP_MAX_RECORDS, 128).
-define(APPEND_FAILURE_MARKER, "segment_log.append_failed.term").
-define(DEFAULT_PREALLOCATE_BYTES, 0).
-define(WRITER_REGISTRY, ferricstore_waraft_segment_writer_registry).
-define(OFFSET_REGISTRY, ferricstore_waraft_segment_offset_registry).
-define(MEMORY_REGISTRY, ferricstore_waraft_segment_log_memory_registry).
-define(LOAD_CONTEXT, ferricstore_waraft_segment_log_load_context).
-define(FOLD_CONTEXT, ferricstore_waraft_segment_log_fold_context).
-define(DEFAULT_MAX_ETS_BYTES, 536870912).
-define(DEFAULT_MAX_ETS_ENTRIES, 65536).
-define(DEFAULT_MIN_ETS_ENTRIES, 4096).

first_index(#raft_log{name = Name} = Log) ->
    first_index_for_name(Name, log_dir(Log)).

first_index_for_name(Name, Dir) ->
    case {logical_trim_floor_result(Dir), memory_boundaries(Name, Dir)} of
        {{ok, Floor}, {undefined, _Last}} ->
            case Floor > 0 of
                true -> Floor;
                false -> undefined
            end;
        {{ok, Floor}, {First, _Last}} ->
            max(First, Floor);
        {{error, Reason}, _Bounds} ->
            {error, Reason}
    end.

last_index(#raft_log{name = Name} = Log) ->
    last_index_for_name(Name, log_dir(Log)).

last_index_for_name(Name, Dir) ->
    case memory_boundaries(Name, Dir) of
        {_First, undefined} -> undefined;
        {_First, Last} -> Last
    end.

fold(Log, Start, End, SizeLimit, Func, Acc) ->
    fold_impl(Log, Start, End, 0, SizeLimit, Func, Acc).

fold_binary(Log, Start, End, SizeLimit, Func, Acc) ->
    fold_binary_impl(Log, Start, End, 0, SizeLimit, Func, Acc).

fold_terms(Log, Start, End, Func, Acc) ->
    fold_terms_impl(Log, Start, End, Func, Acc).

get(#raft_log{name = Name} = Log, Index) ->
    Dir = log_dir(Log),
    case index_below_trim_floor(Dir, Index) of
        true ->
            not_found;
        false ->
            case ets:lookup(Name, Index) of
                [{Index, Entry}] -> {ok, Entry};
                [] -> read_log_disk_record(Log, Index)
            end;
        {error, _Reason} = Error ->
            Error
    end.

term(Log, Index) ->
    case get(Log, Index) of
        {ok, {Term, _Op}} -> {ok, Term};
        not_found -> not_found
    end.

config(Log) ->
    case cached_config(Log) of
        {ok, _Index, _Config} = Cached ->
            Cached;
        none_cached ->
            not_found;
        not_found ->
            case last_index(Log) of
                undefined -> not_found;
                Last ->
                    First =
                        case first_index(Log) of
                            undefined -> 0;
                            Value -> Value
                        end,
                    case config_from_index(Log, Last, First) of
                        {ok, Index, Config} = Found ->
                            cache_latest_config(log_dir(Log), Index, Config),
                            Found;
                        not_found ->
                            Dir = log_dir(Log),
                            cache_latest_config_not_found(Dir, Last),
                            not_found;
                        Other ->
                            Other
                    end
            end
    end.

config_from_index(_Log, Index, First) when Index < First ->
    not_found;
config_from_index(Log, Index, First) ->
    case get(Log, Index) of
        {ok, Entry} ->
            case config_from_entry(Entry) of
                {ok, Config} -> {ok, Index, Config};
                not_found -> config_from_index(Log, Index - 1, First)
            end;
        not_found ->
            config_from_index(Log, Index - 1, First);
        {error, _Reason} = Error ->
            Error
    end.

config_from_entry({_Term, {_Key, {config, Config}}}) ->
    {ok, Config};
config_from_entry({_Term, {_Key, _Label, {config, Config}}}) ->
    {ok, Config};
config_from_entry(_Entry) ->
    not_found.

cached_config(Log) ->
    Dir = log_dir(Log),
    case {last_index(Log), persistent_term:get(latest_config_cache_key(Dir), undefined)} of
        {Last, {Index, Config}} when is_integer(Last), is_integer(Index), Index =< Last ->
            {ok, Index, Config};
        {Last, {not_found, CoveredLast}}
          when is_integer(Last), is_integer(CoveredLast), Last =< CoveredLast ->
            none_cached;
        _Other ->
            not_found
    end.

update_latest_config_from_records(Dir, Records) ->
    {FoundConfig, MaxIndex} =
        lists:foldl(
          fun(Record, {FoundAcc, MaxAcc}) ->
                  RecordMax =
                      case Record of
                          {Index, _Entry} when is_integer(Index), Index > MaxAcc -> Index;
                          _Other -> MaxAcc
                      end,
                  RecordFound =
                      case update_latest_config_from_record(Dir, Record) of
                          updated -> true;
                          not_found -> FoundAcc
                      end,
                  {RecordFound, RecordMax}
          end,
          {false, -1},
          Records),
    case {FoundConfig, MaxIndex} of
        {false, Max} when Max >= 0 -> cache_latest_config_not_found(Dir, Max);
        _Other -> ok
    end.

update_latest_config_from_record(Dir, {Index, Entry}) when is_integer(Index) ->
    case config_from_entry(Entry) of
        {ok, Config} ->
            cache_latest_config(Dir, Index, Config),
            updated;
        not_found ->
            not_found
    end;
update_latest_config_from_record(_Dir, _Record) ->
    not_found.

cache_latest_config(Dir, Index, Config) ->
    CacheKey = latest_config_cache_key(Dir),
    case persistent_term:get(CacheKey, undefined) of
        {ExistingIndex, _ExistingConfig} when is_integer(ExistingIndex), ExistingIndex > Index ->
            ok;
        _Other ->
            persistent_term:put(CacheKey, {Index, Config})
    end.

cache_latest_config_not_found(Dir, Last) when is_integer(Last) ->
    CacheKey = latest_config_cache_key(Dir),
    case persistent_term:get(CacheKey, undefined) of
        {ExistingIndex, _ExistingConfig} when is_integer(ExistingIndex) ->
            ok;
        {not_found, ExistingLast} when is_integer(ExistingLast), ExistingLast >= Last ->
            ok;
        _Other ->
            persistent_term:put(CacheKey, {not_found, Last})
    end.

cache_latest_config_not_found_if_missing(_Dir, undefined) ->
    ok;
cache_latest_config_not_found_if_missing(Dir, Last) when is_integer(Last) ->
    case persistent_term:get(latest_config_cache_key(Dir), undefined) of
        undefined -> cache_latest_config_not_found(Dir, Last);
        _Existing -> ok
    end.

clear_latest_config_cache(Dir) ->
    _ = persistent_term:erase(latest_config_cache_key(Dir)),
    ok.

rebuild_latest_config_cache(Log, Name, Dir) ->
    Previous = persistent_term:get(latest_config_cache_key(Dir), undefined),
    clear_latest_config_cache(Dir),
    case last_index_for_name(Name, Dir) of
        undefined ->
            ok;
        Last ->
            case restore_latest_config_cache(Dir, Previous, Last) of
                restored ->
                    ok;
                not_restored ->
                    First =
                        case first_index_for_name(Name, Dir) of
                            undefined -> 0;
                            FirstValue -> FirstValue
                        end,
                    case config_from_index(Log, Last, First) of
                        {ok, Index, Config} -> cache_latest_config(Dir, Index, Config);
                        _Other -> ok
                    end
            end
    end.

restore_latest_config_cache(Dir, {Index, Config}, Last)
  when is_integer(Index), is_integer(Last), Index =< Last ->
    %% Trim can move the first log index past the original config entry. That
    %% config is still the latest known cluster config; dropping it makes every
    %% append rescan disk until a new config entry is appended.
    persistent_term:put(latest_config_cache_key(Dir), {Index, Config}),
    restored;
restore_latest_config_cache(Dir, {not_found, CoveredLast}, Last)
  when is_integer(CoveredLast), is_integer(Last), Last =< CoveredLast ->
    persistent_term:put(latest_config_cache_key(Dir), {not_found, CoveredLast}),
    restored;
restore_latest_config_cache(_Dir, _Previous, _Last) ->
    not_restored.

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
                                        fold_disk_stream(Dir, Tid, Fun, Acc);
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
        _ = erlang:erase(?FOLD_CONTEXT),
        ets:delete(Tid)
    end.

read_disk(RootDir, Index) when is_integer(Index), Index >= 0 ->
    Dir = fold_disk_segment_dir(RootDir),
    case segment_append_kind(Dir) of
        apply_projection ->
            read_disk_apply_projection_merged(Dir, Index);
        _Other ->
            case index_below_trim_floor(Dir, Index) of
                true -> not_found;
                false -> read_disk_scan(Dir, Index);
                {error, _Reason} = Error -> Error
            end
    end;
read_disk(_RootDir, _Index) ->
    {error, bad_index}.

read_disk_scan(Dir, Index) ->
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
    end.

read_disk_apply_projection_merged(Dir, Index) ->
    case fold_disk(
        Dir,
        fun
            (SeenIndex, {0, {ferricstore_segment_apply_projection_batch, Position, Entries}}, Acc)
              when SeenIndex =:= Index, is_list(Entries) ->
                merge_apply_projection_read_record(Position, Entries, Acc);
            (_SeenIndex, _Entry, Acc) ->
                Acc
        end,
        not_found
    ) of
        {ok, not_found} ->
            not_found;
        {ok, {Position, Entries}} ->
            {ok, {0, {ferricstore_segment_apply_projection_batch, Position, Entries}}};
        {error, _Reason} = Error ->
            Error
    end.

merge_apply_projection_read_record(Position, Entries, not_found) ->
    {Position, normalize_projection_entries(Entries)};
merge_apply_projection_read_record(Position, Entries, {_OldPosition, OldEntries}) ->
    {Position, merge_projection_entries(OldEntries, Entries)}.

fold_disk_stream(Dir, Tid, Fun, Acc) ->
    StartedAt = erlang:monotonic_time(),
    erlang:put(
        ?FOLD_CONTEXT,
        #{callback => Fun, acc => Acc, started_at => StartedAt, disk_records => 0}
    ),
    case load_segments(Dir, Tid) of
        ok ->
            Context = erlang:get(?FOLD_CONTEXT),
            emit_segment_fold(Dir, Context),
            {ok, maps:get(acc, Context)};
        {error, _Reason} = Error ->
            Error
    end.

location_for_index(RootDir, Index) when is_integer(Index), Index >= 0 ->
    Dir = fold_disk_segment_dir(RootDir),
    case index_below_trim_floor(Dir, Index) of
        true ->
            not_found;
        false ->
            case lookup_offset(Dir, Index) of
                {ok, Location} ->
                    {ok, Location};
                not_found ->
                    locate_offset_on_disk(Dir, Index);
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end;
location_for_index(_RootDir, _Index) ->
    {error, bad_index}.

read_disk_at(RootDir, Index, Offset, EncodedSize)
  when is_integer(Index), Index >= 0,
       is_integer(Offset), Offset >= 0,
       is_integer(EncodedSize), EncodedSize >= ?RECORD_HEADER_SIZE ->
    Dir = fold_disk_segment_dir(RootDir),
    case index_below_trim_floor(Dir, Index) of
        true ->
            not_found;
        false ->
            read_disk_at_untrimmed(Dir, Index, Offset, EncodedSize);
        {error, _Reason} = Error ->
            Error
    end;
read_disk_at(_RootDir, _Index, _Offset, _EncodedSize) ->
    {error, bad_location}.

read_disk_at_untrimmed(Dir, Index, Offset, EncodedSize) ->
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
    end.

reset_disk_to_position(RootDir, {raft_log_pos, Index, Term})
        when is_integer(Index), Index >= 0, is_integer(Term), Term >= 0 ->
    Dir = fold_disk_segment_dir(RootDir),
    Record = {Index, {Term, undefined}},
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            case validate_segment_log_dir(Dir) of
                ok ->
                    case check_append_failure_marker(Dir) of
                        ok ->
                            case close_writers_for_dir(Dir) of
                                ok ->
                                    case rewrite_records(Dir, [Record]) of
                                        ok -> set_logical_trim_floor(Dir, Index);
                                        {error, _Reason} = Error -> Error
                                    end;
                                {error, _Reason} = Error -> Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {ensure_segment_log_dir, Reason}}
    end;
reset_disk_to_position(_RootDir, Position) ->
    {error, {bad_reset_position, Position}}.

fold_disk_segment_dir(RootDir) ->
    Path = unicode:characters_to_list(RootDir),
    case filename:basename(Path) of
        "segment_log" -> Path;
        _Other -> filename:join(Path, "segment_log")
    end.

ensure_segment_config(RootDir) ->
    Dir = fold_disk_segment_dir(RootDir),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            case validate_segment_log_dir(Dir) of
                ok ->
                    case load_or_create_segment_config(Dir) of
                        {ok, RecordsPerSegment} ->
                            persistent_term:put(segment_config_cache_key(Dir), RecordsPerSegment),
                            ok;
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {ensure_segment_config_dir, Reason}}
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
    write_projection_batches(RootDir, [{Position, Entries}]).

write_projection_batches(_RootDir, []) ->
    ok;
write_projection_batches(RootDir, Batches) when is_list(Batches) ->
    Dir = fold_disk_segment_dir(RootDir),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            case projection_batch_records(Batches, []) of
                {ok, Records} -> write_projection_batch_records(Dir, Records, nosync);
                {error, _Reason} = Error -> Error
            end;
        {error, Reason} ->
            {error, {ensure_projection_batch_dir, Reason}}
    end.

write_projection_batches_sync(_RootDir, []) ->
    ok;
write_projection_batches_sync(RootDir, Batches) when is_list(Batches) ->
    Dir = fold_disk_segment_dir(RootDir),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            case projection_batch_records(Batches, []) of
                {ok, Records} -> write_projection_batch_records(Dir, Records, sync);
                {error, _Reason} = Error -> Error
            end;
        {error, Reason} ->
            {error, {ensure_projection_batch_dir, Reason}}
    end.

write_projection_batch_records(Dir, Records, Mode) ->
    Normalized = normalize_projection_batch_records(Records),
    case segment_append_kind(Dir) of
        apply_projection ->
            %% Apply-projection is a runtime spill/cache log. Repeated writes for
            %% the same Raft index are append-safe because read_disk/2 merges
            %% duplicate batches in disk order. Keeping this path append-only
            %% avoids multi-second overlap scans during Flow apply.
            write_projection_records_mode(Dir, Normalized, Mode);
        _Other ->
            case projection_records_append_only_fast_path(Dir, Normalized) of
                {ok, true} ->
                    emit_projection_overlap(Dir, length(Normalized), 0, erlang:monotonic_time(), {ok, false}),
                    write_projection_records_mode(Dir, Normalized, Mode);
                {ok, false} ->
                    case projection_records_overlap_disk(Dir, Normalized) of
                        {ok, false} ->
                            write_projection_records_mode(Dir, Normalized, Mode);
                        {ok, true} ->
                            upsert_projection_batch_records(Dir, Normalized);
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end
    end.

write_projection_records_mode(Dir, Records, nosync) ->
    write_records_nosync(Dir, Records);
write_projection_records_mode(Dir, Records, sync) ->
    write_records(Dir, Records).

upsert_projection_batch_records(Dir, NewRecords) ->
    case projection_batch_upsert_plan(Dir, NewRecords) of
        {ok, [], []} ->
            ok;
        {ok, [], AppendRecords} ->
            write_projection_records_mode(Dir, AppendRecords, sync);
        {ok, ReplaceRecords, AppendRecords} ->
            rewrite_projection_upsert_records(Dir, lists:sort(ReplaceRecords ++ AppendRecords));
        {error, _Reason} = Error ->
            Error
    end.

projection_batch_upsert_plan(Dir, NewRecords) ->
    projection_batch_upsert_plan(Dir, NewRecords, [], []).

projection_batch_upsert_plan(_Dir, [], ReplaceAcc, AppendAcc) ->
    {ok, lists:reverse(ReplaceAcc), lists:reverse(AppendAcc)};
projection_batch_upsert_plan(Dir, [{Index, NewEntry} = NewRecord | Rest], ReplaceAcc, AppendAcc) ->
    case read_disk(Dir, Index) of
        {ok, ExistingEntry} ->
            case projection_batch_entry_covers(ExistingEntry, NewEntry) of
                true ->
                    projection_batch_upsert_plan(Dir, Rest, ReplaceAcc, AppendAcc);
                false ->
                    [MergedRecord] =
                        normalize_projection_batch_records([{Index, ExistingEntry}, NewRecord]),
                    projection_batch_upsert_plan(Dir, Rest, [MergedRecord | ReplaceAcc], AppendAcc)
            end;
        not_found ->
            projection_batch_upsert_plan(Dir, Rest, ReplaceAcc, [NewRecord | AppendAcc]);
        {error, _Reason} = Error ->
            Error
    end.

projection_batch_entry_covers(
    {0, {ferricstore_segment_apply_projection_batch, _ExistingPosition, ExistingEntries}},
    {0, {ferricstore_segment_apply_projection_batch, _NewPosition, NewEntries}}
) when is_list(ExistingEntries), is_list(NewEntries) ->
    Existing = projection_entries_to_map(ExistingEntries, #{}),
    lists:all(
        fun
            ({Key, _Value, _ExpireAtMs} = Entry) when is_binary(Key) ->
                maps:get(Key, Existing, undefined) =:= Entry;
            (_Invalid) ->
                true
        end,
        NewEntries
    );
projection_batch_entry_covers(ExistingEntry, NewEntry) ->
    ExistingEntry =:= NewEntry.

projection_records_overlap_disk(_Dir, []) ->
    {ok, false};
projection_records_overlap_disk(Dir, Records) ->
    StartedAt = erlang:monotonic_time(),
    Count = length(Records),
    Result =
        case prepare_projection_overlap_registry(Dir) of
            {ok, Rebuilds} ->
                case projection_records_overlap_registry_lookup(Dir, Records) of
                    {ok, _Overlap} = LookupResult ->
                        emit_projection_overlap(Dir, Count, Rebuilds, StartedAt, LookupResult),
                        LookupResult;
                    {error, _Reason} = Error ->
                        emit_projection_overlap(Dir, Count, Rebuilds, StartedAt, Error),
                        Error
                end;
            {error, _Reason} = Error ->
                emit_projection_overlap(Dir, Count, 0, StartedAt, Error),
                Error
        end,
    Result.

projection_records_append_only_fast_path(_Dir, []) ->
    {ok, true};
projection_records_append_only_fast_path(Dir, [{FirstIndex, _Entry} | _Rest])
  when is_integer(FirstIndex) ->
    case segment_append_kind(Dir) of
        apply_projection ->
            case lookup_offset_dir_last_index(Dir) of
                {ok, LastIndex} when FirstIndex > LastIndex ->
                    {ok, true};
                {ok, _LastIndex} ->
                    {ok, false};
                not_found ->
                    {ok, false};
                {error, _Reason} = Error ->
                    Error
            end;
        _Other ->
            {ok, false}
    end;
projection_records_append_only_fast_path(_Dir, _Records) ->
    {ok, false}.

prepare_projection_overlap_registry(Dir) ->
    case offset_registry_dir_present(Dir) of
        {ok, true} ->
            {ok, 0};
        {ok, false} ->
            case segment_paths(Dir) of
                {ok, []} ->
                    case register_offset_dir_marker(Dir) of
                        ok -> {ok, 0};
                        {error, _Reason} = Error -> Error
                    end;
                {ok, _Paths} ->
                    case register_offset_dir_marker(Dir) of
                        ok -> {ok, 0};
                        {error, _Reason} = Error -> Error
                    end;
                {error, enoent} ->
                    case register_offset_dir_marker(Dir) of
                        ok -> {ok, 0};
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

projection_records_overlap_registry_lookup(Dir, Records) ->
    try
        {ok,
         lists:any(
             fun({Index, _Entry}) ->
                 case lookup_or_locate_offset(Dir, Index) of
                     {ok, _Location} -> true;
                     not_found -> false;
                     {error, _Reason} = Error -> throw(Error)
                 end
             end,
             Records
         )}
    catch
        throw:{error, _Reason} = Error -> Error
    end.

normalize_projection_batch_records(Records) ->
    Map = lists:foldl(fun merge_projection_batch_record/2, #{}, Records),
    [
        {Index, {0, {ferricstore_segment_apply_projection_batch, Position, Entries}}}
     || {Index, {Position, Entries}} <- lists:sort(maps:to_list(Map))
    ].

merge_projection_batch_record(
    {Index, {0, {ferricstore_segment_apply_projection_batch, Position, Entries}}},
    Acc
) ->
    case maps:get(Index, Acc, undefined) of
        undefined ->
            maps:put(Index, {Position, normalize_projection_entries(Entries)}, Acc);
        {_OldPosition, OldEntries} ->
            maps:put(Index, {Position, merge_projection_entries(OldEntries, Entries)}, Acc)
    end.

normalize_projection_entries(Entries) ->
    projection_entries_from_map(projection_entries_to_map(Entries, #{})).

merge_projection_entries(OldEntries, NewEntries) ->
    projection_entries_from_map(
        projection_entries_to_map(NewEntries, projection_entries_to_map(OldEntries, #{}))
    ).

projection_entries_to_map([], Acc) ->
    Acc;
projection_entries_to_map([{Key, _Value, _ExpireAtMs} = Entry | Rest], Acc) when is_binary(Key) ->
    projection_entries_to_map(Rest, maps:put(Key, Entry, Acc));
projection_entries_to_map([_Invalid | Rest], Acc) ->
    projection_entries_to_map(Rest, Acc).

projection_entries_from_map(Map) ->
    [Entry || {_Key, Entry} <- lists:sort(maps:to_list(Map))].

projection_batch_records([], Acc) ->
    {ok, lists:reverse(Acc)};
projection_batch_records([{Position, Entries} | Rest], Acc) when is_list(Entries) ->
    case projection_batch_index(Position) of
        {ok, Index} ->
            Record = {Index, {0, {ferricstore_segment_apply_projection_batch, Position, Entries}}},
            projection_batch_records(Rest, [Record | Acc]);
        {error, _Reason} = Error ->
            Error
    end;
projection_batch_records([_Invalid | _Rest], _Acc) ->
    {error, bad_projection_batch}.

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
                    update_latest_config_from_records(Dir, Records),
                    refresh_memory_stats(Name, Dir),
                    enforce_ets_memory_limit(Name, Dir),
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
                                                    case profile_startup_phase(Dir, preload_segment_config, fun() -> preload_segment_config(Dir) end) of
                                                        ok ->
                                                            case profile_startup_phase(Dir, preload_trim_floor, fun() -> preload_logical_trim_floor(Dir) end) of
                                                                ok ->
                                                                    true = ets:delete_all_objects(Name),
                                                                    _ = ensure_offset_registry(),
                                                                    _ = ensure_memory_registry(),
                                                                    _ = profile_startup_phase(Dir, clear_offset_registry, fun() -> clear_offset_registry_for_dir(Dir) end),
                                                                    clear_latest_config_cache(Dir),
                                                                    case profile_startup_phase(Dir, load_segments, fun() -> load_segments_bounded(Dir, Name) end) of
                                                                        ok ->
                                                                            profile_startup_phase(Dir, refresh_memory_stats, fun() -> refresh_memory_stats(Name, Dir) end),
                                                                            profile_startup_phase(Dir, enforce_ets_memory_limit, fun() -> enforce_ets_memory_limit(Name, Dir) end),
                                                                            {ok, #{dir => Dir}};
                                                                        {error, _Reason} = Error ->
                                                                            true = ets:delete_all_objects(Name),
                                                                            clear_memory_stats(Name),
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
    _ = clear_latest_config_cache(log_dir(Log)),
    _ = clear_logical_trim_floor_cache(log_dir(Log)),
    ok.

close_process_writers(#raft_log{} = Log) ->
    close_writers_for_dir_owner(log_dir(Log), self()).

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
                            ok = set_logical_trim_floor(Dir, Index),
                            clear_latest_config_cache(Dir),
                            set_memory_stats(Name, Dir, 1, record_memory_bytes(Record), Index, Index),
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
    {FirstBefore, _LastBefore} = memory_boundaries(Name, Dir),
    case check_append_failure_marker(Dir) of
        ok ->
            case close_writers_for_dir(Dir) of
                ok ->
                    case truncate_disk_tail_from(Dir, Index) of
                        ok ->
                            delete_from(Name, Index),
                            set_memory_boundaries_and_refresh(Name, Dir, FirstBefore, Index - 1),
                            rebuild_latest_config_cache(Log, Name, Dir),
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

truncate_disk_tail_from(_Dir, Index) when not is_integer(Index); Index < 0 ->
    {error, {bad_truncate_index, Index}};
truncate_disk_tail_from(Dir, Index) ->
    case existing_records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            case lookup_or_locate_offset(Dir, Index) of
                {ok, {Ordinal, Offset, _EncodedSize}} ->
                    case truncate_segment_files_from(Dir, Ordinal, Offset) of
                        ok -> rebuild_offset_registry(Dir);
                        {error, _Reason} = Error -> Error
                    end;
                not_found ->
                    case disk_tail_before_index(Dir, Index, RecordsPerSegment) of
                        true -> ok;
                        false -> {error, {truncate_index_not_found, Index}};
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        not_found ->
            ok;
        {error, _Reason} = Error ->
            Error
    end.

disk_tail_before_index(Dir, Index, RecordsPerSegment) ->
    case lookup_offset_dir_last_index(Dir) of
        {ok, LastIndex} ->
            LastIndex < Index;
        not_found ->
            case segment_paths(Dir) of
                {ok, Paths} ->
                    case scan_segment_paths(Paths, undefined, RecordsPerSegment, undefined, undefined, 0) of
                        {ok, _First, LastIndex, _Count} when is_integer(LastIndex) ->
                            LastIndex < Index;
                        {ok, undefined, undefined, 0} ->
                            true;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, enoent} ->
                    true;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

truncate_segment_files_from(Dir, TargetOrdinal, TargetOffset) ->
    case segment_paths(Dir) of
        {ok, Paths} ->
            truncate_segment_paths_from(Paths, TargetOrdinal, TargetOffset);
        {error, enoent} ->
            ok;
        {error, _Reason} = Error ->
            Error
    end.

truncate_segment_paths_from([], _TargetOrdinal, _TargetOffset) ->
    ok;
truncate_segment_paths_from([{Ordinal, _Path} | Rest], TargetOrdinal, TargetOffset)
  when Ordinal < TargetOrdinal ->
    truncate_segment_paths_from(Rest, TargetOrdinal, TargetOffset);
truncate_segment_paths_from([{TargetOrdinal, Path} | Rest], TargetOrdinal, TargetOffset) ->
    case truncate_segment_path_to(Path, TargetOffset) of
        ok -> truncate_later_segment_paths(Rest);
        {error, _Reason} = Error -> Error
    end;
truncate_segment_paths_from([{Ordinal, Path} | Rest], TargetOrdinal, _TargetOffset)
  when Ordinal > TargetOrdinal ->
    case truncate_segment_path_to(Path, 0) of
        ok -> truncate_later_segment_paths(Rest);
        {error, _Reason} = Error -> Error
    end.

truncate_later_segment_paths([]) ->
    ok;
truncate_later_segment_paths([{_Ordinal, Path} | Rest]) ->
    case truncate_segment_path_to(Path, 0) of
        ok -> truncate_later_segment_paths(Rest);
        {error, _Reason} = Error -> Error
    end.

truncate_segment_path_to(Path, Size) ->
    case close_writer_for_path(Path) of
        ok ->
            case validate_existing_segment_file(Path) of
                ok ->
                    case file:open(Path, [read, write, raw, binary]) of
                        {ok, Fd} ->
                            Result = rollback_append_fd(Path, Fd, Size),
                            CloseResult = file:close(Fd),
                            case {Result, CloseResult} of
                                {ok, ok} -> ok;
                                {{error, Reason}, _} -> {error, {truncate_tail, Reason}};
                                {_, {error, Reason}} -> {error, {truncate_tail, {close, Reason}}}
                            end;
                        {error, Reason} ->
                            {error, {truncate_tail, {open, Reason}}}
                    end;
                {error, Reason} ->
                    {error, {truncate_tail, Reason}}
            end;
        {error, Reason} ->
            {error, {truncate_tail, Reason}}
    end.

trim(#raft_log{name = Name} = Log, Index, State) ->
    %% WARaft advances the in-memory log view only when this returns ok. Keep
    %% that correctness boundary cheap: persist a logical trim floor, drop the
    %% in-memory/offset view below it, and let physical segment cleanup happen
    %% out of the commit path. Recovery honors the floor, so old bytes left on
    %% disk are storage debt, not replayable Raft entries.
    %% The Raft segment is the Bitcask payload location. Before trimming those
    %% records, the storage layer must persist a projection checkpoint so cold
    %% keydir rows can still be rebuilt/read.
    Dir = log_dir(Log),
    {_FirstBefore, LastBefore} = memory_boundaries(Name, Dir),
    case prepare_segment_projection_for_trim(Dir, Index) of
        ok ->
            case check_append_failure_marker(Dir) of
                ok ->
                    case write_logical_trim_floor(Dir, Index) of
                        ok ->
                            delete_before(Name, Index),
                            clear_offset_registry_before(Dir, Index),
                            set_memory_boundaries_and_refresh(Name, Dir, Index, LastBefore),
                            rebuild_latest_config_cache(Log, Name, Dir),
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

memory_status(#raft_log{name = Name} = Log) ->
    Dir = log_dir(Log),
    refresh_memory_stats(Name, Dir),
    memory_status_for(Name, Dir).

fold_impl(_Log, '$end_of_table', _End, _Size, _SizeLimit, _Func, Acc) ->
    {ok, Acc};
fold_impl(_Log, Start, End, _Size, _SizeLimit, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_impl(_Log, _Start, _End, Size, SizeLimit, _Func, Acc) when Size >= SizeLimit ->
    {ok, Acc};
fold_impl(Log, Start, End, Size, SizeLimit, Func, Acc) ->
    case get(Log, Start) of
        {ok, Entry} ->
            EntrySize = erlang:external_size(Entry),
            fold_impl(Log, Start + 1, End, Size + EntrySize, SizeLimit, Func, Func(Start, EntrySize, Entry, Acc));
        not_found ->
            fold_impl(Log, Start + 1, End, Size, SizeLimit, Func, Acc);
        {error, _Reason} = Error ->
            Error
    end.

fold_binary_impl(_Log, '$end_of_table', _End, _Size, _SizeLimit, _Func, Acc) ->
    {ok, Acc};
fold_binary_impl(_Log, Start, End, _Size, _SizeLimit, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_binary_impl(_Log, _Start, _End, Size, SizeLimit, _Func, Acc) when Size >= SizeLimit ->
    {ok, Acc};
fold_binary_impl(Log, Start, End, Size, SizeLimit, Func, Acc) ->
    case get(Log, Start) of
        {ok, Entry} ->
            Binary = term_to_binary(Entry),
            EntrySize = byte_size(Binary),
            fold_binary_impl(Log, Start + 1, End, Size + EntrySize, SizeLimit, Func, Func(Start, Binary, Acc));
        not_found ->
            fold_binary_impl(Log, Start + 1, End, Size, SizeLimit, Func, Acc);
        {error, _Reason} = Error ->
            Error
    end.

fold_terms_impl(_Log, '$end_of_table', _End, _Func, Acc) ->
    {ok, Acc};
fold_terms_impl(_Log, Start, End, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_terms_impl(Log, Start, End, Func, Acc) ->
    case get(Log, Start) of
        {ok, {Term, _Op}} ->
            fold_terms_impl(Log, Start + 1, End, Func, Func(Start, Term, Acc));
        not_found ->
            fold_terms_impl(Log, Start + 1, End, Func, Acc);
        {error, _Reason} = Error ->
            Error
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

clear_offset_registry_before(Dir, Index) ->
    case ensure_offset_registry() of
        ok ->
            clear_offset_registry_before(Dir, Index, ets:first(?OFFSET_REGISTRY));
        {error, _Reason} ->
            ok
    end.

clear_offset_registry_before(_Dir, _Index, '$end_of_table') ->
    ok;
clear_offset_registry_before(Dir, Index, Key) ->
    Next = ets:next(?OFFSET_REGISTRY, Key),
    DirKey = offset_dir_key(Dir),
    case Key of
        {{DirKey, StoredIndex}, _Ordinal, _Offset, _EncodedSize}
          when is_integer(StoredIndex), StoredIndex < Index ->
            true = ets:delete(?OFFSET_REGISTRY, Key);
        _Other ->
            ok
    end,
    clear_offset_registry_before(Dir, Index, Next).

index_below_trim_floor(Dir, Index) when is_integer(Index), Index >= 0 ->
    case logical_trim_floor_result(Dir) of
        {ok, Floor} when Index < Floor -> true;
        {ok, _Floor} -> false;
        {error, _Reason} = Error -> Error
    end;
index_below_trim_floor(_Dir, _Index) ->
    false.

logical_trim_floor_result(Dir) ->
    CacheKey = trim_floor_cache_key(Dir),
    case persistent_term:get(CacheKey, undefined) of
        Floor when is_integer(Floor), Floor >= 0 ->
            {ok, Floor};
        undefined ->
            case read_logical_trim_floor(Dir) of
                {ok, Floor} ->
                    persistent_term:put(CacheKey, Floor),
                    {ok, Floor};
                {error, _Reason} = Error ->
                    Error
            end
    end.

preload_logical_trim_floor(Dir) ->
    case read_logical_trim_floor(Dir) of
        {ok, Floor} ->
            persistent_term:put(trim_floor_cache_key(Dir), Floor),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

read_logical_trim_floor(Dir) ->
    Path = trim_floor_path(Dir),
    case read_segment_metadata_file(Path, trim_floor_file_too_large) of
        {ok, Binary} ->
            decode_trim_floor(Binary);
        {error, enoent} ->
            {ok, 0};
        {error, Reason} ->
            {error, {read_trim_floor, Reason}}
    end.

decode_trim_floor(Binary) ->
    try binary_to_term(Binary, [safe]) of
        #{version := 1, index := Index} when is_integer(Index), Index >= 0 ->
            {ok, Index};
        Other ->
            {error, {bad_trim_floor, Other}}
    catch
        _:Reason ->
            {error, {bad_trim_floor, Reason}}
    end.

write_logical_trim_floor(Dir, Index) when is_integer(Index), Index >= 0 ->
    case logical_trim_floor_result(Dir) of
        {ok, Current} when Index =< Current ->
            ok;
        {ok, _Current} ->
            set_logical_trim_floor(Dir, Index);
        {error, _Reason} = Error ->
            Error
    end;
write_logical_trim_floor(_Dir, Index) ->
    {error, {bad_trim_floor_index, Index}}.

set_logical_trim_floor(Dir, Index) when is_integer(Index), Index >= 0 ->
    Path = trim_floor_path(Dir),
    TmpPath = Path ++ ".tmp." ++ unique_suffix(),
    Metadata = #{version => 1, index => Index},
    case filelib:ensure_dir(Path) of
        ok ->
            case write_file_sync(TmpPath, term_to_binary(Metadata)) of
                ok ->
                    case rename_path(TmpPath, Path) of
                        ok ->
                            case sync_dir(Dir) of
                                ok ->
                                    persistent_term:put(trim_floor_cache_key(Dir), Index),
                                    ok;
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    _ = delete_file_if_exists(TmpPath),
                    Error
            end;
        {error, Reason} ->
            {error, {ensure_trim_floor_dir, Reason}}
    end;
set_logical_trim_floor(_Dir, Index) ->
    {error, {bad_trim_floor_index, Index}}.

clear_logical_trim_floor_cache(Dir) ->
    _ = persistent_term:erase(trim_floor_cache_key(Dir)),
    ok.

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

write_records_nosync(_Dir, []) ->
    ok;
write_records_nosync(Dir, Records) ->
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
                                            write_records_nosync_with_segment_size(Dir, Records, RecordsPerSegment);
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

write_records_nosync_with_segment_size(_Dir, [], _RecordsPerSegment) ->
    ok;
write_records_nosync_with_segment_size(Dir, Records, RecordsPerSegment) ->
    case group_records(Records, RecordsPerSegment) of
        {ok, Groups} -> write_record_group_nosync_list(Dir, Groups, []);
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

write_record_group_nosync_list(Dir, Groups, Rollbacks) ->
    write_record_group_nosync_list(Dir, Groups, Rollbacks, []).

write_record_group_nosync_list(_Dir, [], _Rollbacks, OffsetAcc) ->
    register_offset_entries(lists:append(lists:reverse(OffsetAcc)));
write_record_group_nosync_list(Dir, [{{_Ordinal, Segment}, RecordsRev} | Rest], Rollbacks, OffsetAcc) ->
    case write_record_group_once_nosync(Dir, Segment, RecordsRev) of
        {ok, Rollback, Offsets} ->
            write_record_group_nosync_list(Dir, Rest, [Rollback | Rollbacks], [Offsets | OffsetAcc]);
        {error, Reason} ->
            case rollback_written_groups(Rollbacks) of
                ok -> maybe_poison_after_append_rollback_failure(Dir, Reason);
                {error, RollbackReason} -> poison_segment_log(Dir, {rollback_failed, Reason, RollbackReason})
            end
    end.

write_record_group_once_nosync(Dir, Segment, RecordsRev) ->
    Path = filename:join(Dir, Segment),
    open_record_group_once_file_direct_nosync(Dir, Path, RecordsRev).

write_record_group_once(Dir, Segment, RecordsRev) ->
    Path = filename:join(Dir, Segment),
    open_record_group_once_file_direct(Dir, Path, RecordsRev).

open_record_group_once_file_direct(Dir, Path, RecordsRev) ->
    case acquire_record_group_file_direct(Dir, Path) of
        {ok, Fd, OldSize, NewSegment} ->
            Records = lists:reverse(RecordsRev),
            Writes = [encode_record(Record) || Record <- Records],
            case write_record_group_file_direct(Dir, Path, Fd, OldSize, Writes, NewSegment) of
                ok ->
                    {ok, {Path, OldSize}, offset_entries_for_records(Dir, Path, Records, OldSize, Writes)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _Reason} = Error ->
            Error
    end.

acquire_record_group_file_direct(Dir, Path) ->
    Registry = ensure_writer_registry(),
    maybe_run_writer_registry_hook(after_acquire_ensure, Registry),
    Key = writer_key(Path),
    WriterDir = writer_dir_from_dir(Dir),
    case writer_registry_lookup(Registry, Key) of
        [{Key, WriterDir, file_fd, Fd, Position}] when is_integer(Position), Position >= 0 ->
            {ok, Fd, Position, false};
        [{Key, _Dir, Kind, Handle, _Position}] ->
            case close_writer_entry(Key, Kind, Handle) of
                ok -> open_record_group_file_fd(Registry, Key, WriterDir, Path);
                {error, _Reason} = Error -> Error
            end;
        [{Key, _Dir, Handle, _Position}] ->
            case close_writer_entry(Key, wal_nif, Handle) of
                ok -> open_record_group_file_fd(Registry, Key, WriterDir, Path);
                {error, _Reason} = Error -> Error
            end;
        [] ->
            open_record_group_file_fd(Registry, Key, WriterDir, Path)
    end.

writer_registry_lookup(Registry, Key) ->
    try ets:lookup(Registry, Key) of
        Entries -> Entries
    catch
        error:badarg ->
            RetryRegistry = ensure_writer_registry(),
            try ets:lookup(RetryRegistry, Key) of
                RetryEntries -> RetryEntries
            catch
                error:badarg -> []
            end
    end.

open_record_group_file_fd(Registry, Key, WriterDir, Path) ->
    case close_inactive_writers_for_dir(Registry, WriterDir, Key) of
        ok -> open_record_group_file_fd_after_inactive_close(Registry, Key, WriterDir, Path);
        {error, _Reason} = Error -> Error
    end.

open_record_group_file_fd_after_inactive_close(Registry, Key, WriterDir, Path) ->
    case validate_segment_file_for_append(Path) of
        ok ->
            case file:open(Path, [append, raw, binary]) of
                {ok, Fd} ->
                    case file:position(Fd, eof) of
                        {ok, OldSize} ->
                            case maybe_preallocate_new_segment(Path, OldSize =:= 0) of
                                ok ->
                                    BinaryPath = unicode:characters_to_binary(Path),
                                    case maybe_run_file_open_hook(BinaryPath) of
                                        ok ->
                                            case put_writer_entry(Registry, {Key, WriterDir, file_fd_writing, Fd, OldSize}) of
                                                ok ->
                                                    {ok, Fd, OldSize, OldSize =:= 0};
                                                {error, _Reason} = Error ->
                                                    _ = file:close(Fd),
                                                    Error
                                            end;
                                        {error, _Reason} = Error ->
                                            _ = file:close(Fd),
                                            Error
                                    end;
                                {error, _Reason} = Error ->
                                    _ = file:close(Fd),
                                    Error
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

close_inactive_writers_for_dir(Registry, WriterDir, ActiveKey) ->
    %% The append stream is monotonic. Keeping the active tail segment open is
    %% useful, but retaining every old segment fd makes long runs accumulate
    %% thousands of stale writer entries and descriptors.
    cleanup_writer_entries(writer_registry_entries(Registry), Registry, WriterDir, ActiveKey).

close_writers_for_dir_owner(Dir, Owner) ->
    Registry = ensure_writer_registry(),
    WriterDir = writer_dir_from_dir(Dir),
    close_writer_entries_for_owner(writer_registry_entries(Registry), Registry, WriterDir, Owner).

close_writer_entries_for_owner([], _Registry, _WriterDir, _Owner) ->
    ok;
close_writer_entries_for_owner([Entry | Rest], Registry, WriterDir, Owner) ->
    case writer_close_spec_for_owner(Entry, WriterDir, Owner) of
        {close, Key, Kind, Handle} ->
            case close_writer_entry(Key, Kind, Handle) of
                ok -> close_writer_entries_for_owner(Rest, Registry, WriterDir, Owner);
                {error, _Reason} = Error -> Error
            end;
        skip ->
            close_writer_entries_for_owner(Rest, Registry, WriterDir, Owner)
    end.

writer_close_spec_for_owner({{Owner, _Path} = Key, WriterDir, Kind, Handle, _Position}, WriterDir, Owner) ->
    {close, Key, Kind, Handle};
writer_close_spec_for_owner({{Owner, _Path} = Key, WriterDir, Handle, _Position}, WriterDir, Owner) ->
    {close, Key, wal_nif, Handle};
writer_close_spec_for_owner(_Entry, _WriterDir, _Owner) ->
    skip.

open_record_group_once_file_direct_nosync(Dir, Path, RecordsRev) ->
    case acquire_record_group_file_direct(Dir, Path) of
        {ok, Fd, OldSize, NewSegment} ->
            Records = lists:reverse(RecordsRev),
            Writes = [encode_record(Record) || Record <- Records],
            case write_record_group_file_direct_nosync(Dir, Path, Fd, OldSize, Writes, NewSegment) of
                ok ->
                    {ok, {Path, OldSize}, offset_entries_for_records(Dir, Path, Records, OldSize, Writes)};
                {error, Reason} ->
                    {error, Reason}
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
            case maybe_sync_new_segment_dir(Dir, Path, OldSize, NewSegment) of
                ok ->
                    update_file_fd_writer_position(Dir, Path, Fd, OldSize + Bytes);
                {error, SyncDirReason} ->
                    {error, SyncDirReason}
            end;
        {error, AppendError} ->
            rollback_append_fd_and_close_writer(Path, Fd, OldSize, AppendError)
    end,
    emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment),
    Result.

update_file_fd_writer_position(Dir, Path, Fd, Position) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    WriterDir = writer_dir_from_dir(Dir),
    put_writer_entry(Registry, {Key, WriterDir, file_fd, Fd, Position}).

write_record_group_file_direct_nosync(Dir, Path, Fd, OldSize, Writes, NewSegment) ->
    %% Apply-projection records are rebuilt from the durable WARaft segment log after
    %% crash. Keep their hot append path out of fdatasync; the later segment
    %% projection/snapshot path is the durable cold-query boundary.
    StartedAt = erlang:monotonic_time(),
    Count = length(Writes),
    Bytes = iolist_size(Writes),
    WriteResult =
        case file:write(Fd, Writes) of
            ok -> maybe_run_append_hook(after_write);
            {error, WriteError} -> {error, {write, WriteError}}
        end,

    Result = case WriteResult of
        ok ->
            update_file_fd_writer_position(Dir, Path, Fd, OldSize + Bytes);
        {error, AppendError} ->
            rollback_append_fd_and_close_writer(Path, Fd, OldSize, AppendError)
    end,
    emit_segment_append(Path, Count, Bytes, StartedAt, Result, NewSegment),
    Result.

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
    case close_writer_for_path(Path) of
        ok ->
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
            end;
        {error, Reason} ->
            {error, {rollback_failed, OriginalReason, Reason}}
    end.

rollback_append_fd_and_close_writer(Path, Fd, OldSize, OriginalReason) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    RollbackResult = rollback_append_fd(Path, Fd, OldSize),
    CloseResult = file:close(Fd),
    ok = delete_writer_entry(Registry, Key),
    case {RollbackResult, CloseResult} of
        {ok, ok} -> {error, OriginalReason};
        {{error, RollbackReason}, _} -> {error, {rollback_failed, OriginalReason, RollbackReason}};
        {_, {error, CloseReason}} -> {error, {rollback_failed, OriginalReason, {close, CloseReason}}}
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

rewrite_projection_upsert_records(Dir, Records) ->
    case validate_segment_log_dir(Dir) of
        ok ->
            case recover_rewrite(Dir) of
                ok ->
                    case validate_segment_log_dir(Dir) of
                        ok ->
                            case records_per_segment(Dir) of
                                {ok, RecordsPerSegment} ->
                                    rewrite_projection_upsert_records_atomic(
                                        Dir,
                                        Records,
                                        RecordsPerSegment
                                    );
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

rewrite_projection_upsert_records_atomic(Dir, Records, RecordsPerSegment) ->
    Paths = rewrite_paths(Dir),
    Staging = maps:get(staging, Paths),
    Backup = maps:get(backup, Paths),
    ReplaceMap = maps:from_list([{Index, true} || {Index, _Entry} <- Records]),
    KeepFun = fun(Index) -> not maps:is_key(Index, ReplaceMap) end,
    case prepare_projection_upsert_stage(Staging, Dir, KeepFun, Records, RecordsPerSegment) of
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

prepare_projection_upsert_stage(Staging, SourceDir, KeepFun, Records, RecordsPerSegment) ->
    case remove_tree(Staging) of
        ok ->
            case filelib:ensure_dir(filename:join(Staging, "dummy")) of
                ok ->
                    case write_segment_config(Staging, RecordsPerSegment) of
                        ok ->
                            case write_disk_records_with_segment_size(
                                SourceDir,
                                Staging,
                                KeepFun,
                                RecordsPerSegment
                            ) of
                                ok ->
                                    case write_records_with_segment_size(
                                        Staging,
                                        Records,
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
                        {error, _Reason} = Error -> Error
                    end;
                {error, Reason} ->
                    {error, {ensure_staging_dir, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

write_disk_records_with_segment_size(SourceDir, DestDir, KeepFun, RecordsPerSegment) ->
    case segment_paths(SourceDir) of
        {ok, Paths} ->
            case stream_disk_segment_paths(
                Paths,
                DestDir,
                KeepFun,
                RecordsPerSegment,
                undefined,
                [],
                []
            ) of
                {ok, Ordinal, RecordsRev, Rollbacks} ->
                    case finish_streamed_record_group(DestDir, Ordinal, RecordsRev, Rollbacks) of
                        {ok, _FinalRollbacks} -> ok;
                        {error, _Reason} = Error -> Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, enoent} ->
            ok;
        {error, _Reason} = Error ->
            Error
    end.

stream_disk_segment_paths([], _DestDir, _KeepFun, _RecordsPerSegment, Ordinal, RecordsRev, Rollbacks) ->
    {ok, Ordinal, RecordsRev, Rollbacks};
stream_disk_segment_paths([{SourceOrdinal, Path} | Rest], DestDir, KeepFun, RecordsPerSegment, Ordinal, RecordsRev, Rollbacks) ->
    case stream_disk_segment_path(Path, DestDir, KeepFun, RecordsPerSegment, SourceOrdinal, Ordinal, RecordsRev, Rollbacks) of
        {ok, NextOrdinal, NextRecordsRev, NextRollbacks} ->
            stream_disk_segment_paths(Rest, DestDir, KeepFun, RecordsPerSegment, NextOrdinal, NextRecordsRev, NextRollbacks);
        {error, Reason} = Error ->
            emit_corrupt_segment(Path, Reason),
            Error
    end.

stream_disk_segment_path(Path, DestDir, KeepFun, RecordsPerSegment, SourceOrdinal, Ordinal, RecordsRev, Rollbacks) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    try stream_disk_segment_fd(
                        Fd,
                        Path,
                        DestDir,
                        KeepFun,
                        RecordsPerSegment,
                        SourceOrdinal,
                        0,
                        FileBytes,
                        Ordinal,
                        RecordsRev,
                        Rollbacks
                    ) of
                        Result -> Result
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

stream_disk_segment_fd(Fd, Path, DestDir, KeepFun, RecordsPerSegment, SourceOrdinal, Offset, FileBytes, Ordinal, RecordsRev, Rollbacks) ->
    case file:read(Fd, ?RECORD_HEADER_SIZE) of
        eof ->
            {ok, Ordinal, RecordsRev, Rollbacks};
        {ok, Header} when byte_size(Header) < ?RECORD_HEADER_SIZE ->
            {ok, Ordinal, RecordsRev, Rollbacks};
        {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
            case record_fits_file(Offset, Len, FileBytes) of
                false ->
                    {ok, Ordinal, RecordsRev, Rollbacks};
                true ->
                    case Len > ?MAX_RECORD_BYTES of
                        true ->
                            {error, {record_too_large, Offset, Len}};
                        false ->
                            case file:read(Fd, Len) of
                                {ok, Payload} when byte_size(Payload) =:= Len ->
                                    stream_disk_segment_payload(
                                        Fd,
                                        Path,
                                        DestDir,
                                        KeepFun,
                                        RecordsPerSegment,
                                        SourceOrdinal,
                                        Offset,
                                        FileBytes,
                                        Ordinal,
                                        RecordsRev,
                                        Rollbacks,
                                        Len,
                                        Crc,
                                        Payload
                                    );
                                {ok, Payload} ->
                                    {error, {short_record_read, Offset, Len, byte_size(Payload)}};
                                eof ->
                                    {ok, Ordinal, RecordsRev, Rollbacks};
                                {error, Reason} ->
                                    {error, {read_record_payload, Offset, Reason}}
                            end
                    end
            end;
        {error, Reason} ->
            {error, {read_record_header, Offset, Reason}}
    end.

stream_disk_segment_payload(Fd, Path, DestDir, KeepFun, RecordsPerSegment, SourceOrdinal, Offset, FileBytes, Ordinal, RecordsRev, Rollbacks, Len, Crc, Payload) ->
    case erlang:crc32(Payload) of
        Crc ->
            case decode_segment_record(Path, Payload) of
                {ok, Decoded} ->
                    case Decoded of
                {Index, {_Term, _Op} = Entry} when is_integer(Index), Index >= 0 ->
                    case validate_record_segment_ordinal(Path, Index, SourceOrdinal, RecordsPerSegment) of
                        ok ->
                            case maybe_stream_rewrite_record(
                                DestDir,
                                KeepFun,
                                RecordsPerSegment,
                                {Index, Entry},
                                Ordinal,
                                RecordsRev,
                                Rollbacks
                            ) of
                                {ok, NextOrdinal, NextRecordsRev, NextRollbacks} ->
                                    stream_disk_segment_fd(
                                        Fd,
                                        Path,
                                        DestDir,
                                        KeepFun,
                                        RecordsPerSegment,
                                        SourceOrdinal,
                                        Offset + ?RECORD_HEADER_SIZE + Len,
                                        FileBytes,
                                        NextOrdinal,
                                        NextRecordsRev,
                                        NextRollbacks
                                    );
                                {error, _Reason} = Error ->
                                    Error
                            end;
                        {error, _Reason} = Error ->
                            Error
                    end;
                Other ->
                    {error, {bad_record, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        _Mismatch ->
            {error, {crc_mismatch, Offset}}
    end.

maybe_stream_rewrite_record(DestDir, KeepFun, RecordsPerSegment, {Index, _Entry} = Record, Ordinal, RecordsRev, Rollbacks) ->
    case KeepFun(Index) of
        true -> stream_rewrite_record(DestDir, RecordsPerSegment, Record, Ordinal, RecordsRev, Rollbacks);
        false -> {ok, Ordinal, RecordsRev, Rollbacks}
    end.

stream_rewrite_record(_DestDir, RecordsPerSegment, {Index, _Entry} = Record, undefined, [], Rollbacks) ->
    {ok, segment_ordinal(Index, RecordsPerSegment), [Record], Rollbacks};
stream_rewrite_record(DestDir, RecordsPerSegment, {Index, _Entry} = Record, Ordinal, RecordsRev, Rollbacks) ->
    RecordOrdinal = segment_ordinal(Index, RecordsPerSegment),
    case RecordOrdinal of
        Ordinal when length(RecordsRev) >= ?REWRITE_GROUP_MAX_RECORDS ->
            case finish_streamed_record_group(DestDir, Ordinal, RecordsRev, Rollbacks) of
                {ok, NextRollbacks} ->
                    {ok, RecordOrdinal, [Record], NextRollbacks};
                {error, _Reason} = Error ->
                    Error
            end;
        Ordinal ->
            {ok, Ordinal, [Record | RecordsRev], Rollbacks};
        _OtherOrdinal ->
            case finish_streamed_record_group(DestDir, Ordinal, RecordsRev, Rollbacks) of
                {ok, NextRollbacks} ->
                    {ok, RecordOrdinal, [Record], NextRollbacks};
                {error, _Reason} = Error ->
                    Error
            end
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
                        {ok, LastIndex} ->
                            cache_latest_config_not_found_if_missing(Dir, LastIndex),
                            ok;
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

load_segments_bounded(Dir, Name) ->
    case records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            case segment_paths(Dir) of
                {ok, Paths} ->
                    case segment_append_kind(Dir) of
                        raft_log ->
                            load_raft_segments_bounded(Dir, Name, RecordsPerSegment, Paths);
                        _Other ->
                            load_segments_bounded_full_scan(Dir, Name, RecordsPerSegment, Paths)
                    end;
                {error, enoent} -> ok;
                {error, Reason} = Error ->
                    emit_corrupt_segment(Dir, Reason),
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

load_segments_bounded_full_scan(Dir, Name, RecordsPerSegment, Paths) ->
    StartedAt = erlang:monotonic_time(),
    Result =
        case scan_segment_paths(Paths, undefined, RecordsPerSegment, undefined, undefined, 0) of
            {ok, ScanFirst, ScanLast, _ScanCount} ->
                TailFirst = load_tail_first_index(ScanLast, ets_memory_limits()),
                begin_load_context(Name, Dir, StartedAt, TailFirst, ScanFirst, ScanLast),
                case load_segment_paths(Paths, Name, undefined, RecordsPerSegment) of
                    {ok, LastIndex} ->
                        maybe_cache_latest_config_after_bounded_load(Dir, LastIndex),
                        ok;
                    {error, _Reason} = Error ->
                        Error
                end;
            {error, _Reason} = Error ->
                begin_load_context(Name, Dir, StartedAt, undefined, undefined, undefined),
                Error
        end,
    finish_load_context(Name, Dir, Result).

load_raft_segments_bounded(Dir, Name, RecordsPerSegment, Paths) ->
    StartedAt = erlang:monotonic_time(),
    Limits = ets_memory_limits(),
    TailLimit = maps:get(min_entries, Limits),
    Result =
        case logical_trim_floor_result(Dir) of
            {ok, TrimFloor} ->
                LoadFloor =
                    case TrimFloor > 0 of
                        true -> TrimFloor;
                        false -> undefined
                    end,
                case scan_raft_segment_paths(Paths, undefined, RecordsPerSegment, undefined, 0, TailLimit, {queue:new(), 0}, 0) of
                    {ok, ScanFirst, ScanLast, ScanCount, TailLocations0, ScanPayloadBytes} ->
                        TailFirst0 = load_tail_first_index(ScanLast, Limits),
                        TailFirst = max_defined(TailFirst0, LoadFloor),
                        DiskFirst = max_defined(ScanFirst, LoadFloor),
                        TailLocations = filter_tail_locations(TailLocations0, TailFirst),
                        begin_load_context(Name, Dir, StartedAt, TailFirst, DiskFirst, ScanLast),
                        set_load_context_scan_summary(Name, Dir, ScanCount, DiskFirst, ScanLast, ScanPayloadBytes),
                        case load_raft_tail_locations(Dir, Name, RecordsPerSegment, TailLocations) of
                            ok ->
                                maybe_cache_latest_config_after_bounded_load(Dir, ScanLast),
                                ok;
                            {error, _Reason} = Error ->
                                Error
                        end;
                    {error, _Reason} = Error ->
                        begin_load_context(Name, Dir, StartedAt, undefined, undefined, undefined),
                        Error
                end;
            {error, _Reason} = Error ->
                begin_load_context(Name, Dir, StartedAt, undefined, undefined, undefined),
                Error
        end,
    finish_load_context(Name, Dir, Result).

begin_load_context(Name, Dir, StartedAt, TailFirst, DiskFirst, DiskLast) ->
    erlang:put(
        ?LOAD_CONTEXT,
        #{
            name => Name,
            dir => Dir,
            started_at => StartedAt,
            limits => ets_memory_limits(),
            tail_first_index => TailFirst,
            disk_records => 0,
            disk_records_precounted => false,
            decoded_records => 0,
            scan_payload_bytes => 0,
            ets_entries => 0,
            ets_bytes => 0,
            disk_first => DiskFirst,
            disk_last => DiskLast,
            demoted_records => 0,
            demoted_bytes => 0
        }
    ),
    ok.

set_load_context_scan_summary(Name, Dir, Count, First, Last, PayloadBytes) ->
    case erlang:get(?LOAD_CONTEXT) of
        #{name := Name, dir := Dir} = LoadContext ->
            erlang:put(
                ?LOAD_CONTEXT,
                LoadContext#{
                    disk_records := Count,
                    disk_records_precounted := true,
                    scan_payload_bytes := PayloadBytes,
                    disk_first := First,
                    disk_last := Last
                }
            ),
            ok;
        _Other ->
            ok
    end.

finish_load_context(Name, Dir, Result) ->
    Context = erlang:erase(?LOAD_CONTEXT),
    case {Result, Context} of
        {ok, #{name := Name, dir := Dir} = LoadContext} ->
            EtsEntries = maps:get(ets_entries, LoadContext),
            EtsBytes = maps:get(ets_bytes, LoadContext),
            DiskFirst = maps:get(disk_first, LoadContext),
            DiskLast = maps:get(disk_last, LoadContext),
            ok = set_memory_stats(Name, Dir, EtsEntries, EtsBytes, DiskFirst, DiskLast),
            emit_segment_load(Name, Dir, LoadContext),
            ok;
        _Other ->
            Result
    end.

maybe_validate_load_unique_index(Dir, Index) ->
    case erlang:get(?LOAD_CONTEXT) of
        #{dir := Dir} ->
            case lookup_offset(Dir, Index) of
                {ok, _Location} -> {error, {duplicate_record_index, Index}};
                not_found -> ok;
                {error, _Reason} = Error -> Error
            end;
        _Other ->
            ok
    end.

maybe_track_recovered_record(Dir, Name, {Index, _Entry} = Record) ->
    case erlang:get(?LOAD_CONTEXT) of
        #{name := Name, dir := Dir} = LoadContext ->
            RecordBytes = record_memory_bytes(Record),
            DiskRecords =
                case maps:get(disk_records_precounted, LoadContext, false) of
                    true -> maps:get(disk_records, LoadContext);
                    false -> maps:get(disk_records, LoadContext) + 1
                end,
            UpdatedContext =
                demote_load_context(
                    Name,
                    LoadContext#{
                        disk_records := DiskRecords,
                        decoded_records := maps:get(decoded_records, LoadContext) + 1,
                        ets_entries := maps:get(ets_entries, LoadContext) + 1,
                        ets_bytes := maps:get(ets_bytes, LoadContext) + RecordBytes,
                        disk_first := choose_first(maps:get(disk_first, LoadContext), Index),
                        disk_last := choose_last(maps:get(disk_last, LoadContext), Index)
                    }
                ),
            erlang:put(?LOAD_CONTEXT, UpdatedContext),
            ok;
        _Other ->
            ok
    end.

maybe_track_skipped_record(Dir, Name, Index) ->
    case erlang:get(?LOAD_CONTEXT) of
        #{name := Name, dir := Dir} = LoadContext ->
            DiskRecords =
                case maps:get(disk_records_precounted, LoadContext, false) of
                    true -> maps:get(disk_records, LoadContext);
                    false -> maps:get(disk_records, LoadContext) + 1
                end,
            erlang:put(
                ?LOAD_CONTEXT,
                LoadContext#{
                    disk_records := DiskRecords,
                    disk_first := choose_first(maps:get(disk_first, LoadContext), Index),
                    disk_last := choose_last(maps:get(disk_last, LoadContext), Index)
                }
            ),
            ok;
        _Other ->
            ok
    end.

demote_load_context(Name, #{limits := Limits} = LoadContext) ->
    MinEntries = maps:get(min_entries, Limits),
    MaxEntries = maps:get(max_entries, Limits),
    MaxBytes = maps:get(max_bytes, Limits),
    demote_load_context_loop(Name, LoadContext, MaxEntries, MaxBytes, MinEntries).

demote_load_context_loop(Name, LoadContext, MaxEntries, MaxBytes, MinEntries) ->
    Count = maps:get(ets_entries, LoadContext),
    Bytes = maps:get(ets_bytes, LoadContext),
    case Count > MinEntries andalso over_ets_memory_limit(Count, Bytes, #{max_entries => MaxEntries, max_bytes => MaxBytes}) of
        true ->
            case ets:first(Name) of
                '$end_of_table' ->
                    LoadContext;
                Key ->
                    EntryBytes =
                        case ets:lookup(Name, Key) of
                            [{Key, Entry}] -> erlang:external_size(Entry);
                            [] -> 0
                        end,
                    true = ets:delete(Name, Key),
                    demote_load_context_loop(
                        Name,
                        LoadContext#{
                            ets_entries := max(Count - 1, 0),
                            ets_bytes := max(Bytes - EntryBytes, 0),
                            demoted_records := maps:get(demoted_records, LoadContext) + 1,
                            demoted_bytes := maps:get(demoted_bytes, LoadContext) + EntryBytes
                        },
                        MaxEntries,
                        MaxBytes,
                        MinEntries
                    )
            end;
        false ->
            LoadContext
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
                    case decode_segment_record(Path, Payload) of
                        {ok, Decoded} ->
                            case Decoded of
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
                            end;
                        {error, Reason} ->
                            {error, Reason}
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
                    case record_fits_file(Offset, Len, FileBytes) of
                        false ->
                            not_found;
                        true ->
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
            case decode_segment_record(Path, Payload) of
                {ok, Decoded} ->
                    case Decoded of
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
                    end;
                {error, Reason} ->
                    {error, Reason}
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

scan_segment_paths([], PreviousIndex, _RecordsPerSegment, FirstIndex, _LastIndex, Count) ->
    {ok, FirstIndex, PreviousIndex, Count};
scan_segment_paths([{Ordinal, Path} | Rest], PreviousIndex, RecordsPerSegment, FirstIndex, LastIndex, Count) ->
    case scan_segment(Ordinal, Path, PreviousIndex, RecordsPerSegment, FirstIndex, LastIndex, Count) of
        {ok, NextFirst, NextLast, NextCount} ->
            scan_segment_paths(Rest, NextLast, RecordsPerSegment, NextFirst, NextLast, NextCount);
        {error, _Reason} = Error ->
            Error
    end.

scan_segment(Ordinal, Path, PreviousIndex, RecordsPerSegment, FirstIndex, LastIndex, Count) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    Result =
                        try scan_segment_fd(Fd, Path, PreviousIndex, 0, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, LastIndex, Count) of
                            ScanResult -> ScanResult
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
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

scan_segment_fd(Fd, Path, PreviousIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, LastIndex, Count) ->
    case file:read(Fd, ?RECORD_HEADER_SIZE) of
        eof ->
            {ok, FirstIndex, LastIndex, Count};
        {ok, Header} when byte_size(Header) < ?RECORD_HEADER_SIZE ->
            {ok, FirstIndex, LastIndex, Count};
        {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
            case record_fits_file(Offset, Len, FileBytes) of
                false ->
                    {ok, FirstIndex, LastIndex, Count};
                true ->
                    case Len > ?MAX_RECORD_BYTES of
                        true ->
                            {error, {record_too_large, Offset, Len}};
                        false ->
                            case file:read(Fd, Len) of
                                {ok, Payload} when byte_size(Payload) =:= Len ->
                                    scan_segment_payload(
                                        Fd,
                                        Path,
                                        PreviousIndex,
                                        Offset,
                                        FileBytes,
                                        Ordinal,
                                        RecordsPerSegment,
                                        FirstIndex,
                                        LastIndex,
                                        Count,
                                        Len,
                                        Crc,
                                        Payload
                                    );
                                {ok, Payload} ->
                                    {error, {short_record_read, Offset, Len, byte_size(Payload)}};
                                eof ->
                                    {ok, FirstIndex, LastIndex, Count};
                                {error, Reason} ->
                                    {error, {read_record_payload, Offset, Reason}}
                            end
                    end
            end;
        {error, Reason} ->
            {error, {read_record_header, Offset, Reason}}
    end.

scan_segment_payload(Fd, Path, PreviousIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, _LastIndex, Count, Len, Crc, Payload) ->
    case erlang:crc32(Payload) of
        Crc ->
            case peek_record_index(Payload) of
                {ok, Index} ->
                    case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                        ok ->
                            case recovered_index_allowed(Path, PreviousIndex, Index) of
                                ok ->
                                    scan_segment_fd(
                                        Fd,
                                        Path,
                                        Index,
                                        Offset + ?RECORD_HEADER_SIZE + Len,
                                        FileBytes,
                                        Ordinal,
                                        RecordsPerSegment,
                                        choose_first(FirstIndex, Index),
                                        Index,
                                        Count + 1
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
        _Mismatch ->
            {error, {crc_mismatch, Offset}}
    end.

scan_raft_segment_paths([], PreviousIndex, _RecordsPerSegment, FirstIndex, Count, _TailLimit, TailQueue, ScanPayloadBytes) ->
    {ok, FirstIndex, PreviousIndex, Count, tail_queue_to_list(TailQueue), ScanPayloadBytes};
scan_raft_segment_paths([{Ordinal, Path} | Rest], PreviousIndex, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes) ->
    case scan_raft_segment(Ordinal, Path, PreviousIndex, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes) of
        {ok, NextFirst, NextLast, NextCount, NextTailQueue, NextScanPayloadBytes, ValidBytes, FileBytes} ->
            case maybe_truncate(Path, ValidBytes, FileBytes) of
                ok ->
                    scan_raft_segment_paths(Rest, NextLast, RecordsPerSegment, NextFirst, NextCount, TailLimit, NextTailQueue, NextScanPayloadBytes);
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

scan_raft_segment(Ordinal, Path, PreviousIndex, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    Result =
                        try scan_raft_segment_fd(
                            Fd,
                            Path,
                            PreviousIndex,
                            0,
                            FileBytes,
                            Ordinal,
                            RecordsPerSegment,
                            FirstIndex,
                            Count,
                            TailLimit,
                            TailQueue,
                            ScanPayloadBytes
                        ) of
                            ScanResult -> ScanResult
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
        {error, Reason} ->
            {error, {read_segment_info, Reason}}
    end.

scan_raft_segment_fd(Fd, Path, PreviousIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes) ->
    case file:read(Fd, ?RECORD_HEADER_SIZE) of
        eof ->
            {ok, FirstIndex, PreviousIndex, Count, TailQueue, ScanPayloadBytes, Offset, FileBytes};
        {ok, Header} when byte_size(Header) < ?RECORD_HEADER_SIZE ->
            {ok, FirstIndex, PreviousIndex, Count, TailQueue, ScanPayloadBytes, Offset, FileBytes};
        {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
            case Len > ?MAX_RECORD_BYTES of
                true ->
                    {error, {record_too_large, Offset, Len}};
                false ->
                    case record_fits_file(Offset, Len, FileBytes) of
                        false ->
                            {ok, FirstIndex, PreviousIndex, Count, TailQueue, ScanPayloadBytes, Offset, FileBytes};
                        true ->
                            scan_raft_segment_record(
                                Fd,
                                Path,
                                PreviousIndex,
                                Offset,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                FirstIndex,
                                Count,
                                TailLimit,
                                TailQueue,
                                ScanPayloadBytes,
                                Len,
                                Crc
                            )
                    end
            end;
        {error, Reason} ->
            {error, {read_record_header, Offset, Reason}}
    end.

scan_raft_segment_record(Fd, Path, undefined, Offset, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes, Len, Crc) ->
    case file:read(Fd, Len) of
        {ok, Payload} when byte_size(Payload) =:= Len ->
            case erlang:crc32(Payload) of
                Crc ->
                    case peek_record_index(Payload) of
                        {ok, Index} ->
                            maybe_update_latest_config_from_payload(filename:dirname(Path), Index, Payload),
                            scan_raft_segment_known_index(
                                Fd,
                                Path,
                                undefined,
                                Index,
                                Offset,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                FirstIndex,
                                Count,
                                TailLimit,
                                TailQueue,
                                ScanPayloadBytes + Len,
                                Len
                            );
                        {error, _Reason} = Error ->
                            Error
                    end;
                _Mismatch ->
                    {error, {crc_mismatch, Offset}}
            end;
        {ok, Payload} ->
            {error, {short_record_read, Offset, Len, byte_size(Payload)}};
        eof ->
            {ok, FirstIndex, undefined, Count, TailQueue, ScanPayloadBytes, Offset, FileBytes};
        {error, Reason} ->
            {error, {read_record_payload, Offset, Reason}}
    end;
scan_raft_segment_record(Fd, Path, PreviousIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes, Len, Crc) ->
    case file:read(Fd, Len) of
        {ok, Payload} when byte_size(Payload) =:= Len ->
            case erlang:crc32(Payload) of
                Crc ->
                    case peek_record_index(Payload) of
                        {ok, Index} ->
                            maybe_update_latest_config_from_payload(filename:dirname(Path), Index, Payload),
                            scan_raft_segment_known_index(
                                Fd,
                                Path,
                                PreviousIndex,
                                Index,
                                Offset,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                FirstIndex,
                                Count,
                                TailLimit,
                                TailQueue,
                                ScanPayloadBytes + Len,
                                Len
                            );
                        {error, _Reason} = Error ->
                            Error
                    end;
                _Mismatch ->
                    {error, {crc_mismatch, Offset}}
            end;
        {ok, Payload} ->
            {error, {short_record_read, Offset, Len, byte_size(Payload)}};
        eof ->
            {ok, FirstIndex, PreviousIndex, Count, TailQueue, ScanPayloadBytes, Offset, FileBytes};
        {error, Reason} ->
            {error, {read_record_payload, Offset, Reason}}
    end.

scan_raft_segment_known_index(Fd, Path, PreviousIndex, Index, Offset, FileBytes, Ordinal, RecordsPerSegment, FirstIndex, Count, TailLimit, TailQueue, ScanPayloadBytes, Len) ->
    case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
        ok ->
            case recovered_index_allowed(Path, PreviousIndex, Index) of
                ok ->
                    Location = {Index, Ordinal, Path, Offset, ?RECORD_HEADER_SIZE + Len},
                    scan_raft_segment_fd(
                        Fd,
                        Path,
                        Index,
                        Offset + ?RECORD_HEADER_SIZE + Len,
                        FileBytes,
                        Ordinal,
                        RecordsPerSegment,
                        choose_first(FirstIndex, Index),
                        Count + 1,
                        TailLimit,
                        append_tail_location(TailQueue, TailLimit, Location),
                        ScanPayloadBytes
                    );
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

append_tail_location(TailQueue, TailLimit, _Location) when TailLimit =< 0 ->
    TailQueue;
append_tail_location({Queue0, Count0}, TailLimit, Location) ->
    Queue1 = queue:in(Location, Queue0),
    Count1 = Count0 + 1,
    case Count1 > TailLimit of
        true ->
            {{value, _Dropped}, Queue2} = queue:out(Queue1),
            {Queue2, Count1 - 1};
        false ->
            {Queue1, Count1}
    end.

tail_queue_to_list({Queue, _Count}) ->
    queue:to_list(Queue).

filter_tail_locations(Locations, undefined) ->
    Locations;
filter_tail_locations(Locations, TailFirst) ->
    [Location || {Index, _Ordinal, _Path, _Offset, _EncodedSize} = Location <- Locations, Index >= TailFirst].

load_raft_tail_locations(_Dir, _Name, _RecordsPerSegment, []) ->
    ok;
load_raft_tail_locations(Dir, Name, RecordsPerSegment, [{Index, Ordinal, Path, Offset, EncodedSize} | Rest]) ->
    case maybe_register_record_offset(Dir, Index, Ordinal, Offset, EncodedSize) of
        ok ->
            case read_disk_record_at(Dir, Index, Offset, EncodedSize, RecordsPerSegment) of
                {ok, Entry} ->
                    case insert_recovered_record(Path, Name, {Index, Entry}, Index - 1) of
                        {ok, _LastIndex} ->
                            update_latest_config_from_record(Dir, {Index, Entry}),
                            load_raft_tail_locations(Dir, Name, RecordsPerSegment, Rest);
                        {error, _Reason} = Error ->
                            Error
                    end;
                not_found ->
                    {error, {tail_location_not_found, Index, Offset, EncodedSize}};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

maybe_update_latest_config_from_payload(Dir, Index, Payload) ->
    case config_candidate_payload(Payload) of
        true ->
            try binary_to_term(Payload, [safe]) of
                {Index, {_Term, _Op} = Entry} ->
                    _ = update_latest_config_from_record(Dir, {Index, Entry}),
                    ok;
                _Other ->
                    ok
            catch
                _:_ -> ok
            end;
        false ->
            ok
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
            case record_fits_file(Offset, Len, FileBytes) of
                false ->
                    {ok, PreviousIndex, Offset};
                true ->
                    case Len > ?MAX_RECORD_BYTES of
                        true ->
                            {error, {record_too_large, Offset, Len}};
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
            case peek_record_index(Payload) of
                {ok, Index} ->
                    case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                        ok ->
                            Dir = filename:dirname(Path),
                            case recovered_index_allowed(Path, PreviousIndex, Index) of
                                ok ->
                                    load_segment_valid_payload(
                                        Fd,
                                        Path,
                                        Name,
                                        Index,
                                        Offset,
                                        FileBytes,
                                        Ordinal,
                                        RecordsPerSegment,
                                        Len,
                                        Payload,
                                        Dir
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
        _Mismatch ->
            {error, {crc_mismatch, Offset}}
    end.

load_segment_valid_payload(Fd, Path, Name, Index, Offset, FileBytes, Ordinal, RecordsPerSegment, Len, Payload, Dir) ->
    case should_decode_recovered_payload(Index, Payload) of
        false ->
            ok = maybe_track_skipped_record(Dir, Name, Index),
            load_segment_fd(
                Fd,
                Path,
                Name,
                Index,
                Offset + ?RECORD_HEADER_SIZE + Len,
                FileBytes,
                Ordinal,
                RecordsPerSegment
            );
        true ->
            case maybe_validate_load_unique_index(Dir, Index) of
                ok ->
                    case maybe_register_record_offset(Dir, Index, Ordinal, Offset, ?RECORD_HEADER_SIZE + Len) of
                        ok ->
                            load_segment_decoded_payload(
                                Fd,
                                Path,
                                Name,
                                Index,
                                Offset,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                Len,
                                Payload,
                                Dir
                            );
                        {error, _Reason} = Error ->
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end
    end.

load_segment_decoded_payload(Fd, Path, Name, ParsedIndex, Offset, FileBytes, Ordinal, RecordsPerSegment, Len, Payload, Dir) ->
    case decode_segment_record(Path, Payload) of
        {ok, Decoded} ->
            case Decoded of
        {ParsedIndex, {_Term, _Op} = Entry} ->
            case insert_recovered_record(Path, Name, {ParsedIndex, Entry}, ParsedIndex - 1) of
                {ok, LastIndex} ->
                    update_latest_config_from_record(Dir, {ParsedIndex, Entry}),
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
        {OtherIndex, {_Term, _Op}} when is_integer(OtherIndex), OtherIndex >= 0 ->
            {error, {record_index_mismatch, Offset, ParsedIndex, OtherIndex}};
        Other ->
            {error, {bad_record, Other}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

validate_record_segment_ordinal(Path, Index, ExpectedOrdinal, RecordsPerSegment) ->
    ActualOrdinal = segment_ordinal(Index, RecordsPerSegment),
    case ActualOrdinal of
        ExpectedOrdinal ->
            ok;
        _Other ->
            {error, {segment_ordinal_mismatch, Path, Index, ExpectedOrdinal, ActualOrdinal}}
    end.

insert_recovered_record(Path, Name, {Index, _Entry} = Record, PreviousIndex) ->
    case ets:lookup(Name, Index) of
        [] ->
            case recovered_index_allowed(Path, PreviousIndex, Index) of
                ok ->
                    maybe_store_recovered_record(Path, Name, Record),
                    {ok, Index};
                {error, _Reason} = Error ->
                    Error
            end;
        [_Existing] ->
            case segment_append_kind(Path) of
                apply_projection ->
                    maybe_store_recovered_record(Path, Name, Record),
                    {ok, Index};
                _Other ->
                    {error, {duplicate_record_index, Index}}
            end
    end.

maybe_store_recovered_record(Path, Name, Record) ->
    case maybe_fold_recovered_record(Record) of
        folded ->
            ok;
        not_folded ->
            true = ets:insert(Name, Record),
            maybe_track_recovered_record(filename:dirname(Path), Name, Record)
    end.

maybe_fold_recovered_record({Index, Entry}) ->
    case erlang:get(?FOLD_CONTEXT) of
        #{callback := Fun, acc := Acc, disk_records := DiskRecords} = FoldContext ->
            erlang:put(
                ?FOLD_CONTEXT,
                FoldContext#{acc := Fun(Index, Entry, Acc), disk_records := DiskRecords + 1}
            ),
            folded;
        _Other ->
            not_folded
    end.

recovered_index_allowed(Path, PreviousIndex, Index) ->
    case segment_append_kind(Path) of
        raft_log ->
            recovered_index_contiguous(PreviousIndex, Index);
        apply_projection ->
            ok;
        segment_projection ->
            ok
    end.

recovered_index_contiguous(undefined, _Index) ->
    ok;
recovered_index_contiguous(PreviousIndex, Index) when Index =:= PreviousIndex + 1 ->
    ok;
recovered_index_contiguous(PreviousIndex, Index) when Index =:= PreviousIndex ->
    {error, {duplicate_record_index, Index}};
recovered_index_contiguous(PreviousIndex, Index) ->
    {error, {non_contiguous_record_index, PreviousIndex, Index}}.

load_tail_first_index(undefined, _Limits) ->
    undefined;
load_tail_first_index(LastIndex, Limits) when is_integer(LastIndex), LastIndex >= 0 ->
    MinEntries = maps:get(min_entries, Limits),
    max(0, LastIndex - MinEntries + 1).

maybe_cache_latest_config_after_bounded_load(Dir, LastIndex) ->
    %% Bounded startup skips old command payloads. Config entries are decoded
    %% when seen in the retained tail or in a small stream-start candidate; if
    %% none was seen, cache the covered miss so config/1 does not rescan the
    %% entire old log during every restart.
    case persistent_term:get(latest_config_cache_key(Dir), undefined) of
        undefined -> cache_latest_config_not_found_if_missing(Dir, LastIndex);
        _Existing -> cache_latest_config_not_found_if_missing(Dir, LastIndex)
    end.

should_decode_recovered_payload(Index, Payload) ->
    case erlang:get(?LOAD_CONTEXT) of
        #{tail_first_index := TailFirst} when is_integer(TailFirst), is_integer(Index), Index >= TailFirst ->
            true;
        #{tail_first_index := TailFirst} when is_integer(TailFirst) ->
            config_candidate_payload(Payload);
        _Other ->
            true
    end.

config_candidate_payload(Payload) when byte_size(Payload) =< 4096 ->
    binary:match(Payload, <<"config">>) =/= nomatch;
config_candidate_payload(_Payload) ->
    false.

peek_record_index(<<131, 104, 2, Rest/binary>>) ->
    peek_non_neg_integer(Rest);
peek_record_index(<<131, 105, 0, 0, 0, 2, Rest/binary>>) ->
    peek_non_neg_integer(Rest);
peek_record_index(_Other) ->
    {error, bad_record_envelope}.

peek_non_neg_integer(<<97, Value, _Rest/binary>>) ->
    {ok, Value};
peek_non_neg_integer(<<98, Value:32/signed-big, _Rest/binary>>) when Value >= 0 ->
    {ok, Value};
peek_non_neg_integer(<<110, Size, 0, Digits:Size/binary, _Rest/binary>>) ->
    {ok, little_unsigned(Digits, 0, 0)};
peek_non_neg_integer(<<111, Size:32/unsigned-big, 0, Digits:Size/binary, _Rest/binary>>) ->
    {ok, little_unsigned(Digits, 0, 0)};
peek_non_neg_integer(_Other) ->
    {error, bad_record_index}.

little_unsigned(<<>>, _Shift, Acc) ->
    Acc;
little_unsigned(<<Digit, Rest/binary>>, Shift, Acc) ->
    little_unsigned(Rest, Shift + 8, Acc bor (Digit bsl Shift)).

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

record_fits_file(Offset, Len, FileBytes) ->
    Offset + ?RECORD_HEADER_SIZE + Len =< FileBytes.

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

maybe_register_record_offset(Dir, _Index, _Ordinal, _Offset, _EncodedSize) ->
    case erlang:get(?LOAD_CONTEXT) of
        #{dir := Dir} ->
            ok;
        _Other ->
            register_record_offset(Dir, _Index, _Ordinal, _Offset, _EncodedSize)
    end.

register_offset_entries([]) ->
    ok;
register_offset_entries(Entries) ->
    case ensure_offset_registry() of
        ok ->
            case put_offset_entries(offset_dir_markers(Entries) ++ Entries) of
                ok -> put_offset_dir_last_entries(offset_dir_last_entries(Entries));
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

register_offset_dir_marker(Dir) ->
    case ensure_offset_registry() of
        ok ->
            put_offset_entries(offset_dir_marker(offset_dir_key(Dir)));
        {error, _Reason} = Error ->
            Error
    end.

offset_dir_markers(Entries) ->
    DirKeys =
        lists:foldl(
            fun
                ({{DirKey, _Index}, _Ordinal, _Offset, _EncodedSize}, Acc) ->
                    maps:put(DirKey, true, Acc);
                (_Other, Acc) ->
                    Acc
            end,
            #{},
            Entries
        ),
    [offset_dir_marker(DirKey) || DirKey <- maps:keys(DirKeys)].

offset_dir_marker(DirKey) ->
    {{DirKey, dir_marker}, dir_marker, 0, 0}.

offset_dir_last_entries(Entries) ->
    LastByDir =
        lists:foldl(
            fun
                ({{DirKey, Index}, _Ordinal, _Offset, _EncodedSize}, Acc)
                  when is_integer(Index) ->
                    maps:update_with(
                        DirKey,
                        fun(Existing) -> erlang:max(Existing, Index) end,
                        Index,
                        Acc
                    );
                (_Other, Acc) ->
                    Acc
            end,
            #{},
            Entries
        ),
    [{{DirKey, last_index}, last_index, Index, 0} || {DirKey, Index} <- maps:to_list(LastByDir)].

put_offset_dir_last_entries([]) ->
    ok;
put_offset_dir_last_entries([{{DirKey, last_index}, last_index, Index, 0} | Rest]) ->
    Key = {DirKey, last_index},
    maybe_run_offset_registry_hook(before_last_lookup),
    case lookup_offset_registry(Key) of
        {ok, [{{_DirKey, last_index}, last_index, Existing, 0}]} when is_integer(Existing), Existing >= Index ->
            put_offset_dir_last_entries(Rest);
        {ok, _MissingOrOlder} ->
            case put_offset_entries([{{DirKey, last_index}, last_index, Index, 0}]) of
                ok -> put_offset_dir_last_entries(Rest);
                {error, _Reason} = Error -> Error
            end;
        {error, _RegistryUnavailable} ->
            case put_offset_entries([{{DirKey, last_index}, last_index, Index, 0}]) of
                ok -> put_offset_dir_last_entries(Rest);
                {error, _Reason} = Error -> Error
            end
    end.

offset_registry_dir_present(Dir) ->
    case lookup_offset_registry({offset_dir_key(Dir), dir_marker}) of
        {ok, [{{_DirKey, dir_marker}, dir_marker, 0, 0}]} -> {ok, true};
        {ok, []} -> {ok, false};
        {error, _Reason} = Error -> Error
    end.

lookup_offset(Dir, Index) ->
    case lookup_offset_registry({offset_dir_key(Dir), Index}) of
        {ok, [{{_DirKey, Index}, Ordinal, Offset, EncodedSize}]} ->
            {ok, {Ordinal, Offset, EncodedSize}};
        {ok, []} ->
            not_found;
        {error, _Reason} = Error ->
            Error
    end.

lookup_offset_dir_last_index(Dir) ->
    case lookup_offset_registry({offset_dir_key(Dir), last_index}) of
        {ok, [{{_DirKey, last_index}, last_index, LastIndex, 0}]} when is_integer(LastIndex) ->
            {ok, LastIndex};
        {ok, []} ->
            not_found;
        {error, _Reason} = Error ->
            Error
    end.

lookup_offset_registry(Key) ->
    try ets:lookup(?OFFSET_REGISTRY, Key) of
        Rows -> {ok, Rows}
    catch
        error:badarg ->
            case ensure_offset_registry() of
                ok ->
                    try ets:lookup(?OFFSET_REGISTRY, Key) of
                        Rows -> {ok, Rows}
                    catch
                        error:badarg -> {error, offset_registry_unavailable}
                    end;
                {error, _Reason} = Error ->
                    Error
            end
    end.

lookup_or_locate_offset(Dir, Index) ->
    case lookup_offset(Dir, Index) of
        {ok, _Location} = Ok -> Ok;
        not_found -> locate_offset_on_disk(Dir, Index);
        {error, _Reason} = Error -> Error
    end.

locate_offset_on_disk(Dir, Index) when is_integer(Index), Index >= 0 ->
    case existing_records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            locate_disk_record_offset(Dir, Index, RecordsPerSegment);
        not_found ->
            not_found;
        {error, _Reason} = Error ->
            Error
    end;
locate_offset_on_disk(_Dir, _Index) ->
    {error, bad_index}.

locate_disk_record_offset(Dir, Index, RecordsPerSegment) ->
    Ordinal = segment_ordinal(Index, RecordsPerSegment),
    Path = filename:join(Dir, segment_file_from_ordinal(Ordinal)),
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular, size = FileBytes}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, Fd} ->
                    Result =
                        try locate_disk_record_offset_fd(Fd, Path, Index, 0, FileBytes, Ordinal, RecordsPerSegment) of
                            LocateResult -> LocateResult
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

locate_disk_record_offset_fd(Fd, Path, WantedIndex, Offset, FileBytes, Ordinal, RecordsPerSegment) ->
    locate_disk_record_offset_fd(
        Fd,
        Path,
        WantedIndex,
        Offset,
        FileBytes,
        Ordinal,
        RecordsPerSegment,
        segment_append_kind(Path),
        not_found
    ).

locate_disk_record_offset_fd(
    Fd,
    Path,
    WantedIndex,
    Offset,
    FileBytes,
    Ordinal,
    RecordsPerSegment,
    Kind,
    Latest
) ->
    case file:read(Fd, ?RECORD_HEADER_SIZE) of
        eof ->
            Latest;
        {ok, Header} when byte_size(Header) < ?RECORD_HEADER_SIZE ->
            Latest;
        {ok, <<Len:32/unsigned-big, Crc:32/unsigned-big>>} ->
            case record_fits_file(Offset, Len, FileBytes) of
                false ->
                    Latest;
                true ->
                    case Len > ?MAX_RECORD_BYTES of
                        true ->
                            {error, {record_too_large, Offset, Len}};
                        false ->
                            case file:read(Fd, Len) of
                                {ok, Payload} when byte_size(Payload) =:= Len ->
                                    locate_disk_record_offset_payload(
                                        Fd,
                                        Path,
                                        WantedIndex,
                                        Offset,
                                        FileBytes,
                                        Ordinal,
                                        RecordsPerSegment,
                                        Kind,
                                        Latest,
                                        Len,
                                        Crc,
                                        Payload
                                    );
                                {ok, Payload} ->
                                    {error, {short_record_read, Offset, Len, byte_size(Payload)}};
                                eof ->
                                    Latest;
                                {error, Reason} ->
                                    {error, {read_record_payload, Offset, Reason}}
                            end
                    end
            end;
        {error, Reason} ->
            {error, {read_record_header, Offset, Reason}}
    end.

locate_disk_record_offset_payload(
    Fd,
    Path,
    WantedIndex,
    Offset,
    FileBytes,
    Ordinal,
    RecordsPerSegment,
    Kind,
    Latest,
    Len,
    Crc,
    Payload
) ->
    case erlang:crc32(Payload) of
        Crc ->
            case decode_segment_record(Path, Payload) of
                {ok, Decoded} ->
                    case Decoded of
                {Index, {_Term, _Op}} when is_integer(Index), Index >= 0 ->
                    case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                        ok when Index =:= WantedIndex, Kind =:= apply_projection ->
                            locate_disk_record_offset_fd(
                                Fd,
                                Path,
                                WantedIndex,
                                Offset + ?RECORD_HEADER_SIZE + Len,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                Kind,
                                {ok, {Ordinal, Offset, ?RECORD_HEADER_SIZE + Len}}
                            );
                        ok when Index =:= WantedIndex ->
                            {ok, {Ordinal, Offset, ?RECORD_HEADER_SIZE + Len}};
                        ok when Index < WantedIndex ->
                            locate_disk_record_offset_fd(
                                Fd,
                                Path,
                                WantedIndex,
                                Offset + ?RECORD_HEADER_SIZE + Len,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                Kind,
                                Latest
                            );
                        ok when Kind =:= apply_projection ->
                            locate_disk_record_offset_fd(
                                Fd,
                                Path,
                                WantedIndex,
                                Offset + ?RECORD_HEADER_SIZE + Len,
                                FileBytes,
                                Ordinal,
                                RecordsPerSegment,
                                Kind,
                                Latest
                            );
                        ok ->
                            not_found;
                        {error, _Reason} = Error ->
                            Error
                    end;
                Other ->
                    {error, {bad_record, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        _Mismatch ->
            {error, {crc_mismatch, Offset}}
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
    try ets:match_delete(?OFFSET_REGISTRY, {{DirKey, '_'}, '_', '_', '_'}) of
        true -> ok
    catch
        error:badarg ->
            case ensure_offset_registry() of
                ok ->
                    try ets:match_delete(?OFFSET_REGISTRY, {{DirKey, '_'}, '_', '_', '_'}) of
                        true -> ok
                    catch
                        error:badarg -> ok
                    end;
                {error, _Reason} ->
                    ok
            end
    end.

offset_dir_key(Dir) ->
    unicode:characters_to_binary(cache_dir_key(Dir)).

read_log_disk_record(Log, Index) when is_integer(Index), Index >= 0 ->
    Dir = log_dir(Log),
    case existing_records_per_segment(Dir) of
        {ok, RecordsPerSegment} -> read_disk_record(Dir, Index, RecordsPerSegment);
        not_found -> not_found;
        {error, _Reason} = Error -> Error
    end;
read_log_disk_record(_Log, _Index) ->
    not_found.

memory_boundaries(Name, Dir) ->
    case memory_registry_lookup(Name) of
        {ok, #{first := First, last := Last}} ->
            {First, Last};
        not_found ->
            refresh_memory_stats(Name, Dir),
            case memory_registry_lookup(Name) of
                {ok, #{first := First, last := Last}} -> {First, Last};
                not_found -> {ets_first(Name), ets_last(Name)}
            end
    end.

refresh_memory_stats(Name, Dir) ->
    {Count, Bytes, EtsFirst, EtsLast} = ets_memory_usage(Name),
    {First, Last} =
        case memory_registry_lookup(Name) of
            {ok, #{first := OldFirst, last := OldLast}} ->
                {
                    choose_first(OldFirst, EtsFirst),
                    choose_last(OldLast, EtsLast)
                };
            not_found ->
                {EtsFirst, EtsLast}
        end,
    set_memory_stats(Name, Dir, Count, Bytes, First, Last).

set_memory_boundaries_and_refresh(Name, Dir, First, Last) ->
    {SafeFirst, SafeLast} =
        case {First, Last} of
            {F, L} when is_integer(F), is_integer(L), L >= F -> {F, L};
            _Other -> {undefined, undefined}
        end,
    {Count, Bytes, _EtsFirst, _EtsLast} = ets_memory_usage(Name),
    set_memory_stats(Name, Dir, Count, Bytes, SafeFirst, SafeLast).

set_memory_stats(Name, Dir, Count, Bytes, First, Last) ->
    case ensure_memory_registry() of
        ok ->
            true = ets:insert(?MEMORY_REGISTRY, {Name, offset_dir_key(Dir), Count, Bytes, First, Last}),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

clear_memory_stats(Name) ->
    case ensure_memory_registry() of
        ok ->
            true = ets:delete(?MEMORY_REGISTRY, Name),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

memory_status_for(Name, Dir) ->
    Limits = ets_memory_limits(),
    case memory_registry_lookup(Name) of
        {ok, #{count := Count, bytes := Bytes, first := First, last := Last}} ->
            #{
                ets_entries => Count,
                ets_bytes => Bytes,
                disk_first_index => First,
                disk_last_index => Last,
                max_ets_bytes => maps:get(max_bytes, Limits),
                max_ets_entries => maps:get(max_entries, Limits),
                min_ets_entries => maps:get(min_entries, Limits),
                dir => unicode:characters_to_binary(filename:absname(Dir))
            };
        not_found ->
            #{
                ets_entries => 0,
                ets_bytes => 0,
                disk_first_index => undefined,
                disk_last_index => undefined,
                max_ets_bytes => maps:get(max_bytes, Limits),
                max_ets_entries => maps:get(max_entries, Limits),
                min_ets_entries => maps:get(min_entries, Limits),
                dir => unicode:characters_to_binary(filename:absname(Dir))
            }
    end.

enforce_ets_memory_limit(Name, Dir) ->
    Limits = ets_memory_limits(),
    case memory_registry_lookup(Name) of
        {ok, #{count := Count, bytes := Bytes, first := First, last := Last}} ->
            case over_ets_memory_limit(Count, Bytes, Limits) of
                true ->
                    demote_ets_tail(Name, Dir, Count, Bytes, First, Last, Limits);
                false ->
                    ok
            end;
        not_found ->
            ok
    end.

demote_ets_tail(Name, Dir, Count, Bytes, First, Last, Limits) ->
    MinEntries = maps:get(min_entries, Limits),
    MaxEntries = maps:get(max_entries, Limits),
    MaxBytes = maps:get(max_bytes, Limits),
    {NewCount, NewBytes, Deleted, Freed} =
        demote_ets_tail_loop(Name, Count, Bytes, First, Last, MaxEntries, MaxBytes, MinEntries, 0, 0),
    set_memory_stats(Name, Dir, NewCount, NewBytes, First, Last),
    emit_ets_demote(Name, Dir, Deleted, Freed, NewCount, NewBytes),
    ok.

demote_ets_tail_loop(Name, Count, Bytes, First, Last, MaxEntries, MaxBytes, MinEntries, Deleted, Freed) ->
    case Count > MinEntries andalso over_ets_memory_limit(Count, Bytes, #{max_entries => MaxEntries, max_bytes => MaxBytes}) of
        true ->
            case ets:first(Name) of
                '$end_of_table' ->
                    {Count, Bytes, Deleted, Freed};
                Key ->
                    case ets:lookup(Name, Key) of
                        [{Key, Entry}] ->
                            EntryBytes = erlang:external_size(Entry),
                            true = ets:delete(Name, Key),
                            demote_ets_tail_loop(
                                Name,
                                Count - 1,
                                max(Bytes - EntryBytes, 0),
                                First,
                                Last,
                                MaxEntries,
                                MaxBytes,
                                MinEntries,
                                Deleted + 1,
                                Freed + EntryBytes
                            );
                        [] ->
                            true = ets:delete(Name, Key),
                            demote_ets_tail_loop(Name, Count, Bytes, First, Last, MaxEntries, MaxBytes, MinEntries, Deleted, Freed)
                    end
            end;
        false ->
            {Count, Bytes, Deleted, Freed}
    end.

over_ets_memory_limit(Count, Bytes, #{max_entries := MaxEntries, max_bytes := MaxBytes}) ->
    over_limit(Count, MaxEntries) orelse over_limit(Bytes, MaxBytes).

over_limit(_Value, infinity) ->
    false;
over_limit(Value, Limit) when is_integer(Value), is_integer(Limit) ->
    Value > Limit.

ets_memory_usage(Name) ->
    try ets_memory_usage(Name, ets:first(Name), 0, 0, undefined, undefined) of
        Usage -> Usage
    catch
        error:badarg -> {0, 0, undefined, undefined}
    end.

ets_memory_usage(_Name, '$end_of_table', Count, Bytes, First, Last) ->
    {Count, Bytes, First, Last};
ets_memory_usage(Name, Key, Count, Bytes, First, _Last) ->
    Next = ets:next(Name, Key),
    case ets:lookup(Name, Key) of
        [{Key, Entry}] ->
            ets_memory_usage(
                Name,
                Next,
                Count + 1,
                Bytes + erlang:external_size(Entry),
                choose_first(First, Key),
                Key
            );
        [] ->
            ets_memory_usage(Name, Next, Count, Bytes, First, Key)
    end.

record_memory_bytes({_Index, Entry}) ->
    erlang:external_size(Entry).

ets_first(Name) ->
    try ets:first(Name) of
        '$end_of_table' -> undefined;
        Key -> Key
    catch
        error:badarg -> undefined
    end.

ets_last(Name) ->
    try ets:last(Name) of
        '$end_of_table' -> undefined;
        Key -> Key
    catch
        error:badarg -> undefined
    end.

choose_first(undefined, Value) -> Value;
choose_first(Value, undefined) -> Value;
choose_first(A, B) when A =< B -> A;
choose_first(_A, B) -> B.

choose_last(undefined, Value) -> Value;
choose_last(Value, undefined) -> Value;
choose_last(A, B) when A >= B -> A;
choose_last(_A, B) -> B.

max_defined(undefined, Value) -> Value;
max_defined(Value, undefined) -> Value;
max_defined(A, B) when A >= B -> A;
max_defined(_A, B) -> B.

ets_memory_limits() ->
    MaxEntries = memory_limit_env(waraft_segment_log_max_ets_entries, ?DEFAULT_MAX_ETS_ENTRIES),
    MaxBytes = memory_limit_env(waraft_segment_log_max_ets_bytes, ?DEFAULT_MAX_ETS_BYTES),
    MinRaw = non_neg_int_env_value(waraft_segment_log_min_ets_entries, ?DEFAULT_MIN_ETS_ENTRIES),
    MinEntries =
        case MaxEntries of
            infinity -> MinRaw;
            _ -> min(MinRaw, MaxEntries)
        end,
    #{max_entries => MaxEntries, max_bytes => MaxBytes, min_entries => MinEntries}.

memory_limit_env(Key, Default) ->
    normalize_memory_limit(memory_budget_limit(Key, Default), Default).

non_neg_int_env_value(Key, Default) ->
    case normalize_memory_limit(memory_budget_limit(Key, Default), Default) of
        infinity -> Default;
        Value -> Value
    end.

memory_budget_limit(Key, Default) ->
    try 'Elixir.Ferricstore.MemoryBudget':limit(Key, Default) of
        Value -> Value
    catch
        _:_ ->
            application:get_env(ferricstore, Key, Default)
    end.

normalize_memory_limit(infinity, _Default) -> infinity;
normalize_memory_limit(false, _Default) -> infinity;
normalize_memory_limit(undefined, _Default) -> infinity;
normalize_memory_limit(Value, _Default) when is_integer(Value), Value >= 0 -> Value;
normalize_memory_limit(_Other, Default) -> Default.

memory_registry_lookup(Name) ->
    case ensure_memory_registry() of
        ok ->
            case ets:lookup(?MEMORY_REGISTRY, Name) of
                [{Name, Dir, Count, Bytes, First, Last}] ->
                    {ok, #{dir => Dir, count => Count, bytes => Bytes, first => First, last => Last}};
                [] ->
                    not_found
            end;
        {error, _Reason} ->
            not_found
    end.

ensure_memory_registry() ->
    case ets:info(?MEMORY_REGISTRY) of
        undefined ->
            try
                ets:new(?MEMORY_REGISTRY, [
                    named_table,
                    public,
                    set,
                    {read_concurrency, true},
                    {write_concurrency, true}
                ]),
                ok
            catch
                error:badarg ->
                    case ets:info(?MEMORY_REGISTRY) of
                        undefined -> {error, memory_registry_unavailable};
                        _Info -> ok
                    end
            end;
        _Info ->
            ok
    end.

emit_ets_demote(_Name, _Dir, 0, _Freed, _Count, _Bytes) ->
    ok;
emit_ets_demote(Name, Dir, Deleted, Freed, Count, Bytes) ->
    try telemetry:execute(
        [ferricstore, waraft, segment_log, ets_demote],
        #{count => Deleted, bytes => Freed, ets_entries => Count, ets_bytes => Bytes},
        #{log_name => Name, dir => unicode:characters_to_binary(filename:absname(Dir))}
    ) of
        _ -> ok
    catch
        _:_ -> ok
    end.

emit_segment_load(Name, Dir, LoadContext) ->
    StartedAt = maps:get(started_at, LoadContext),
    Measurements = #{
        duration_ms => erlang:convert_time_unit(erlang:monotonic_time() - StartedAt, native, millisecond),
        disk_records => maps:get(disk_records, LoadContext),
        decoded_records => maps:get(decoded_records, LoadContext),
        scan_payload_bytes => maps:get(scan_payload_bytes, LoadContext, 0),
        ets_entries => maps:get(ets_entries, LoadContext),
        ets_bytes => maps:get(ets_bytes, LoadContext),
        demoted_records => maps:get(demoted_records, LoadContext),
        demoted_bytes => maps:get(demoted_bytes, LoadContext)
    },
    Metadata = #{
        log_name => Name,
        dir => unicode:characters_to_binary(filename:absname(Dir)),
        kind => segment_append_kind(Dir)
    },
    emit_telemetry([ferricstore, waraft, segment_log, load], Measurements, Metadata).

emit_segment_fold(Dir, FoldContext) ->
    StartedAt = maps:get(started_at, FoldContext),
    Measurements = #{
        duration_ms => erlang:convert_time_unit(erlang:monotonic_time() - StartedAt, native, millisecond),
        disk_records => maps:get(disk_records, FoldContext)
    },
    Metadata = #{
        dir => unicode:characters_to_binary(filename:absname(Dir)),
        kind => segment_append_kind(Dir)
    },
    emit_telemetry([ferricstore, waraft, segment_log, fold_disk], Measurements, Metadata).

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

put_offset_entries(Entries) ->
    try ets:insert(?OFFSET_REGISTRY, Entries) of
        true -> ok
    catch
        error:badarg ->
            case ensure_offset_registry() of
                ok ->
                    try ets:insert(?OFFSET_REGISTRY, Entries) of
                        true -> ok
                    catch
                        error:badarg -> {error, offset_registry_unavailable}
                    end;
                {error, _Reason} = Error ->
                    Error
            end
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
    {?MODULE, records_per_segment, cache_dir_key(Dir)}.

trim_floor_path(Dir) ->
    filename:join(Dir, ?TRIM_FLOOR_FILE).

trim_floor_cache_key(Dir) ->
    {?MODULE, trim_floor, cache_dir_key(Dir)}.

latest_config_cache_key(Dir) ->
    {?MODULE, latest_config, cache_dir_key(Dir)}.

cache_dir_key(Dir) ->
    case filename:pathtype(Dir) of
        absolute -> Dir;
        _Relative -> filename:absname(Dir)
    end.

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
    case maybe_run_file_sync_hook(BinaryPath, datasync) of
        ok -> file:datasync(Fd);
        {error, _Reason} = Error -> Error
    end.

close_writers_for_dir(Dir) ->
    Registry = ensure_writer_registry(),
    WriterDir = writer_dir_from_dir(Dir),
    maybe_run_writer_registry_hook(before_tab2list, Registry),
    close_destructive_writer_entries(writer_registry_entries(Registry), Registry, WriterDir).

close_writer_for_path(Path) ->
    Registry = ensure_writer_registry(),
    Key = writer_key(Path),
    case ets:lookup(Registry, Key) of
        [{Key, _Dir, Kind, Handle, _Position}] -> close_writer_entry(Key, Kind, Handle);
        [{Key, _Dir, Handle, _Position}] -> close_writer_entry(Key, wal_nif, Handle);
        [] -> ok
    end.

cleanup_writer_entries([], _Registry, _WriterDir, _ActiveKey) ->
    ok;
cleanup_writer_entries([Entry | Rest], Registry, WriterDir, ActiveKey) ->
    case writer_cleanup_spec(Entry, WriterDir, ActiveKey) of
        {close, Key, Kind, Handle} ->
            case close_writer_entry(Key, Kind, Handle) of
                ok -> cleanup_writer_entries(Rest, Registry, WriterDir, ActiveKey);
                {error, _Reason} = Error -> Error
            end;
        {delete, Key} ->
            ok = delete_writer_entry(Registry, Key),
            cleanup_writer_entries(Rest, Registry, WriterDir, ActiveKey);
        skip ->
            cleanup_writer_entries(Rest, Registry, WriterDir, ActiveKey)
    end.

writer_cleanup_spec({{Owner, _Path} = Key, WriterDir, Kind, Handle, _Position}, WriterDir, ActiveKey)
  when Key =/= ActiveKey ->
    writer_cleanup_spec_for_owner(Owner, Key, Kind, Handle);
writer_cleanup_spec({{Owner, _Path} = Key, WriterDir, Handle, _Position}, WriterDir, ActiveKey)
  when Key =/= ActiveKey ->
    writer_cleanup_spec_for_owner(Owner, Key, wal_nif, Handle);
writer_cleanup_spec(_Entry, _WriterDir, _ActiveKey) ->
    skip.

writer_cleanup_spec_for_owner(Owner, Key, Kind, Handle) when Owner =:= self() ->
    {close, Key, Kind, Handle};
writer_cleanup_spec_for_owner(Owner, Key, _Kind, _Handle) when is_pid(Owner) ->
    case is_process_alive(Owner) of
        false -> {delete, Key};
        true -> skip
    end;
writer_cleanup_spec_for_owner(_Owner, _Key, _Kind, _Handle) ->
    skip.

close_destructive_writer_entries([], _Registry, _WriterDir) ->
    ok;
close_destructive_writer_entries([Entry | Rest], Registry, WriterDir) ->
    case destructive_writer_action(Entry, WriterDir) of
        {close, Key, Kind, Handle} ->
            case close_writer_entry(Key, Kind, Handle) of
                ok -> close_destructive_writer_entries(Rest, Registry, WriterDir);
                {error, _Reason} = Error -> Error
            end;
        {wait_idle_or_owner, Key, Owner} ->
            case wait_for_writer_idle_or_owner_exit(Registry, Key, Owner) of
                ok -> close_destructive_writer_entries(Rest, Registry, WriterDir);
                {error, _Reason} = Error -> Error
            end;
        {wait_owner, Key, Owner} ->
            case wait_for_writer_owner_exit(Registry, Key, Owner) of
                ok -> close_destructive_writer_entries(Rest, Registry, WriterDir);
                {error, _Reason} = Error -> Error
            end;
        {delete, Key} ->
            ok = delete_writer_entry(Registry, Key),
            close_destructive_writer_entries(Rest, Registry, WriterDir);
        skip ->
            close_destructive_writer_entries(Rest, Registry, WriterDir)
    end.

destructive_writer_action({{Owner, _Path} = Key, WriterDir, Kind, Handle, _Position}, WriterDir) ->
    destructive_writer_action_for_owner(Owner, Key, Kind, Handle);
destructive_writer_action({{Owner, _Path} = Key, WriterDir, Handle, _Position}, WriterDir) ->
    destructive_writer_action_for_owner(Owner, Key, wal_nif, Handle);
destructive_writer_action(_Entry, _WriterDir) ->
    skip.

destructive_writer_action_for_owner(Owner, Key, Kind, Handle) when Owner =:= self() ->
    {close, Key, Kind, Handle};
destructive_writer_action_for_owner(Owner, Key, file_fd, _Handle) when is_pid(Owner) ->
    %% file_fd means the append finished and the fd is only cached for reuse.
    %% Raw file descriptors cannot be closed from a different controlling
    %% process, but deleting the registry entry prevents the owner from reusing
    %% a stale fd after the directory rewrite. Active appends are marked
    %% file_fd_writing below and are never invalidated mid-write.
    {delete, Key};
destructive_writer_action_for_owner(Owner, Key, file_fd_writing, _Handle) when is_pid(Owner) ->
    {wait_idle_or_owner, Key, Owner};
destructive_writer_action_for_owner(Owner, Key, _Kind, _Handle) when is_pid(Owner) ->
    case is_process_alive(Owner) of
        true -> {wait_owner, Key, Owner};
        false -> {delete, Key}
    end;
destructive_writer_action_for_owner(_Owner, _Key, _Kind, _Handle) ->
    skip.

wait_for_writer_owner_exit(Registry, Key, Owner) ->
    Deadline = erlang:monotonic_time(millisecond) + file_writer_call_timeout_ms(),
    wait_for_writer_owner_exit(Registry, Key, Owner, Deadline).

wait_for_writer_idle_or_owner_exit(Registry, Key, Owner) ->
    Deadline = erlang:monotonic_time(millisecond) + file_writer_call_timeout_ms(),
    wait_for_writer_idle_or_owner_exit(Registry, Key, Owner, Deadline).

wait_for_writer_idle_or_owner_exit(Registry, Key, Owner, Deadline) ->
    case ets:lookup(Registry, Key) of
        [] ->
            ok;
        [{Key, _Dir, file_fd, _Fd, _Position}] ->
            ok = delete_writer_entry(Registry, Key),
            ok;
        [{Key, _Dir, file_fd_writing, _Fd, _Position}] ->
            wait_for_writer_idle_or_owner_exit_or_retry(Registry, Key, Owner, Deadline);
        [{Key, _Dir, _Kind, _Handle, _Position}] ->
            wait_for_writer_owner_exit(Registry, Key, Owner, Deadline);
        [{Key, _Dir, _Handle, _Position}] ->
            wait_for_writer_owner_exit(Registry, Key, Owner, Deadline)
    end.

wait_for_writer_idle_or_owner_exit_or_retry(Registry, Key, Owner, Deadline) ->
    case is_process_alive(Owner) of
        false ->
            ok = delete_writer_entry(Registry, Key),
            ok;
        true ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    {error, {writer_owner_alive, Key, Owner, file_writer_call_timeout_ms()}};
                false ->
                    timer:sleep(min(10, Deadline - Now)),
                    wait_for_writer_idle_or_owner_exit(Registry, Key, Owner, Deadline)
            end
    end.

wait_for_writer_owner_exit(Registry, Key, Owner, Deadline) ->
    case is_process_alive(Owner) of
        false ->
            ok = delete_writer_entry(Registry, Key),
            ok;
        true ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    {error, {writer_owner_alive, Key, Owner, file_writer_call_timeout_ms()}};
                false ->
                    timer:sleep(min(10, Deadline - Now)),
                    wait_for_writer_owner_exit(Registry, Key, Owner, Deadline)
            end
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
close_writer_entry(Key, file_fd_writing, Fd) ->
    close_writer_entry(Key, file_fd, Fd);
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

put_writer_entry(Registry, Entry) ->
    try ets:insert(Registry, Entry) of
        true -> ok
    catch
        error:badarg ->
            RetryRegistry = ensure_writer_registry(),
            try ets:insert(RetryRegistry, Entry) of
                true -> ok
            catch
                error:badarg -> {error, writer_registry_unavailable}
            end
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
    {self(), cache_dir_key(Path)}.

writer_dir_from_dir(Dir) ->
    cache_dir_key(Dir).

file_writer_call_timeout_ms() ->
    case application:get_env(ferricstore, waraft_segment_log_file_writer_timeout_ms, 30000) of
        Value when is_integer(Value), Value >= 0 -> Value;
        _Other -> 30000
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

maybe_run_file_open_hook(BinaryPath) ->
    case application:get_env(ferricstore, waraft_segment_log_file_open_hook) of
        {ok, {notify, Notify}} ->
            Notify ! {waraft_segment_log_file_open, BinaryPath},
            ok;
        {ok, {fail_once, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_file_open_hook),
            Notify ! {waraft_segment_log_file_open, BinaryPath},
            {error, {file_open_hook, BinaryPath}};
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

maybe_fail_file_sync_count(BinaryPath, Target, Notify, Count) ->
    Notify ! {waraft_segment_log_file_sync, BinaryPath, Count},
    case Count >= Target of
        true ->
            application:unset_env(ferricstore, waraft_segment_log_file_sync_hook),
            {error, {file_sync_hook, BinaryPath, Count}};
        false ->
            application:set_env(
                ferricstore,
                waraft_segment_log_file_sync_hook,
                {fail_on_count, Target, Notify, Count}
            ),
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

maybe_run_offset_registry_hook(Phase) ->
    case application:get_env(ferricstore, waraft_segment_log_offset_registry_hook) of
        {ok, {delete_once, Phase, Notify}} ->
            application:unset_env(ferricstore, waraft_segment_log_offset_registry_hook),
            Notify ! {waraft_segment_log_offset_registry_hook, Phase},
            catch ets:delete(?OFFSET_REGISTRY),
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
        kind => segment_append_kind(Path),
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

emit_projection_overlap(Dir, Count, Rebuilds, StartedAt, Result) ->
    Duration = erlang:monotonic_time() - StartedAt,
    Measurements = #{
        count => Count,
        rebuilds => Rebuilds,
        duration => Duration
    },
    Metadata0 = #{
        path => unicode:characters_to_binary(Dir),
        result => projection_overlap_result(Result)
    },
    Metadata =
        case Result of
            {error, Reason} -> Metadata0#{reason => Reason};
            _ -> Metadata0
        end,
    emit_telemetry([ferricstore, waraft, segment_log, projection_overlap], Measurements, Metadata).

profile_startup_phase(Dir, Phase, Fun) ->
    StartedAt = erlang:monotonic_time(),
    Result = Fun(),
    DurationUs = erlang:convert_time_unit(erlang:monotonic_time() - StartedAt, native, microsecond),
    Metadata0 = #{
        path => unicode:characters_to_binary(Dir),
        phase => Phase,
        kind => segment_append_kind(Dir)
    },
    Metadata =
        case Result of
            {error, Reason} -> Metadata0#{reason => Reason};
            _ -> Metadata0
        end,
    emit_telemetry([ferricstore, waraft, segment_log, startup_phase], #{duration_us => DurationUs}, Metadata),
    Result.

projection_overlap_result({ok, true}) ->
    overlap;
projection_overlap_result({ok, false}) ->
    no_overlap;
projection_overlap_result({error, _Reason}) ->
    error;
projection_overlap_result(_Other) ->
    unknown.

segment_append_kind(Path) ->
    Parts = filename:split(Path),
    case {lists:member("apply_projection_log", Parts), lists:member("segment_projection_log", Parts)} of
        {true, _} -> apply_projection;
        {_, true} -> segment_projection;
        _ -> raft_log
    end.

decode_segment_record(Path, Payload) ->
    try
        Decoded =
            case segment_append_kind(Path) of
                raft_log ->
                    %% Safe decoding still supports VM references used as local
                    %% correlation ids, but rejects new atoms from corrupt WAL.
                    binary_to_term(Payload, [safe]);
                _Projection ->
                    binary_to_term(Payload, [safe])
            end,
        {ok, Decoded}
    catch
        _:Reason ->
            {error, {bad_term, Reason}}
    end.

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
