%% Included by ferricstore_waraft_spike_segment_log.erl; generated split section 1.

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
                    append_memory_stats(Name, Dir, Records),
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
