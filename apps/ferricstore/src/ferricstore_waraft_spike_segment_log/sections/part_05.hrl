%% Included by ferricstore_waraft_spike_segment_log.erl; generated split section 5.

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
