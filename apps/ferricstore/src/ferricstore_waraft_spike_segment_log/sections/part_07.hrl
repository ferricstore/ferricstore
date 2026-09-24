%% Rebuildable, fixed-width disk index for older WAL offsets. The bounded ETS
%% registry remains the hot path. Index records are not authoritative: missing
%% or malformed slots fall back to the CRC-verified segment scan.

offset_index_path(Dir, Ordinal) ->
    filename:join(Dir, integer_to_list(Ordinal) ++ ".idx").

report_offset_index_failure({error, Reason}, Entries) ->
    lists:foreach(
        fun(DirKey) ->
            Untrusted = {?MODULE, offset_index_untrusted, DirKey},
            case persistent_term:get(Untrusted, false) of
                true -> ok;
                false -> persistent_term:put(Untrusted, true)
            end
        end,
        lists:usort([DirKey || {{DirKey, _Index}, _Ordinal, _Offset, _Size} <- Entries])
    ),
    Key = {?MODULE, offset_index_failure},
    case erlang:get(Key) of
        true -> ok;
        _First ->
            erlang:put(Key, true),
            logger:warning("WARaft derived offset index unavailable; using verified disk fallback: ~p", [Reason]),
            emit_telemetry([ferricstore, waraft, segment_log, offset_index_failure],
                           #{count => 1}, #{reason => Reason}),
            ok
    end.

trust_offset_index_for_dir(Dir) ->
    persistent_term:erase({?MODULE, offset_index_untrusted, offset_dir_key(Dir)}),
    ok.

offset_index_record(Index, Offset, EncodedSize)
  when is_integer(Index), Index >= 0, Index < 18446744073709551616,
       is_integer(Offset), Offset >= 0, Offset < 18446744073709551616,
       is_integer(EncodedSize), EncodedSize >= ?RECORD_HEADER_SIZE,
       EncodedSize < 4294967296 ->
    Body = <<?OFFSET_INDEX_MAGIC:32/unsigned-big, Index:64/unsigned-big,
             Offset:64/unsigned-big, EncodedSize:32/unsigned-big>>,
    <<Body/binary, (erlang:crc32(Body)):32/unsigned-big>>;
offset_index_record(_Index, _Offset, _EncodedSize) ->
    {error, invalid_offset_index_record}.

write_offset_index_entries([]) ->
    ok;
write_offset_index_entries(Entries) ->
    case {erlang:get(?FOLD_CONTEXT), erlang:get(?OFFSET_INDEX_BUILD)} of
        {#{callback := _}, _} ->
            %% A read-only fold already visits validated WAL frames. Startup and
            %% rewrites build the sidecar; repeated reads must not rewrite it.
            ok;
        {_, {Dir, Buffered, Count, Floor}} ->
            Next = lists:reverse(Entries, Buffered),
            NextCount = Count + length(Entries),
            case NextCount >= 1024 of
                true ->
                    erlang:put(?OFFSET_INDEX_BUILD, {Dir, [], 0, Floor}),
                    write_offset_index_entries_now(lists:reverse(Next));
                false ->
                    erlang:put(?OFFSET_INDEX_BUILD, {Dir, Next, NextCount, Floor}),
                    ok
            end;
        _NoBuild ->
            write_offset_index_entries_now(Entries)
    end.

start_offset_index_build(Dir) ->
    start_offset_index_build(Dir, 0).

start_offset_index_build(Dir, Floor) ->
    erlang:put(?OFFSET_INDEX_BUILD, {Dir, [], 0, Floor}),
    ok.

finish_offset_index_build() ->
    case erlang:erase(?OFFSET_INDEX_BUILD) of
        {_Dir, [], 0, _Floor} -> ok;
        {_Dir, Buffered, _Count, _Floor} -> write_offset_index_entries_now(lists:reverse(Buffered));
        _Missing -> ok
    end.

discard_offset_index_build() ->
    _ = erlang:erase(?OFFSET_INDEX_BUILD),
    ok.

reset_offset_indexes_for_dir(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            Result = lists:foldl(
                fun(File, ok) ->
                        case filename:extension(File) of
                            ".idx" ->
                                case parse_segment_ordinal(filename:basename(File, ".idx")) of
                                    {ok, _Ordinal} ->
                                        Path = filename:join(Dir, File),
                                        case file:read_link_info(Path) of
                                            {ok, #file_info{type = regular}} -> file:delete(Path);
                                            {ok, #file_info{type = Type}} -> {error, {unsafe_offset_index_path, Type}};
                                            {error, _Reason} = Error -> Error
                                        end;
                                    _UnknownFile -> ok
                                end;
                            _Other -> ok
                        end;
                   (_File, {error, _Reason} = Error) -> Error
                end,
                ok, Files
            ),
            case Result of
                ok ->
                    erlang:erase({?MODULE, offset_index_pruned_before, Dir}),
                    update_offset_index_generation(Dir);
                {error, _Reason} = Error -> Error
            end;
        {error, enoent} -> update_offset_index_generation(Dir);
        {error, _Reason} = Error -> Error
    end.

maybe_prune_offset_indexes_before(Dir, Index) ->
    case existing_records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            BeforeOrdinal = Index div RecordsPerSegment,
            Marker = {?MODULE, offset_index_pruned_before, Dir},
            case erlang:get(Marker) of
                N when is_integer(N), N >= BeforeOrdinal -> ok;
                _ ->
                    case prune_offset_index_files(Dir, BeforeOrdinal) of
                        ok -> erlang:put(Marker, BeforeOrdinal), ok;
                        {error, _Reason} -> ok
                    end
            end;
        _Missing -> ok
    end.

prune_offset_index_files(Dir, BeforeOrdinal) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foldl(
                fun(File, ok) ->
                    case filename:extension(File) of
                        ".idx" ->
                            case parse_segment_ordinal(filename:basename(File, ".idx")) of
                                {ok, Ordinal} when Ordinal < BeforeOrdinal ->
                                    Path = filename:join(Dir, File),
                                    case file:read_link_info(Path) of
                                        {ok, #file_info{type = regular}} -> file:delete(Path);
                                        _Unsafe -> {error, unsafe_offset_index_path}
                                    end;
                                _Keep -> ok
                            end;
                        _Other -> ok
                    end;
                   (_File, {error, _Reason} = Error) -> Error
                end,
                ok, Files
            );
        {error, _Reason} = Error -> Error
    end.

ensure_offset_index_generations() ->
    case ets:info(?OFFSET_INDEX_GENERATIONS) of
        undefined ->
            try ets:new(?OFFSET_INDEX_GENERATIONS, [named_table, public, set,
                                                    {read_concurrency, true}]), ok
            catch error:badarg -> ok end;
        _ -> ok
    end.

update_offset_index_generation(Dir) ->
    ensure_offset_index_generations(),
    try ets:insert(?OFFSET_INDEX_GENERATIONS, {Dir, make_ref()}) of
        true -> ok
    catch error:badarg -> {error, offset_index_generation_unavailable}
    end.

offset_index_generation(Path) ->
    Dir = filename:dirname(Path),
    try ets:lookup(?OFFSET_INDEX_GENERATIONS, Dir) of
        [{Dir, Generation}] -> Generation;
        [] -> undefined
    catch error:badarg -> undefined
    end.

index_scanned_offset(Path, Index, Ordinal, Offset, EncodedSize) ->
    case erlang:get(?OFFSET_INDEX_BUILD) of
        {_Dir, _, _, Floor} when Index < Floor -> ok;
        {Dir, _, _, _Floor} ->
            case filename:dirname(Path) of
                Dir ->
                    write_offset_index_entries([
                        {{offset_dir_key(Dir), Index}, Ordinal, Offset, EncodedSize}
                    ]);
                _OtherDir -> {error, offset_index_scan_dir_mismatch}
            end;
        _NoBuild -> ok
    end.

write_offset_index_entries_now(Entries) ->
    Groups = lists:foldl(
        fun({{DirKey, Index}, Ordinal, Offset, EncodedSize}, Acc) ->
            Key = {DirKey, Ordinal},
            maps:update_with(Key, fun(Rows) -> [{Index, Offset, EncodedSize} | Rows] end,
                             [{Index, Offset, EncodedSize}], Acc)
        end,
        #{}, Entries
    ),
    maps:fold(
        fun({DirKey, Ordinal}, Rows, ok) ->
                write_offset_index_group(binary_to_list(DirKey), Ordinal, lists:reverse(Rows));
           (_Key, _Rows, {error, _Reason} = Error) ->
                Error
        end,
        ok, Groups
    ).

write_offset_index_group(Dir, Ordinal, Rows) ->
    case existing_records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            case lists:all(fun({Index, _, _}) ->
                               segment_ordinal(Index, RecordsPerSegment) =:= Ordinal
                           end, Rows) of
                true ->
                    Path = offset_index_path(Dir, Ordinal),
                    case open_offset_index_for_write(Path) of
                        {ok, Fd} ->
                            Result = write_offset_index_rows(Fd, RecordsPerSegment, Rows),
                            CloseResult = file:close(Fd),
                            case {Result, CloseResult} of
                                {ok, ok} -> ok;
                                {{error, _Reason} = Error, _} -> Error;
                                {ok, {error, Reason}} -> {error, {close_offset_index, Reason}}
                            end;
                        {error, _Reason} = Error -> Error
                    end;
                false ->
                    {error, {offset_index_wrong_segment, Ordinal}}
            end;
        Other ->
            {error, {offset_index_segment_config, Other}}
    end.

write_offset_index_rows(Fd, RecordsPerSegment, Rows) ->
    write_offset_index_rows(Fd, RecordsPerSegment, Rows, undefined, 0, []).

write_offset_index_rows(Fd, _RecordsPerSegment, [], Start, _Next, Buffer) ->
    flush_offset_index_rows(Fd, Start, Buffer);
write_offset_index_rows(Fd, RecordsPerSegment, [{Index, Offset, Size} | Rest], Start, Next, Buffer) ->
    case offset_index_record(Index, Offset, Size) of
        Record when is_binary(Record) ->
            Slot = (Index rem RecordsPerSegment) * ?OFFSET_INDEX_RECORD_SIZE,
            case {Start, Slot =:= Next} of
                {undefined, _} ->
                    write_offset_index_rows(Fd, RecordsPerSegment, Rest, Slot, Slot + ?OFFSET_INDEX_RECORD_SIZE, [Record]);
                {_Previous, true} ->
                    write_offset_index_rows(Fd, RecordsPerSegment, Rest, Start, Next + ?OFFSET_INDEX_RECORD_SIZE, [Record | Buffer]);
                {_Previous, false} ->
                    case flush_offset_index_rows(Fd, Start, Buffer) of
                        ok -> write_offset_index_rows(Fd, RecordsPerSegment, Rest, Slot, Slot + ?OFFSET_INDEX_RECORD_SIZE, [Record]);
                        {error, _Reason} = Error -> Error
                    end
            end;
        {error, _Reason} = Error -> Error
    end.

flush_offset_index_rows(_Fd, undefined, []) -> ok;
flush_offset_index_rows(Fd, Start, Buffer) ->
    file:pwrite(Fd, Start, lists:reverse(Buffer)).

open_offset_index_for_write(Path) ->
    case file:read_link_info(Path) of
        {ok, #file_info{type = regular}} ->
            open_verified_segment_file(Path, [read, write, raw, binary]);
        {ok, #file_info{type = Type}} ->
            {error, {unsafe_offset_index_path, Type}};
        {error, enoent} ->
            case file:open(Path, [write, raw, binary, exclusive]) of
                {ok, Fd} ->
                    case validate_open_segment_file(Path, Fd) of
                        ok -> {ok, Fd};
                        {error, _Reason} = Error ->
                            file:close(Fd),
                            Error
                    end;
                {error, eexist} ->
                    open_verified_segment_file(Path, [read, write, raw, binary]);
                {error, _Reason} = Error -> Error
            end;
        {error, _Reason} = Error -> Error
    end.

lookup_offset_index(Dir, Index) when is_integer(Index), Index >= 0 ->
    case persistent_term:get({?MODULE, offset_index_untrusted, offset_dir_key(Dir)}, false) of
        true -> not_found;
        false -> lookup_trusted_offset_index(Dir, Index)
    end;
lookup_offset_index(_Dir, _Index) ->
    not_found.

lookup_trusted_offset_index(Dir, Index) ->
    case existing_records_per_segment(Dir) of
        {ok, RecordsPerSegment} ->
            Ordinal = segment_ordinal(Index, RecordsPerSegment),
            Path = offset_index_path(Dir, Ordinal),
            case cached_offset_index_reader(Path) of
                {ok, Fd} ->
                    Slot = (Index rem RecordsPerSegment) * ?OFFSET_INDEX_RECORD_SIZE,
                    Read = file:pread(Fd, Slot, ?OFFSET_INDEX_RECORD_SIZE),
                    case decode_offset_index_record(Index, Ordinal, Read) of
                        {ok, {Ordinal, Offset, Size}} = Location ->
                            case offset_index_frame_matches(Dir, Index, Ordinal, Offset, Size) of
                                true -> Location;
                                false -> not_found
                            end;
                        not_found -> not_found
                    end;
                {error, enoent} -> not_found;
                {error, _Reason} -> not_found
            end;
        _Missing -> not_found
    end.

cached_offset_index_reader(Path) ->
    Generation = offset_index_generation(Path),
    Readers = case erlang:get(?OFFSET_INDEX_READERS) of
        Cached when is_list(Cached) -> Cached;
        _None -> []
    end,
    case lists:keytake(Path, 1, Readers) of
        {value, {Path, Fd, Generation}, Others} ->
            erlang:put(?OFFSET_INDEX_READERS, [{Path, Fd, Generation} | Others]),
            {ok, Fd};
        {value, {Path, Fd, _StaleGeneration}, Others} ->
            _ = file:close(Fd),
            erlang:put(?OFFSET_INDEX_READERS, Others),
            open_cached_offset_index_reader(Path, Others, Generation);
        false ->
            open_cached_offset_index_reader(Path, Readers, Generation)
    end.

open_cached_offset_index_reader(Path, Readers, Generation) ->
    case open_verified_segment_file(Path, [read, raw, binary]) of
        {ok, Fd} ->
            Kept = case length(Readers) >= 16 of
                true ->
                    {_OldPath, OldFd, _OldGeneration} = lists:last(Readers),
                    _ = file:close(OldFd),
                    lists:droplast(Readers);
                false -> Readers
            end,
            erlang:put(?OFFSET_INDEX_READERS, [{Path, Fd, Generation} | Kept]),
            {ok, Fd};
        {error, _Reason} = Error -> Error
    end.

decode_offset_index_record(Index, Ordinal,
                           {ok, <<?OFFSET_INDEX_MAGIC:32/unsigned-big,
                                  Index:64/unsigned-big, Offset:64/unsigned-big,
                                  Size:32/unsigned-big, Crc:32/unsigned-big>>})
  when Size >= ?RECORD_HEADER_SIZE ->
    Body = <<?OFFSET_INDEX_MAGIC:32/unsigned-big, Index:64/unsigned-big,
             Offset:64/unsigned-big, Size:32/unsigned-big>>,
    case erlang:crc32(Body) of
        Crc -> {ok, {Ordinal, Offset, Size}};
        _Mismatch -> not_found
    end;
decode_offset_index_record(_Index, _Ordinal, _Invalid) ->
    not_found.

offset_index_frame_matches(Dir, Index, Ordinal, Offset, Size) ->
    Segment = filename:join(Dir, segment_file_from_ordinal(Ordinal)),
    case cached_offset_index_reader(Segment) of
        {ok, Fd} ->
            case file:pread(Fd, Offset, min(Size, 40)) of
                {ok, <<Len:32/unsigned-big, _Crc:32/unsigned-big, PayloadPrefix/binary>>}
                  when Len + ?RECORD_HEADER_SIZE =:= Size ->
                    peek_record_index(PayloadPrefix) =:= {ok, Index};
                _MissingOrDifferent -> false
            end;
        {error, _Reason} -> false
    end.
