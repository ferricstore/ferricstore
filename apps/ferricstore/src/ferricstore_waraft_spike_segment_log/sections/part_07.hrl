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
                                        case file:read_link_info(Path, ?STAT_OPTS) of
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
                                    case file:read_link_info(Path, ?STAT_OPTS) of
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
        [] -> missing_offset_index_generation(Dir)
    catch error:badarg -> missing_offset_index_generation(Dir)
    end.

missing_offset_index_generation(Dir) ->
    case temporary_rewrite_dir(Dir) of
        %% Never reuse an old temporary descriptor after its generation is
        %% retired, and never allocate a per-abandoned-directory tombstone.
        true -> make_ref();
        false -> undefined
    end.

index_scanned_offset(Path, Index, Ordinal, Offset, EncodedSize) ->
    case erlang:get(?OFFSET_INDEX_BUILD) of
        {_Dir, _, _, Floor} when Index < Floor -> ok;
        {Dir, _, _, _Floor} ->
            case cached_dirname(Path) of
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
                    with_offset_index_fd(Path, fun(Fd) ->
                        write_offset_index_rows(Fd, RecordsPerSegment, Rows)
                    end);
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
    case file:read_link_info(Path, ?STAT_OPTS) of
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

%% ---------------------------------------------------------------------------
%% Long-lived per-directory segment writer.
%%
%% wa_raft runs every async append in a freshly spawned process. Raw file
%% descriptors belong to the process that opened them, so a writer cache keyed
%% by the caller always missed and each append re-validated and reopened the
%% segment and its .idx sidecar. Appends now run inside one writer process per
%% segment directory, which keeps its descriptors across appends. The caller
%% waits for the result, so durability (datasync before reply) is unchanged.
%% ---------------------------------------------------------------------------

-define(SEGMENT_WRITER_IDLE_MS, 30000).

run_in_segment_writer(Dir, Fun) ->
    run_in_segment_writer(Dir, Fun, 2).

run_in_segment_writer(_Dir, Fun, 0) ->
    Fun();
run_in_segment_writer(Dir, Fun, Attempts) ->
    case segment_writer_pid(Dir) of
        {ok, Pid} ->
            MRef = erlang:monitor(process, Pid),
            Pid ! {segment_writer_run, self(), MRef, Fun},
            await_segment_writer(Dir, Fun, Attempts, Pid, MRef, false);
        {error, _Reason} ->
            Fun()
    end.

await_segment_writer(Dir, Fun, Attempts, Pid, MRef, Started) ->
    receive
        {MRef, started} ->
            await_segment_writer(Dir, Fun, Attempts, Pid, MRef, true);
        {MRef, result, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Pid, Reason} when Started ->
            %% The write may have been partially applied; never replay it.
            {error, {segment_writer_down, Reason}};
        {'DOWN', MRef, process, Pid, _Reason} ->
            %% The writer exited (idle) before taking the request: safe to retry.
            run_in_segment_writer(Dir, Fun, Attempts - 1)
    end.

segment_writer_pid(Dir) ->
    Registry = ensure_writer_registry(),
    Key = {segment_writer_proc, writer_dir_from_dir(Dir)},
    case writer_registry_lookup(Registry, Key) of
        [{Key, Pid}] when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Pid};
                false ->
                    ok = safe_delete_writer_object(Registry, {Key, Pid}),
                    start_segment_writer(Registry, Key)
            end;
        _ ->
            start_segment_writer(Registry, Key)
    end.

start_segment_writer(Registry, Key) ->
    Pid = spawn(fun() -> segment_writer_loop(Registry, Key) end),
    try ets:insert_new(Registry, {Key, Pid}) of
        true ->
            Pid ! segment_writer_registered,
            {ok, Pid};
        false ->
            exit(Pid, kill),
            case writer_registry_lookup(Registry, Key) of
                [{Key, Winner}] when is_pid(Winner) -> {ok, Winner};
                _ -> {error, segment_writer_unavailable}
            end
    catch
        error:badarg ->
            exit(Pid, kill),
            {error, segment_writer_unavailable}
    end.

segment_writer_loop(Registry, Key) ->
    _ = put({?MODULE, segment_writer}, true),
    receive
        segment_writer_registered -> segment_writer_serve(Registry, Key)
    after 5000 ->
        ok
    end.

segment_writer_serve(Registry, Key) ->
    receive
        {segment_writer_run, From, MRef, Fun} ->
            case is_process_alive(From) of
                true ->
                    %% wa_raft cancels an async append by killing its caller;
                    %% the guard makes that kill stop this write, as it did
                    %% when the write ran in the caller itself.
                    Guard = spawn_link(fun() -> segment_writer_guard(From) end),
                    From ! {MRef, started},
                    Result =
                        try Fun() of
                            Value -> Value
                        catch
                            Class:Reason:Stacktrace -> {error, {segment_writer_failed, Class, Reason, Stacktrace}}
                        end,
                    true = unlink(Guard),
                    true = exit(Guard, kill),
                    From ! {MRef, result, Result};
                false ->
                    %% The caller died before this request was taken: no write.
                    ok
            end,
            segment_writer_serve(Registry, Key);
        {segment_writer_sync, From, MRef} ->
            From ! {MRef, synced},
            segment_writer_serve(Registry, Key)
    after ?SEGMENT_WRITER_IDLE_MS ->
        %% Unregister first; a request racing this exit sees DOWN before
        %% "started" and retries against a fresh writer.
        ok = safe_delete_writer_object(Registry, {Key, self()}),
        receive
            {segment_writer_run, _From, _MRef, _Fun} = Late ->
                self() ! Late,
                segment_writer_serve(Registry, Key)
        after 0 ->
            ok
        end
    end.

%% Ties one request's write to its caller, one way: the guard is linked to the
%% writer and only monitors the caller, so a killed caller kills the writer,
%% while a writer that dies for another reason leaves a live caller running
%% (it gets {segment_writer_down, Reason}).
segment_writer_guard(Caller) ->
    MRef = erlang:monitor(process, Caller),
    receive
        {'DOWN', MRef, process, Caller, Reason} ->
            exit({segment_writer_caller_down, Reason})
    end.

%% wa_raft rolls back a cancelled async append (close_process_writers, then
%% truncate) as soon as its caller is dead, which can be before the guard has
%% killed the writer. The write ran in this directory's segment writer, so wait
%% until the writer has died with that caller or has finished the request
%% before the rollback truncates.
await_segment_writer_idle(Dir) ->
    Registry = ensure_writer_registry(),
    Key = {segment_writer_proc, writer_dir_from_dir(Dir)},
    case writer_registry_lookup(Registry, Key) of
        [{Key, Pid}] when is_pid(Pid) ->
            MRef = erlang:monitor(process, Pid),
            Pid ! {segment_writer_sync, self(), MRef},
            Timeout = file_writer_call_timeout_ms(),
            receive
                {MRef, synced} ->
                    erlang:demonitor(MRef, [flush]),
                    ok;
                {'DOWN', MRef, process, Pid, _Reason} ->
                    ok
            after Timeout ->
                erlang:demonitor(MRef, [flush]),
                {error, {segment_writer_busy, Pid, Timeout}}
            end;
        _None ->
            ok
    end.

%% A writer keeps descriptors across appends. A destructive rewrite can only
%% delete another owner's registry entry (raw descriptors cannot be closed from
%% a different process), so the owner closes its stale descriptor before it
%% opens a replacement.
track_segment_fd(Key, Fd) ->
    Identity =
        case file:read_file_info(Fd, ?STAT_OPTS) of
            {ok, Info} -> file_identity(Info);
            {error, _Reason} -> unknown
        end,
    _ = put({?MODULE, segment_fd, Key}, {Fd, Identity}),
    ok.

untrack_segment_fd(Key, Fd) ->
    case get({?MODULE, segment_fd, Key}) of
        {Fd, _Identity} -> _ = erase({?MODULE, segment_fd, Key}), ok;
        _Other -> ok
    end.

close_tracked_segment_fd(Key) ->
    case erase({?MODULE, segment_fd, Key}) of
        {Fd, _Identity} -> _ = file:close(Fd), ok;
        undefined -> ok
    end.

%% A reused descriptor must still be the file its path names; a directory
%% deleted and recreated outside this module would otherwise swallow
%% acknowledged appends into an unlinked inode.
tracked_segment_fd_current(Key, Path, Fd) ->
    case get({?MODULE, segment_fd, Key}) of
        {Fd, Identity} when Identity =/= unknown ->
            case lstat_path(Path) of
                {ok, #file_info{type = regular} = Info} -> file_identity(Info) =:= Identity;
                _Missing -> false
            end;
        {Fd, unknown} ->
            false;
        _NotTracked ->
            true
    end.

%% The .idx sidecar is derived: readers verify every record it points at, so a
%% stale index only costs a fallback scan. Short-lived callers open, write and
%% close it. The segment writer keeps one descriptor with its inode and only
%% checks that the path still names that inode (one lstat) before writing.
with_offset_index_fd(Path, Fun) ->
    case get({?MODULE, segment_writer}) of
        true -> with_cached_offset_index_fd(Path, Fun);
        _ -> with_transient_offset_index_fd(Path, Fun)
    end.

with_transient_offset_index_fd(Path, Fun) ->
    case open_offset_index_for_write(Path) of
        {ok, Fd} ->
            Result = Fun(Fd),
            CloseResult = file:close(Fd),
            case {Result, CloseResult} of
                {ok, ok} -> ok;
                {{error, _Reason} = Error, _} -> Error;
                {ok, {error, Reason}} -> {error, {close_offset_index, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

with_cached_offset_index_fd(Path, Fun) ->
    case cached_offset_index_fd(Path) of
        {ok, Fd} ->
            case Fun(Fd) of
                ok ->
                    ok;
                {error, _Reason} = Error ->
                    ok = drop_cached_offset_index_fd(),
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

cached_offset_index_fd(Path) ->
    case get({?MODULE, offset_index_fd}) of
        {Path, Fd, Identity} ->
            case lstat_path(Path) of
                {ok, #file_info{type = regular} = Info} ->
                    case file_identity(Info) of
                        Identity -> {ok, Fd};
                        _Replaced -> reopen_cached_offset_index_fd(Path)
                    end;
                _Missing ->
                    reopen_cached_offset_index_fd(Path)
            end;
        _NoneOrOtherSegment ->
            reopen_cached_offset_index_fd(Path)
    end.

reopen_cached_offset_index_fd(Path) ->
    ok = drop_cached_offset_index_fd(),
    case open_offset_index_for_write(Path) of
        {ok, Fd} ->
            case file:read_file_info(Fd, ?STAT_OPTS) of
                {ok, Info} ->
                    _ = put({?MODULE, offset_index_fd}, {Path, Fd, file_identity(Info)}),
                    {ok, Fd};
                {error, Reason} ->
                    _ = file:close(Fd),
                    {error, {stat_offset_index, Reason}}
            end;
        {error, _Reason} = Error ->
            Error
    end.

drop_cached_offset_index_fd() ->
    case erase({?MODULE, offset_index_fd}) of
        {_Path, Fd, _Identity} -> _ = file:close(Fd), ok;
        undefined -> ok
    end.

file_identity(#file_info{major_device = Device, inode = Inode}) -> {Device, Inode}.

safe_delete_writer_object(Registry, Object) ->
    try ets:delete_object(Registry, Object) of
        true -> ok
    catch
        error:badarg -> ok
    end.
