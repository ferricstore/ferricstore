%% Included by ferricstore_waraft_spike_segment_log.erl; generated split section 2.

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
