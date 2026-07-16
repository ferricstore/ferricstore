%% Included by ferricstore_waraft_spike_segment_log.erl; generated split section 3.

write_append_failure_marker(Dir, Reason) ->
    MarkerPath = append_failure_marker_path(Dir),
    TmpPath = MarkerPath ++ ".tmp." ++ unique_suffix(),
    Marker = #{
        version => 1,
        reason => Reason
    },
    case filelib:ensure_dir(MarkerPath) of
        ok ->
            case write_file_sync(TmpPath, encode_external_term(Marker)) of
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

compact_apply_projection_records(Dir, TrimIndex, Records) ->
    case validate_apply_projection_compaction_records(Records, TrimIndex) of
        ok ->
            case validate_segment_log_dir(Dir) of
                ok ->
                    case recover_rewrite(Dir) of
                        ok ->
                            case close_writers_for_dir(Dir) of
                                ok ->
                                    case records_per_segment(Dir) of
                                        {ok, RecordsPerSegment} ->
                                            KeepFun = fun(Index) -> Index >= TrimIndex end,
                                            rewrite_projection_filtered_records_atomic(
                                                Dir,
                                                KeepFun,
                                                Records,
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
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

validate_apply_projection_compaction_records([], _TrimIndex) ->
    ok;
validate_apply_projection_compaction_records(
    [{Index, {0, {ferricstore_segment_apply_projection_batch, _Position, Entries}}} | Rest],
    TrimIndex
) when is_integer(Index), Index > 0, Index < TrimIndex, is_list(Entries) ->
    validate_apply_projection_compaction_records(Rest, TrimIndex);
validate_apply_projection_compaction_records([Record | _Rest], TrimIndex) ->
    {error, {bad_apply_projection_compaction_record, TrimIndex, Record}}.

rewrite_projection_filtered_records_atomic(Dir, KeepFun, Records, RecordsPerSegment) ->
    Paths = rewrite_paths(Dir),
    Staging = maps:get(staging, Paths),
    Backup = maps:get(backup, Paths),
    case prepare_projection_upsert_stage(
        Staging,
        Dir,
        KeepFun,
        Records,
        RecordsPerSegment
    ) of
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
            case open_verified_segment_file(Path, [read, raw, binary]) of
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
            case decode_external_term_exact(Binary) of
                {ok, #{version := 1, dir := Dir, staging := Staging, backup := Backup} = Marker}
                  when is_list(Staging), is_list(Backup) ->
                    case validate_rewrite_marker_paths(Dir, Staging, Backup) of
                        ok -> rollback_rewrite(Dir, Marker);
                        {error, _Reason} = Error -> Error
                    end;
                {ok, Other} ->
                    {error, {bad_rewrite_marker, Other}};
                {error, Reason} ->
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
    case write_file_sync(TmpPath, encode_external_term(Marker)) of
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
            case open_verified_segment_file(Path, [read, raw, binary]) of
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
