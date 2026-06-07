%% Included by ferricstore_waraft_spike_segment_log.erl; generated split section 4.

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
