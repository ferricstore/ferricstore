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

open_disk_reader(RootDir, Index, ExpectedOrdinal)
  when is_integer(Index), Index >= 0,
       is_integer(ExpectedOrdinal), ExpectedOrdinal >= 0 ->
    Dir = fold_disk_segment_dir(RootDir),
    case index_below_trim_floor(Dir, Index) of
        true ->
            not_found;
        false ->
            open_disk_reader_untrimmed(Dir, Index, ExpectedOrdinal);
        {error, _Reason} = Error ->
            Error
    end;
open_disk_reader(_RootDir, _Index, _ExpectedOrdinal) ->
    {error, bad_disk_reader_location}.

open_disk_reader_untrimmed(Dir, Index, ExpectedOrdinal) ->
    case validate_segment_log_dir(Dir) of
        ok ->
            case recover_rewrite(Dir) of
                ok ->
                    case validate_segment_log_dir(Dir) of
                        ok ->
                            case existing_records_per_segment(Dir) of
                                {ok, RecordsPerSegment} ->
                                    open_disk_reader_file(
                                        Dir,
                                        Index,
                                        ExpectedOrdinal,
                                        RecordsPerSegment
                                    );
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

open_disk_reader_file(Dir, Index, ExpectedOrdinal, RecordsPerSegment) ->
    Ordinal = segment_ordinal(Index, RecordsPerSegment),
    case Ordinal =:= ExpectedOrdinal of
        false ->
            {error, {segment_ordinal_mismatch, Ordinal, ExpectedOrdinal}};
        true ->
            Path = filename:join(Dir, segment_file_from_ordinal(Ordinal)),
            case file:read_link_info(Path) of
                {ok, #file_info{type = regular, size = FileBytes}} ->
                    case open_verified_segment_file(Path, [read, raw, binary]) of
                        {ok, Fd} ->
                            {ok, {ferricstore_segment_disk_reader_v1, Fd, Path, FileBytes,
                                  Ordinal, RecordsPerSegment, Dir}};
                        {error, Reason} ->
                            {error, {open_segment, Reason}}
                    end;
                {ok, #file_info{type = Type}} ->
                    {error, {unsafe_segment_path, Path, Type}};
                {error, enoent} ->
                    not_found;
                {error, Reason} ->
                    {error, {read_segment_info, Reason}}
            end
    end.

read_disk_reader(
  {ferricstore_segment_disk_reader_v1, Fd, Path, FileBytes, Ordinal,
   RecordsPerSegment, Dir},
  Index,
  Offset,
  EncodedSize
 )
  when is_integer(Index), Index >= 0,
       is_integer(Offset), Offset >= 0,
       is_integer(EncodedSize), EncodedSize >= ?RECORD_HEADER_SIZE ->
    case index_below_trim_floor(Dir, Index) of
        true ->
            not_found;
        false ->
            case segment_ordinal(Index, RecordsPerSegment) of
                Ordinal ->
                    Result = read_disk_record_at_fd(
                        Fd,
                        Path,
                        Index,
                        Offset,
                        EncodedSize,
                        FileBytes,
                        Ordinal,
                        RecordsPerSegment
                    ),
                    case Result of
                        {error, Reason} ->
                            emit_corrupt_segment(Path, Reason),
                            Result;
                        _Other ->
                            Result
                    end;
                ActualOrdinal ->
                    {error, {segment_ordinal_mismatch, ActualOrdinal, Ordinal}}
            end;
        {error, _Reason} = Error ->
            Error
    end;
read_disk_reader(_Reader, _Index, _Offset, _EncodedSize) ->
    {error, bad_disk_reader_location}.

read_disk_reader_many(
  {ferricstore_segment_disk_reader_v1, Fd, Path, FileBytes, Ordinal,
   RecordsPerSegment, Dir},
  Requests
 )
  when is_list(Requests) ->
    case logical_trim_floor_result(Dir) of
        {ok, Floor} ->
            case validate_disk_reader_batch(
                Requests,
                Floor,
                FileBytes,
                Ordinal,
                RecordsPerSegment,
                0,
                0,
                [],
                []
            ) of
                {ok, _Locations, Validated} ->
                    {SpanLocations, Spans} =
                        coalesce_adjacent_disk_reader_requests(Validated),
                    case file:pread(Fd, SpanLocations) of
                        {ok, SpanFrames} when length(SpanFrames) =:= length(Spans) ->
                            case decode_disk_reader_spans(
                                SpanFrames,
                                Spans,
                                Path,
                                Ordinal,
                                RecordsPerSegment,
                                []
                            ) of
                                {ok, _Entries} = Ok ->
                                    Ok;
                                {error, Reason} = Error ->
                                    emit_corrupt_segment(Path, Reason),
                                    Error
                            end;
                        {ok, SpanFrames} ->
                            {error, {batch_read_count_mismatch, length(Spans), length(SpanFrames)}};
                        {error, Reason} ->
                            {error, {batch_pread, Reason}}
                    end;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end;
read_disk_reader_many(_Reader, _Requests) ->
    {error, bad_disk_reader_batch}.

validate_disk_reader_batch([], _Floor, _FileBytes, _Ordinal, _RecordsPerSegment,
                           _Count, _TotalBytes, Locations, Validated) ->
    {ok, lists:reverse(Locations), lists:reverse(Validated)};
validate_disk_reader_batch(_Requests, _Floor, _FileBytes, _Ordinal, _RecordsPerSegment,
                           Count, _TotalBytes, _Locations, _Validated)
  when Count >= ?MAX_DISK_READER_BATCH_RECORDS ->
    {error, disk_reader_batch_record_limit_exceeded};
validate_disk_reader_batch(
  [{Index, Offset, EncodedSize} = Request | Rest],
  Floor,
  FileBytes,
  Ordinal,
  RecordsPerSegment,
  Count,
  TotalBytes,
  Locations,
  Validated
 )
  when is_integer(Index), Index >= 0,
       is_integer(Offset), Offset >= 0,
       is_integer(EncodedSize), EncodedSize >= ?RECORD_HEADER_SIZE,
       EncodedSize =< ?MAX_RECORD_BYTES + ?RECORD_HEADER_SIZE ->
    NextBytes = TotalBytes + EncodedSize,
    case {
        Index < Floor,
        segment_ordinal(Index, RecordsPerSegment) =:= Ordinal,
        Offset + EncodedSize =< FileBytes,
        NextBytes =< ?MAX_DISK_READER_BATCH_BYTES
    } of
        {true, _, _, _} ->
            {error, {record_below_trim_floor, Index, Floor}};
        {false, false, _, _} ->
            {error, {segment_ordinal_mismatch, segment_ordinal(Index, RecordsPerSegment), Ordinal}};
        {false, true, false, _} ->
            {error, {record_outside_segment, Index, Offset, EncodedSize, FileBytes}};
        {false, true, true, false} ->
            {error, disk_reader_batch_byte_limit_exceeded};
        {false, true, true, true} ->
            validate_disk_reader_batch(
                Rest,
                Floor,
                FileBytes,
                Ordinal,
                RecordsPerSegment,
                Count + 1,
                NextBytes,
                [{Offset, EncodedSize} | Locations],
                [Request | Validated]
            )
    end;
validate_disk_reader_batch(_Requests, _Floor, _FileBytes, _Ordinal, _RecordsPerSegment,
                           _Count, _TotalBytes, _Locations, _Validated) ->
    {error, bad_disk_reader_batch_request}.

coalesce_adjacent_disk_reader_requests(Requests) ->
    Positioned = position_disk_reader_requests(Requests, 0, []),
    Sorted = lists:sort(fun disk_reader_request_before/2, Positioned),
    Spans = coalesce_sorted_disk_reader_requests(Sorted, []),
    {[{Offset, EndOffset - Offset} || {Offset, EndOffset, _Items} <- Spans], Spans}.

position_disk_reader_requests([], _Position, Positioned) ->
    Positioned;
position_disk_reader_requests(
  [{Index, Offset, EncodedSize} | Requests],
  Position,
  Positioned
 ) ->
    position_disk_reader_requests(
        Requests,
        Position + 1,
        [{Position, Index, Offset, EncodedSize} | Positioned]
    ).

disk_reader_request_before(
  {LeftPosition, _LeftIndex, LeftOffset, _LeftSize},
  {RightPosition, _RightIndex, RightOffset, _RightSize}
 ) ->
    case LeftOffset =:= RightOffset of
        true -> LeftPosition < RightPosition;
        false -> LeftOffset < RightOffset
    end.

coalesce_sorted_disk_reader_requests([], Spans) ->
    lists:reverse([
        {Offset, EndOffset, lists:reverse(Items)}
     || {Offset, EndOffset, Items} <- Spans
    ]);
coalesce_sorted_disk_reader_requests(
  [{_Position, _Index, Offset, EncodedSize} = Request | Requests],
  [{SpanOffset, Offset, Items} | Spans]
 ) ->
    coalesce_sorted_disk_reader_requests(
        Requests,
        [{SpanOffset, Offset + EncodedSize, [Request | Items]} | Spans]
    );
coalesce_sorted_disk_reader_requests(
  [{_Position, _Index, Offset, EncodedSize} = Request | Requests],
  Spans
 ) ->
    coalesce_sorted_disk_reader_requests(
        Requests,
        [{Offset, Offset + EncodedSize, [Request]} | Spans]
    ).

decode_disk_reader_spans([], [], _Path, _Ordinal, _RecordsPerSegment, Entries) ->
    {ok, [Entry || {_Position, Entry} <- lists:keysort(1, Entries)]};
decode_disk_reader_spans(
  [SpanFrame | SpanFrames],
  [{SpanOffset, SpanEndOffset, Requests} | Spans],
  Path,
  Ordinal,
  RecordsPerSegment,
  Entries
 ) ->
    SpanSize = SpanEndOffset - SpanOffset,
    case SpanFrame of
        Frame when is_binary(Frame), byte_size(Frame) =:= SpanSize ->
            case decode_disk_reader_span(
                Requests,
                Frame,
                SpanOffset,
                Path,
                Ordinal,
                RecordsPerSegment,
                Entries
            ) of
                {ok, NextEntries} ->
                    decode_disk_reader_spans(
                        SpanFrames,
                        Spans,
                        Path,
                        Ordinal,
                        RecordsPerSegment,
                        NextEntries
                    );
                {error, _Reason} = Error ->
                    Error
            end;
        Frame when is_binary(Frame) ->
            {error, {short_batch_span_read, SpanOffset, SpanSize, byte_size(Frame)}};
        _Other ->
            {error, {record_not_found, SpanOffset}}
    end;
decode_disk_reader_spans(_Frames, _Spans, _Path, _Ordinal, _RecordsPerSegment, _Entries) ->
    {error, batch_read_count_mismatch}.

decode_disk_reader_span([], _Frame, _SpanOffset, _Path, _Ordinal,
                        _RecordsPerSegment, Entries) ->
    {ok, Entries};
decode_disk_reader_span(
  [{Position, Index, Offset, EncodedSize} | Requests],
  SpanFrame,
  SpanOffset,
  Path,
  Ordinal,
  RecordsPerSegment,
  Entries
 ) ->
    RelativeOffset = Offset - SpanOffset,
    Frame = binary:part(SpanFrame, RelativeOffset, EncodedSize),
    case decode_disk_reader_frame(
        Frame,
        Path,
        Index,
        Offset,
        EncodedSize,
        Ordinal,
        RecordsPerSegment
    ) of
        {ok, Entry} ->
            decode_disk_reader_span(
                Requests,
                SpanFrame,
                SpanOffset,
                Path,
                Ordinal,
                RecordsPerSegment,
                [{Position, Entry} | Entries]
            );
        {error, _Reason} = Error ->
            Error
    end.

decode_disk_reader_frame(
  Frame,
  Path,
  WantedIndex,
  Offset,
  EncodedSize,
  Ordinal,
  RecordsPerSegment
 )
  when is_binary(Frame) ->
    case Frame of
        <<Len:32/unsigned-big, Crc:32/unsigned-big, Payload/binary>> ->
            ExpectedSize = ?RECORD_HEADER_SIZE + Len,
            case {
                byte_size(Frame) =:= EncodedSize,
                Len =< ?MAX_RECORD_BYTES,
                ExpectedSize =:= EncodedSize,
                byte_size(Payload) =:= Len,
                erlang:crc32(Payload) =:= Crc
            } of
                {false, _, _, _, _} ->
                    {error, {short_record_read, Offset, EncodedSize, byte_size(Frame)}};
                {true, false, _, _, _} ->
                    {error, {record_too_large, Offset, Len}};
                {true, true, false, _, _} ->
                    {error, {record_size_mismatch, Offset, EncodedSize, ExpectedSize}};
                {true, true, true, false, _} ->
                    {error, {short_record_read, Offset, Len, byte_size(Payload)}};
                {true, true, true, true, false} ->
                    {error, {crc_mismatch, Offset}};
                {true, true, true, true, true} ->
                    decode_disk_reader_frame_payload(
                        Path,
                        Payload,
                        WantedIndex,
                        Offset,
                        Ordinal,
                        RecordsPerSegment
                    )
            end;
        _Other ->
            {error, {short_record_header, Offset, byte_size(Frame)}}
    end;
decode_disk_reader_frame(_Frame, _Path, _WantedIndex, Offset, _EncodedSize,
                         _Ordinal, _RecordsPerSegment) ->
    {error, {record_not_found, Offset}}.

decode_disk_reader_frame_payload(
  Path,
  Payload,
  WantedIndex,
  Offset,
  Ordinal,
  RecordsPerSegment
 ) ->
    case decode_segment_record(Path, Payload) of
        {ok, {Index, {_Term, _Op} = Entry}} when is_integer(Index), Index >= 0 ->
            case validate_record_segment_ordinal(Path, Index, Ordinal, RecordsPerSegment) of
                ok when Index =:= WantedIndex ->
                    {ok, Entry};
                ok ->
                    {error, {record_index_mismatch, Offset, WantedIndex, Index}};
                {error, _Reason} = Error ->
                    Error
            end;
        {ok, Other} ->
            {error, {bad_record, Other}};
        {error, Reason} ->
            {error, Reason}
    end.

close_disk_reader(
  {ferricstore_segment_disk_reader_v1, Fd, _Path, _FileBytes, _Ordinal,
   _RecordsPerSegment, _Dir}
 ) ->
    file:close(Fd);
close_disk_reader(_Reader) ->
    {error, bad_disk_reader}.

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

compact_apply_projection(RootDir, TrimIndex, RetainedBatches)
  when is_integer(TrimIndex), TrimIndex >= 0, is_list(RetainedBatches) ->
    Dir = fold_disk_segment_dir(RootDir),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            case segment_append_kind(Dir) of
                apply_projection ->
                    case projection_batch_records(RetainedBatches, []) of
                        {ok, Records} ->
                            compact_apply_projection_records(
                                Dir,
                                TrimIndex,
                                normalize_projection_batch_records(Records)
                            );
                        {error, _Reason} = Error ->
                            Error
                    end;
                Kind ->
                    {error, {not_apply_projection_log, Kind}}
            end;
        {error, Reason} ->
            {error, {ensure_apply_projection_compaction_dir, Reason}}
    end;
compact_apply_projection(_RootDir, TrimIndex, _RetainedBatches) ->
    {error, {bad_apply_projection_compaction_trim_index, TrimIndex}}.

compact_apply_projection_stream(RootDir, TrimIndex, PageFun)
  when is_integer(TrimIndex), TrimIndex >= 0, is_function(PageFun, 1) ->
    Dir = fold_disk_segment_dir(RootDir),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            case segment_append_kind(Dir) of
                apply_projection ->
                    compact_apply_projection_stream_records(Dir, TrimIndex, PageFun);
                Kind ->
                    {error, {not_apply_projection_log, Kind}}
            end;
        {error, Reason} ->
            {error, {ensure_apply_projection_compaction_dir, Reason}}
    end;
compact_apply_projection_stream(_RootDir, TrimIndex, _PageFun) ->
    {error, {bad_apply_projection_compaction_stream, TrimIndex}}.

write_projection_batch_records(Dir, Records, Mode) ->
    Normalized = normalize_projection_batch_records(Records),
    case segment_append_kind(Dir) of
        apply_projection ->
            %% Every appended duplicate is a complete view of its Raft index.
            %% Physical query-row locators may therefore address the latest
            %% frame directly without folding older duplicates.
            write_canonical_apply_projection_records(Dir, Normalized, Mode);
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

write_canonical_apply_projection_records(Dir, Records, Mode) ->
    case projection_records_append_only_fast_path(Dir, Records) of
        {ok, true} ->
            write_projection_records_mode(Dir, Records, Mode);
        {ok, false} ->
            case existing_records_per_segment(Dir) of
                {ok, RecordsPerSegment} ->
                    case canonical_apply_projection_records(Dir, Records, RecordsPerSegment, []) of
                        {ok, Canonical} -> write_projection_records_mode(Dir, Canonical, Mode);
                        {error, _Reason} = Error -> Error
                    end;
                not_found ->
                    write_projection_records_mode(Dir, Records, Mode);
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

canonical_apply_projection_records(_Dir, [], _RecordsPerSegment, Acc) ->
    {ok, lists:reverse(Acc)};
canonical_apply_projection_records(
  Dir,
  [{Index, NewEntry} | Rest],
  RecordsPerSegment,
  Acc
 ) ->
    case lookup_or_locate_offset(Dir, Index) of
        {ok, {_Ordinal, Offset, EncodedSize}} ->
            case read_disk_record_at(Dir, Index, Offset, EncodedSize, RecordsPerSegment) of
                {ok, ExistingEntry} ->
                    case merge_canonical_apply_projection_entry(ExistingEntry, NewEntry) of
                        {ok, MergedEntry} ->
                            canonical_apply_projection_records(
                                Dir,
                                Rest,
                                RecordsPerSegment,
                                [{Index, MergedEntry} | Acc]
                            );
                        {error, _Reason} = Error ->
                            Error
                    end;
                not_found ->
                    {error, {missing_apply_projection_overlap, Index}};
                {error, _Reason} = Error ->
                    Error
            end;
        not_found ->
            canonical_apply_projection_records(
                Dir,
                Rest,
                RecordsPerSegment,
                [{Index, NewEntry} | Acc]
            );
        {error, _Reason} = Error ->
            Error
    end.

merge_canonical_apply_projection_entry(
  {0, {ferricstore_segment_apply_projection_batch, _OldPosition, OldEntries}},
  {0, {ferricstore_segment_apply_projection_batch, NewPosition, NewEntries}}
 ) when is_list(OldEntries), is_list(NewEntries) ->
    {ok,
     {0,
      {ferricstore_segment_apply_projection_batch,
       NewPosition,
       merge_projection_entries(OldEntries, NewEntries)}}};
merge_canonical_apply_projection_entry(ExistingEntry, NewEntry) ->
    {error, {bad_apply_projection_overlap, ExistingEntry, NewEntry}}.

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
