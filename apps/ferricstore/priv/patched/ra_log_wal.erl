%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
%%
%% PATCHED by FerricStore: async fdatasync (updated for ra 3.x)
%%
%% Changes from upstream ra_log_wal (ra 3.1.2):
%%
%% 1. flush_pending/1 — writes data to kernel buffer but does NOT call sync.
%%    The original would call file:datasync/file:sync inline (blocking ~20ms on macOS).
%%
%% 2. complete_batch/1 — instead of notifying writers immediately after sync,
%%    it queues {Waiting, WalRanges} into pending_sync. Writers are NOT notified yet.
%%
%% 3. maybe_start_async_sync/1 — if no sync is in flight, spawns a linked process
%%    that calls file:datasync (or file:sync) and sends {wal_async_sync_complete}
%%    back. If a sync is already in flight, the new entries will be covered by the
%%    next sync cycle.
%%
%% 4. handle_op({info, {wal_async_sync_complete}}, ...) — when sync completes,
%%    releases only pending writer batches whose WAL end offset is covered by
%%    the completed fdatasync, then starts another sync if more data arrived.
%%
%% INVARIANT PRESERVED: Writers are ONLY notified AFTER fdatasync completes.
%% This preserves Raft's durability guarantee — a quorum of nodes must have
%% the entry on durable storage before the leader commits it.
%%
-module(ra_log_wal).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1
        ]).

-export([
         write/6,
         write/7,
         write_batch/2,
         last_writer_seq/2,
         force_roll_over/1,
         forget_writer/2,
         fill_ratio/1]).

-export([wal2list/1]).
-export(['__close_file_for_test__'/1,
         '__wal_write_for_test__'/3,
         '__adaptive_commit_delay_us_for_test__'/2,
         '__next_adaptive_commit_delay_us_for_test__'/4,
         '__complete_sync_partition_for_test__'/2,
         '__complete_sync_frontier_for_test__'/3]).

-compile([inline_list_funcs]).
-compile(inline).

-include("ra.hrl").

-define(CURRENT_VERSION, 1).
-define(MAGIC, "RAWA").
-define(HEADER_SIZE, 5).
-define(WAL_DELAY_FLOOR_US, 200).

-define(C_WAL_FILES, 1).
-define(C_BATCHES, 2).
-define(C_WRITES, 3).
-define(C_BYTES_WRITTEN, 4).
-define(C_CURRENT_FILE_SIZE, 5).
-define(C_MAX_FILE_SIZE, 6).
-define(COUNTER_FIELDS,
        [{wal_files, ?C_WAL_FILES, counter, "Number of write-ahead log files created"},
         {batches, ?C_BATCHES, counter, "Number of batches written"},
         {writes, ?C_WRITES, counter, "Number of entries written"},
         {bytes_written, ?C_BYTES_WRITTEN, counter, "Number of bytes written"},
         {current_file_size, ?C_CURRENT_FILE_SIZE, gauge,
          "Current WAL file size in bytes"},
         {max_file_size, ?C_MAX_FILE_SIZE, gauge,
          "Maximum WAL file size in bytes"}
         ]).

-define(FILE_MODES, [raw, write, read, binary]).

% a writer_id consists of a unique local name (see ra_directory) and a writer's
% current pid().
% The pid is used for the immediate writer notification
% The atom is used by the segment writer to send the segments
% This has the effect that a restarted server has a different identity in terms
% of it's write notification but the same identity in terms of it's ets
% tables and segment notification
-type writer_id() :: {ra_uid(), pid()}.

-record(batch_writer, {smallest_live_idx :: ra_index(),
                       tid :: ets:tid(),
                       uid :: term(),
                       seq :: ra_seq:state(),
                       term :: ra_term(),
                       old :: undefined | #batch_writer{}
                      }).

-record(batch, {num_writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{pid() => #batch_writer{}},
                pending = [] :: iolist()
               }).

-type writer_name_cache() :: {NextIntId :: non_neg_integer(),
                              #{writer_id() => binary()}}.

-record(conf, {dir :: file:filename_all(),
               system :: atom(),
               segment_writer = ra_log_segment_writer :: atom() | pid(),
               compute_checksums = false :: boolean(),
               max_size_bytes :: non_neg_integer(),
               max_entries :: undefined | non_neg_integer(),
               recovery_chunk_size = ?WAL_RECOVERY_CHUNK_SIZE :: non_neg_integer(),
               sync_method = datasync :: sync | datasync | none,
               counter :: counters:counters_ref(),
               mem_tables_tid :: ets:tid(),
               names :: ra_system:names(),
               explicit_gc = false :: boolean(),
               pre_allocate = false :: boolean(),
               ra_log_snapshot_state_tid :: ets:tid(),
               wal_io_module :: atom() | undefined,
               wal_commit_delay_us = 6000 :: non_neg_integer(),
               wal_max_buffer_bytes = 67108864 :: non_neg_integer()
              }).

-record(wal, {fd :: option(file:io_device()),
              filename :: option(file:filename()),
              file_size = 0 :: non_neg_integer(),
              writer_name_cache = {0, #{}} :: writer_name_cache(),
              max_size :: non_neg_integer(),
              entry_count = 0 :: non_neg_integer(),
              ranges = #{} :: #{ra_uid() =>
                                [{ets:tid(), {ra:index(), ra:index()}}]}
              }).

-record(recovery, {mode :: initial | post_boot,
                   ranges = #{} :: #{ra_uid() =>
                                     [{ets:tid(), ra_seq:state()}]},
                   tables = #{} :: #{ra_uid() => ra_mt:state()},
                   writers = #{} :: #{ra_uid() => {in_seq, ra:index()}}
                  }).

%% PATCHED: added pending_sync and sync_in_flight fields
-record(state, {conf = #conf{},
                file_num = 0 :: non_neg_integer(),
                wal :: #wal{} | undefined,
                % writers that have attempted to write an non-truncating
                % out of seq % entry.
                % No further writes are allowed until the missing
                % index has been received.
                % out_of_seq are kept after a roll over or until
                % a truncating write is received.
                % no attempt is made to recover this information after a crash
                % beyond the available WAL files
                % all writers seen within the lifetime of a WAL file
                % and the last index seen
                writers = #{} :: #{ra_uid() =>
                                   {in_seq | out_of_seq, ra_index()}},
                batch :: option(#batch{}),
                %% --- async fdatasync patch fields ---
                %% List of {Waiting, WalRanges} tuples queued for writer
                %% notification by a future fdatasync. Batches appended while
                %% sync_in_flight=true stay here until the current sync
                %% completes, then they are covered by the next sync cycle.
                %% Newest entries are prepended (reversed order).
                pending_sync = [] :: [{map(), map(), non_neg_integer()}],
                %% Batches covered by the currently in-flight fdatasync. Only
                %% this set may be notified when that fdatasync completes.
                syncing_sync = [] :: [{map(), map(), non_neg_integer()}],
                %% True when a spawned process is currently running fdatasync.
                sync_in_flight = false :: boolean(),
                %% Monotonic timestamp for the current async fdatasync. Used only
                %% for low-cardinality telemetry so SET p99 can be profiled.
                sync_started_at = undefined :: undefined | integer(),
                %% Adaptive per-sync delay selected when fdatasync is requested.
                sync_delay_us = 0 :: non_neg_integer(),
                %% Delay to try for the next fdatasync. This is adjusted from
                %% completion pressure: queued batches ramp it up, idle syncs
                %% decay it back to the low-latency floor.
                sync_next_delay_us = ?WAL_DELAY_FLOOR_US :: non_neg_integer()
               }).

-type state() :: #state{}.
-type wal_conf() :: #{names := ra_system:names(),
                      system := atom(),
                      dir := file:filename_all(),
                      max_size_bytes => non_neg_integer(),
                      max_entries => non_neg_integer(),
                      compute_checksums => boolean(),
                      pre_allocate => boolean(),
                      sync_method => sync | datasync,
                      recovery_chunk_size  => non_neg_integer(),
                      hibernate_after => non_neg_integer(),
                      max_batch_size => non_neg_integer(),
                      garbage_collect => boolean(),
                      min_heap_size => non_neg_integer(),
                      min_bin_vheap_size => non_neg_integer()
                     }.

-export_type([wal_conf/0]).

-type wal_command() ::
    {append, writer_id(), PrevIndex :: ra:index() | -1,
     Index :: ra:index(), Term :: ra_term(), wal_cmd()}.

-type wal_op() :: {cast, wal_command()} |
                  {call, from(), wal_command()}.
-type wal_cmd() :: term() | {ttb, iodata()}.

-spec write(atom() | pid(), writer_id(), ets:tid(), ra_index(), ra_term(),
            wal_cmd()) ->
    {ok, pid()} | {error, wal_down}.
write(Wal, From, MtTid, Idx, Term, Cmd) ->
    %% normal operation where we assume a contiguous sequence
    %% this may be removed at some point
    write(Wal, From, MtTid, Idx-1, Idx, Term, Cmd).

-spec write(atom() | pid(), writer_id(), ets:tid(),
            PrevIndex :: ra:index() | -1,
            Index :: ra_index(),
            Term :: ra_term(),
            wal_cmd()) ->
    {ok, pid()} | {error, wal_down}.
write(Wal, {_, _} = From, MtTid, PrevIdx, Idx, Term, Cmd)
  when is_integer(Idx) andalso
       is_integer(PrevIdx) andalso
       is_integer(Term) andalso
       PrevIdx < Idx ->
    named_cast(Wal, {append, From, MtTid, PrevIdx, Idx, Term, Cmd}).

-spec write_batch(Wal :: atom() | pid(), [wal_command()]) ->
    {ok, pid()} | {error, wal_down}.
write_batch(Wal, WalCommands) when is_pid(Wal) ->
    case is_process_alive(Wal) of
        true ->
            gen_batch_server:cast_batch(Wal, WalCommands),
            {ok, Wal};
        false ->
            {error, wal_down}
    end;
write_batch(Wal, WalCommands) when is_atom(Wal) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            write_batch(Pid, WalCommands)
    end.

-spec last_writer_seq(Wal :: atom() | pid(), ra:uid()) ->
    {ok, undefined | ra:index()} | {error, wal_down}.
last_writer_seq(Wal, UId) when is_pid(Wal) ->
    case is_process_alive(Wal) of
        true ->
            {ok, gen_batch_server:call(Wal, {?FUNCTION_NAME, UId}, infinity)};
        false ->
            {error, wal_down}
    end;
last_writer_seq(Wal, UId) when is_atom(Wal) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            last_writer_seq(Pid, UId)
    end.

% force a wal file to roll over to a new file
% mostly useful for testing
force_roll_over(Wal) ->
    ok = gen_batch_server:cast(Wal, rollover),
    ok.

-spec forget_writer(atom() | pid(), ra_uid()) -> ok.
forget_writer(Wal, UId) ->
    gen_batch_server:cast(Wal, {forget_writer, UId}),
    ok.

-spec fill_ratio(atom()) -> float().
fill_ratio(WalName) ->
    case ra_counters:fetch(WalName) of
        undefined ->
            0.5;
        CRef ->
            Max = counters:get(CRef, ?C_MAX_FILE_SIZE),
            case Max > 0 of
                true ->
                    min(1.0, counters:get(CRef, ?C_CURRENT_FILE_SIZE) / Max);
                false ->
                    0.5
            end
    end.

%% ra_log_wal
%%
%% Writes Raft entries to shared persistent storage for multiple "writers"
%% fsyncs in batches, typically the write requests
%% received in the mailbox during
%% the previous fsync operation. Notifies all writers after each fsync batch.
%% Also have got a dynamically increasing max writes limit that grows in order
%% to trade-off latency for throughput.
%%
%% Entries are written to the .wal file as well as a per-writer mem table (ETS).
%% The mem table state is managed by ra_mt and tracked via the open_mem_tbls
%% ETS table (see ra_log_ets). Each writer maintains its own mem table chain
%% that allows entries to be read while older tables are being flushed to disk.
%%
%% Once the current .wal file is full a new one is opened. The
%% ra_log_segment_writer is notified of all the mem tables written to during
%% the lifetime of the .wal file and will begin writing these to on-disk segment
%% files. Once it has finished the current set of mem_tables it will delete the
%% corresponding .wal file.

-spec start_link(Config :: wal_conf()) ->
    {ok, pid()} |
    {error, {already_started, pid()}} |
    {error, wal_checksum_validation_failure}.
start_link(#{dir := _,
             system := _,
             names := #{wal := Name}} = Config)
  when is_atom(Name) ->
    WalMaxBatchSize = maps:get(max_batch_size, Config,
                               ?WAL_DEFAULT_MAX_BATCH_SIZE),
    Options0 = case maps:get(hibernate_after, Config, undefined) of
                   undefined ->
                       [{max_batch_size, WalMaxBatchSize}];
                   Hib ->
                       [{hibernate_after, Hib},
                        {max_batch_size, WalMaxBatchSize}]
               end,
    Options = [{reversed_batch, true} | Options0],
    gen_batch_server:start_link({local, Name}, ?MODULE, Config, Options).

%%% Callbacks

-spec init(wal_conf()) ->
    {ok, state()} |
    {stop, wal_checksum_validation_failure} | {stop, term()}.
init(#{system := System,
       dir := Dir} = Conf0) ->
    #{max_size_bytes := MaxWalSize,
      max_entries := MaxEntries,
      recovery_chunk_size := RecoveryChunkSize,
      compute_checksums := ComputeChecksums,
      pre_allocate := PreAllocate,
      sync_method := SyncMethod,
      garbage_collect := Gc,
      min_heap_size := MinHeapSize,
      min_bin_vheap_size := MinBinVheapSize,
      names := #{wal := WalName,
                 segment_writer := SegWriter,
                 open_mem_tbls := MemTablesName} = Names} =
        merge_conf_defaults(Conf0),
    ?NOTICE("WAL: ~ts init (async-sync patched for ra 3.x), system: ~ts",
            [WalName, System]),
    process_flag(trap_exit, true),
    % given ra_log_wal is effectively a fan-in sink it is likely that it will
    % at times receive large number of messages from a large number of
    % writers
    process_flag(message_queue_data, off_heap),
    process_flag(min_bin_vheap_size, MinBinVheapSize),
    process_flag(min_heap_size, MinHeapSize),
    CRef = ra_counters:new(WalName,
                           ?COUNTER_FIELDS,
                           #{ra_system => System, module => ?MODULE}),
    WalIoModule = maps:get(wal_io_module, Conf0, undefined),
    WalCommitDelayUs = maps:get(wal_commit_delay_us, Conf0, 6000),
    WalMaxBufferBytes = maps:get(wal_max_buffer_bytes, Conf0, 64 * 1024 * 1024),
    Conf = #conf{dir = Dir,
                 system = System,
                 segment_writer = SegWriter,
                 compute_checksums = ComputeChecksums,
                 max_size_bytes = max(?WAL_MIN_SIZE, MaxWalSize),
                 max_entries = MaxEntries,
                 recovery_chunk_size = RecoveryChunkSize,
                 sync_method = SyncMethod,
                 counter = CRef,
                 mem_tables_tid = ets:whereis(MemTablesName),
                 names = Names,
                 explicit_gc = Gc,
                 pre_allocate = PreAllocate,
                 ra_log_snapshot_state_tid = ets:whereis(ra_log_snapshot_state),
                 wal_io_module = WalIoModule,
                 wal_commit_delay_us = WalCommitDelayUs,
                 wal_max_buffer_bytes = WalMaxBufferBytes},
    try recover_wal(Dir, Conf) of
        Result ->
            % wait for the segment writer to process any flush requests
            % generated during recovery
            ok = ra_log_segment_writer:await(SegWriter),
            {ok, Result}
    catch _:Err:Stack ->
              ?ERROR("WAL in ~ts failed to initialise with ~p, stack ~p",
                     [System, Err, Stack]),
              {stop, Err}
    end.

-spec handle_batch([wal_op()], state()) ->
    {ok, [gen_batch_server:action()], state()}.
handle_batch(Ops, #state{conf = #conf{explicit_gc = Gc}} = State0) ->
    Actions0 = case Gc of
                   true ->
                       [garbage_collect];
                   false ->
                       []
               end,
    {State, Actions} = lists:foldr(fun handle_op/2,
                                   {start_batch(State0), Actions0}, Ops),
    %% process all ops
    {ok, Actions, complete_batch(State)}.

terminate(Reason, #state{conf = #conf{system = System}} = State) ->
    ?DEBUG("WAL in ~ts: terminating with ~0P", [System, Reason, 20]),
    _ = cleanup(State),
    ok.

format_status(#state{conf = #conf{sync_method = SyncMeth,
                                  compute_checksums = Cs,
                                  names = #{wal := WalName},
                                  max_size_bytes = MaxSize},
                     writers = Writers,
                     pending_sync = PendingSync,
                     syncing_sync = SyncingSync,
                     sync_in_flight = SyncInFlight,
                     sync_next_delay_us = NextDelayUs,
                     wal = #wal{file_size = FSize,
                                filename = Fn}}) ->
    #{sync_method => SyncMeth,
      compute_checksums => Cs,
      writers => maps:size(Writers),
      filename => filename:basename(Fn),
      current_size => FSize,
      max_size_bytes => MaxSize,
      counters => ra_counters:overview(WalName),
                     %% PATCHED: expose async sync state
                     async_sync => #{pending_batches => length(PendingSync),
                      syncing_batches => length(SyncingSync),
                      sync_in_flight => SyncInFlight,
                      next_delay_us => NextDelayUs}
     }.

%% Internal

handle_op({cast, WalCmd}, {State, Actions}) ->
    {handle_msg(WalCmd, State), Actions};
handle_op({call, From, {last_writer_seq, UId}},
           {#state{writers = Writers} = State, Actions}) ->
    {_, Res} = maps:get(UId, Writers, {undefined, undefined}),
    {State, [{reply, From, Res} | Actions]};
%% PATCHED: handle async fdatasync completion message.
%% gen_batch_server wraps info messages as {info, Msg} before passing them
%% to the module's ops processing. When our spawned sync process sends
%% {wal_async_sync_complete} back to this process, it arrives here.
handle_op({info, {wal_async_sync_complete, SyncedFileSize}},
          {#state{} = State0, Actions}) ->
    emit_wal_sync_telemetry(State0, ok, SyncedFileSize),
    State2 = complete_async_sync(State0, SyncedFileSize),
    State3 = maybe_start_async_sync(State2),
    {State3, Actions};
handle_op({info, {wal_async_sync_complete}},
          {#state{} = State0, Actions}) ->
    emit_wal_sync_telemetry(State0, ok),
    State2 = complete_async_sync(State0, undefined),
    State3 = maybe_start_async_sync(State2),
    {State3, Actions};
handle_op({info, {wal_sync_complete, _Ref, SyncedFileSize}},
          {#state{} = State0, Actions}) ->
    emit_wal_sync_telemetry(State0, ok, SyncedFileSize),
    State2 = complete_async_sync(State0, SyncedFileSize),
    State3 = maybe_start_async_sync(State2),
    {State3, Actions};
handle_op({info, {wal_sync_complete, _Ref}},
          {#state{} = State0, Actions}) ->
    emit_wal_sync_telemetry(State0, ok),
    State2 = complete_async_sync(State0, undefined),
    State3 = maybe_start_async_sync(State2),
    {State3, Actions};
handle_op({info, {wal_sync_error, _Ref, Reason}}, {#state{} = State0, _Actions}) ->
    emit_wal_sync_telemetry(State0, error),
    throw({stop, {wal_sync_failed, Reason}});
handle_op({info, {'EXIT', _, normal}}, {State, Actions}) ->
    {State, Actions};
handle_op({info, {'EXIT', _, Reason}}, _State) ->
    %% This is here for testing purposes (original ra code) and also catches
    %% failures from our async sync process (e.g., fdatasync I/O error).
    throw({stop, Reason}).

recover_wal(Dir, #conf{system = System,
                       segment_writer = SegWriter,
                       mem_tables_tid = MemTblsTid} = Conf) ->
    % ensure configured directory exists
    ok = ra_lib:make_dir(Dir),

    %% TODO: provide a proper ra_log_ets API to discover recovery mode
    Mode = case ets:info(MemTblsTid, size) of
               0 ->
                   %% there are no mem tables
                   initial;
               _ ->
                   %% in this case it is possible that the segment writer
                   %% could be flushing wal data right now so we need to
                   %% wait for the segment writer to finish the current work
                   %% before we get the wal files to recover
                   ok = ra_log_segment_writer:await(SegWriter),
                   post_boot
           end,
    {ok, Files0} = file:list_dir(Dir),
    Files = [begin
                 ra_lib:zpad_upgrade(Dir, File, ".wal")
             end || File <- Files0,
                    filename:extension(File) == ".wal"],
    WalFiles = lists:sort(Files),
    SeedWriters = recover_writers(Dir),
    ?DEBUG("WAL in ~ts: recovered ~b writers from snapshot file",
           [System, map_size(SeedWriters)]),
    {FinalWriters, _FinalTables} =
    lists:foldl(fun (F, {Writers0, Tables0}) ->
                        ?DEBUG("WAL in ~ts: recovering ~ts, Mode ~s",
                               [System, F, Mode]),
                        WalFile = filename:join(Dir, F),
                        Fd = open_at_first_record(WalFile),
                        {Time, #recovery{ranges = Ranges,
                                         writers = Writers,
                                         tables = Tables}} =
                            timer:tc(fun () ->
                                             recover_wal_chunks(Conf, Fd,
                                                                Writers0,
                                                                Tables0, Mode)
                                     end),

                        ok = ra_log_segment_writer:accept_mem_tables(SegWriter,
                                                                     Ranges,
                                                                     WalFile),
                        close_existing(Fd),
                        ?DEBUG("WAL in ~ts: recovered ~ts time taken ~bms - recovered ~b writers",
                               [System, F, Time div 1000, map_size(Writers)]),
                        {Writers, Tables}
                end, {SeedWriters, #{}}, WalFiles),

    ?DEBUG("WAL in ~ts: final writers recovered ~b",
           [System, map_size(FinalWriters)]),

    %% trim writers for UIDs that are no longer registered
    RegisteredUIds = sets:from_list(
                       [UId || {_, UId}
                               <- ra_directory:list_registered(Conf#conf.names)],
                       [{version, 2}]),
    TrimmedWriters = maps:filter(
                       fun (UId, _) ->
                               sets:is_element(UId, RegisteredUIds)
                       end, FinalWriters),
    NumTrimmed = map_size(FinalWriters) - map_size(TrimmedWriters),
    NumTrimmed > 0 andalso
        ?DEBUG("WAL in ~ts: trimmed ~b stale writers",
               [System, NumTrimmed]),

    FileNum = extract_file_num(lists:reverse(WalFiles)),
    State = roll_over(#state{conf = Conf,
                             writers = TrimmedWriters,
                             file_num = FileNum}),
    true = erlang:garbage_collect(),
    State.

extract_file_num([]) ->
    0;
extract_file_num([F | _]) ->
    ra_lib:zpad_extract_num(filename:basename(F)).


cleanup(#state{wal = #wal{fd = undefined}}) ->
    ok;
cleanup(#state{wal = #wal{fd = Fd},
               conf = #conf{wal_io_module = IoMod,
                            sync_method = SyncMeth}} = State) ->
    _ = drain_pending_sync(State),
    ok = wal_sync_blocking(IoMod, Fd, SyncMeth),
    ok = close_file(Fd),
    ok.

serialize_header(UId, Trunc, {Next, Cache} = WriterCache) ->
    case Cache of
        #{UId := <<_:1, BinId:23/bitstring>>} when Trunc ->
            {<<1:1/unsigned, BinId/bitstring>>, 2, WriterCache};
        #{UId := BinId} ->
            {BinId, 3, WriterCache};
        _ ->
            % TODO: check overflows of Next
            % cache the header index binary to avoid re-creating it every time
            % sets Truncate  = false initially as this is the most common case
            T = case Trunc of true -> 1; false -> 0 end,
            BinId = <<0:1/unsigned, 1:1/unsigned, Next:22/unsigned>>,
            IdDataLen = byte_size(UId),
            Header = <<T:1/unsigned, 0:1/unsigned, Next:22/unsigned,
                       IdDataLen:16/unsigned, UId/binary>>,
            {Header, byte_size(Header),
             {Next + 1, Cache#{UId => BinId}}}
    end.

write_data({UId, Pid} = Id, MtTid, Idx, Term, Data0, Trunc, SmallestIndex,
           #state{conf = #conf{counter = Counter,
                               compute_checksums = ComputeChecksum} = _Cfg,
                  batch = Batch0,
                  writers = Writers,
                  wal = #wal{writer_name_cache = Cache0,
                             file_size = FileSize,
                             entry_count = Count} = Wal} = State0) ->
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case should_roll_wal(State0) of
        true ->
            State = complete_batch_and_roll(State0),
            write_data(Id, MtTid, Idx, Term, Data0, Trunc, SmallestIndex, State);
        false ->
            EntryData = case Data0 of
                            {ttb, Bin} ->
                                Bin;
                            _ ->
                                to_binary(Data0)
                        end,
            EntryDataLen = iolist_size(EntryData),
            {HeaderData, HeaderLen, Cache} = serialize_header(UId, Trunc, Cache0),
            % fixed overhead =
            % 24 bytes 2 * 64bit ints (idx, term) + 2 * 32 bit ints (checksum, datalen)
            DataSize = HeaderLen + 24 + EntryDataLen,

            Entry = [<<Idx:64/unsigned,
                       Term:64/unsigned>> |
                     EntryData],
            Checksum = case ComputeChecksum of
                           true -> erlang:adler32(Entry);
                           false -> 0
                       end,
            Record = [HeaderData,
                      <<Checksum:32/integer, EntryDataLen:32/unsigned>> |
                      Entry],
            Batch = incr_batch(Batch0, UId, Pid, MtTid,
                               Idx, Term, Record, SmallestIndex),
            NewFileSize = FileSize + DataSize,
            counters:add(Counter, ?C_BYTES_WRITTEN, DataSize),
            counters:put(Counter, ?C_CURRENT_FILE_SIZE, NewFileSize),
            State0#state{batch = Batch,
                         wal = Wal#wal{writer_name_cache = Cache,
                                       file_size = NewFileSize,
                                       entry_count = Count + 1},
                         writers = Writers#{UId => {in_seq, Idx}}}
    end.


handle_msg({append, {UId, Pid} = Id, MtTid, ExpectedPrevIdx, Idx, Term, Entry},
           #state{conf = Conf,
                  writers = Writers} = State0) ->
    SmallestIdx = smallest_live_index(Conf, UId),
    %% detect if truncating flag should be set
    Trunc = Idx == SmallestIdx,

    case maps:get(UId, Writers, undefined) of
        _ when Idx < SmallestIdx ->
            %% the smallest live index for the last snapshot is higher than
            %% this index, just drop it
            LastIdx = SmallestIdx - 1,
            State0#state{writers = Writers#{UId => {in_seq, LastIdx}}};
        {_, PrevIdx}
          when ExpectedPrevIdx =< PrevIdx orelse
               Trunc ->
            %% if the passed in previous index is less than the last written
            %% index (gap detection) _or_ it is a truncation
            %% then we can proceed and write the entry
            write_data(Id, MtTid, Idx, Term, Entry, Trunc, SmallestIdx, State0);
        undefined ->
            %% no state for the UId is known so go ahead and write
            write_data(Id, MtTid, Idx, Term, Entry, false, SmallestIdx, State0);
        {out_of_seq, _} ->
            % writer is out of seq simply ignore drop the write
            % TODO: capture metric for dropped writes?
            State0;
        {in_seq, PrevIdx} ->
            % writer was in seq but has sent an out of seq entry
            % notify writer
            ?DEBUG("WAL in ~ts: requesting resend for `~s`, "
                   "last idx ~b idx received (~b,~b)",
                   [Conf#conf.system, UId, PrevIdx, ExpectedPrevIdx, Idx]),
            Pid ! {ra_log_event, {resend_write, PrevIdx + 1}},
            State0#state{writers = Writers#{UId => {out_of_seq, PrevIdx}}}
    end;
handle_msg({query, Fun}, State) ->
    %% for testing
    _ = catch Fun(State),
    State;
handle_msg(rollover, State) ->
    complete_batch_and_roll(State);
handle_msg({forget_writer, UId}, #state{writers = Writers} = State) ->
    State#state{writers = maps:remove(UId, Writers)}.

incr_batch(#batch{num_writes = Writes,
                  waiting = Waiting0,
                  pending = Pend} = Batch,
           UId, Pid, MT_TID = MtTid,
           Idx, TERM = Term, Data, SmallestLiveIdx) ->
    Waiting = case Waiting0 of
                  #{Pid := #batch_writer{term = TERM,
                                         tid = MT_TID,
                                         seq = Seq0} = W} ->
                      %% The Tid and term is the same so add to
                      %% current batch_writer
                      Seq = case Idx > ra_seq:last(Seq0) of
                                true ->
                                    ra_seq:append(Idx, Seq0);
                                false ->
                                    %% this is a rewrite / resend
                                    %% we need to limit the seq before
                                    %% appending
                                    ra_seq:append(Idx, ra_seq:limit(Idx - 1,
                                                                    Seq0))
                            end,
                      Waiting0#{Pid => W#batch_writer{seq = Seq,
                                                      smallest_live_idx = SmallestLiveIdx,
                                                      term = Term}};
                  _ ->
                      %% The tid or term is different
                      %% open a new batch writer for the new tid and term
                      PrevBatchWriter = maps:get(Pid, Waiting0, undefined),
                      Writer = #batch_writer{smallest_live_idx = SmallestLiveIdx,
                                             tid = MtTid,
                                             seq = ra_seq:append(Idx, []),
                                             uid = UId,
                                             term = Term,
                                             old = PrevBatchWriter},
                      Waiting0#{Pid => Writer}
              end,

    Batch#batch{num_writes = Writes + 1,
                waiting = Waiting,
                pending = [Pend | Data]}.

%% PATCHED: complete_batch_and_roll must ensure pending sync is flushed
%% before rolling over, because the Fd changes during rollover.
%% We do a SYNCHRONOUS sync here to drain all pending notifications,
%% because the old Fd is about to be closed.
complete_batch_and_roll(#state{} = State0) ->
    State1 = complete_batch(State0),
    %% Before rolling over, we must synchronously drain any in-flight
    %% async sync and notify all pending writers. The old Fd is about
    %% to be closed, so we can't have an async sync referencing it.
    State2 = drain_pending_sync(State1),
    roll_over(start_batch(State2)).

%% Synchronously drain any pending async sync. Called before WAL rollover
%% and during terminate to ensure all writers are notified.
drain_pending_sync(#state{pending_sync = [],
                          syncing_sync = [],
                          sync_in_flight = false} = State) ->
    State;
drain_pending_sync(#state{sync_in_flight = true} = State) ->
    SyncResult = wait_for_sync_complete(),
    State2 = case SyncResult of
                 {ok, SyncedFileSize} ->
                     complete_async_sync(State, SyncedFileSize);
                 ok ->
                     complete_async_sync(State, undefined)
             end,
    drain_pending_sync(State2);
drain_pending_sync(#state{wal = #wal{fd = Fd},
                          conf = #conf{sync_method = SyncMeth,
                                       wal_io_module = IoMod},
                          pending_sync = Pending} = State) when Pending =/= [] ->
    wal_sync_blocking(IoMod, Fd, SyncMeth),
    drain_pending_sync(notify_all_pending(State));
drain_pending_sync(#state{wal = #wal{fd = Fd},
                          conf = #conf{sync_method = SyncMeth,
                                       wal_io_module = IoMod},
                          sync_in_flight = false,
                          syncing_sync = Syncing} = State) when Syncing =/= [] ->
    wal_sync_blocking(IoMod, Fd, SyncMeth),
    drain_pending_sync(notify_syncing_pending(State#state{sync_in_flight = false})).

wait_for_sync_complete() ->
    receive
        {wal_async_sync_complete, SyncedFileSize} -> {ok, SyncedFileSize};
        {wal_async_sync_complete} -> ok;
        {wal_sync_complete, _Ref, SyncedFileSize} -> {ok, SyncedFileSize};
        {wal_sync_complete, _Ref} -> ok;
        {wal_sync_error, _Ref, Reason} -> throw({stop, {wal_sync_failed, Reason}})
    after 30000 -> throw({stop, wal_sync_timeout})
    end.

wal_sync_blocking(undefined, Fd, SyncMeth) ->
    sync(Fd, SyncMeth);
wal_sync_blocking(IoMod, Handle, _SyncMeth) ->
    Ref = make_ref(),
    ok = wal_sync_async(IoMod, Handle, self(), Ref, 0),
    receive
        {wal_sync_complete, Ref, _SyncedFileSize} -> ok;
        {wal_sync_complete, Ref} -> ok;
        {wal_sync_error, Ref, Reason} -> throw({stop, {wal_sync_failed, Reason}})
    after 30000 ->
        throw({stop, wal_sync_timeout})
    end.

roll_over(#state{wal = Wal0, file_num = Num0,
                 writers = Writers,
                 conf = #conf{dir = Dir,
                              system = System,
                              segment_writer = SegWriter,
                              max_size_bytes = MaxBytes} = Conf0} = State0) ->
    counters:add(Conf0#conf.counter, ?C_WAL_FILES, 1),
    Num = Num0 + 1,
    Fn = ra_lib:zpad_filename("", "wal", Num),
    NextFile = filename:join(Dir, Fn),
    ?DEBUG("WAL in ~ts: opening new file ~ts", [System, Fn]),
    ok = persist_writers(Dir, Writers),
    NextMaxBytes =
        case Wal0 of
            undefined ->
                Half = MaxBytes div 2,
                Half + rand:uniform(Half);
            #wal{ranges = Ranges,
                 filename = Filename} ->
                _ = file:advise(Wal0#wal.fd, 0, 0, dont_need),
                ok = close_file(Wal0#wal.fd),
                MemTables = maps:map(
                              fun (UId, TidRanges) ->
                                      SmallestIdx = smallest_live_index(Conf0, UId),
                                      [{Tid, ra_seq:floor(SmallestIdx, Seq)}
                                       || {Tid, Seq} <- TidRanges]
                              end, Ranges),
                ok = ra_log_segment_writer:accept_mem_tables(SegWriter,
                                                             MemTables,
                                                             Filename),
                MaxBytes
        end,

    {Conf, Wal} = open_wal(NextFile, NextMaxBytes, Conf0),
    counters:put(Conf#conf.counter, ?C_CURRENT_FILE_SIZE, 0),
    counters:put(Conf#conf.counter, ?C_MAX_FILE_SIZE, NextMaxBytes),
    %% ignore the result as not supported on windows
    _ = ra_lib:sync_dir(Dir),
    State0#state{conf = Conf,
                 wal = Wal,
                 file_num = Num}.

open_wal(File, Max, #conf{wal_io_module = IoMod,
                          wal_commit_delay_us = CommitDelayUs,
                          wal_max_buffer_bytes = MaxBufBytes} = Conf0) ->
    {Fd, Conf} = case IoMod of
        undefined ->
            {ok, Fd0} = prepare_file(File, ?FILE_MODES),
            {Fd0, maybe_pre_allocate(Conf0, Fd0, Max)};
        _Mod ->
            Tmp = make_tmp(File),
            ok = prim_file:rename(Tmp, File),
            FileBin = case File of
                          L when is_list(L) -> list_to_binary(L);
                          B when is_binary(B) -> B
                      end,
            {ok, Handle} = IoMod:open(FileBin, CommitDelayUs, Max, MaxBufBytes),
            persistent_term:put(ferricstore_wal_handle, Handle),
            {Handle, Conf0}
    end,
    {Conf, #wal{fd = Fd,
                max_size = Max,
                filename = File}}.

persist_writers(Dir, Writers) ->
    File = writers_snapshot_file(Dir),
    Tmp = File ++ ".tmp",
    Bin = term_to_binary(Writers),
    ok = ra_lib:write_file(Tmp, Bin),
    ok = prim_file:rename(Tmp, File).

recover_writers(Dir) ->
    File = writers_snapshot_file(Dir),
    case file:read_file(File) of
        {ok, Bin} ->
            try binary_to_term(Bin)
            catch _:Err ->
                ?WARN("~ts: failed to deserialise writers snapshot ~ts: ~p",
                      [Dir, File, Err]),
                #{}
            end;
        {error, enoent} ->
            #{};
        {error, Reason} ->
            ?WARN("~ts: failed to read writers snapshot ~ts: ~p",
                  [Dir, File, Reason]),
            #{}
    end.

writers_snapshot_file(Dir) ->
    filename:join(Dir, "writers.snapshot").

prepare_file(File, Modes) ->
    Tmp = make_tmp(File),
    %% rename is atomic-ish so we will never accidentally write an empty wal file
    %% using prim_file here as file:rename/2 uses the file server
    ok = prim_file:rename(Tmp, File),
    case file:open(File, Modes) of
        {ok, Fd2} ->
            {ok, ?HEADER_SIZE} = file:position(Fd2, ?HEADER_SIZE),
            {ok, Fd2};
        {error, _} = Err ->
            Err
    end.

make_tmp(File) ->
    Tmp = filename:rootname(File) ++ ".tmp",
    {ok, Fd} = file:open(Tmp, [write, binary, raw]),
    ok = file:write(Fd, <<?MAGIC, ?CURRENT_VERSION:8/unsigned>>),
    ok = ra_file:sync(Fd),
    ok = file:close(Fd),
    Tmp.

maybe_pre_allocate(#conf{system = System,
                         pre_allocate = true} = Conf, Fd, Max0) ->
    Max = Max0 - ?HEADER_SIZE,
    case file:allocate(Fd, ?HEADER_SIZE, Max) of
        ok ->
            {ok, Max} = file:position(Fd, Max),
            ok = file:truncate(Fd),
            {ok, ?HEADER_SIZE} = file:position(Fd, ?HEADER_SIZE),
            Conf;
        {error, _} ->
            %% fallocate may not be supported, fall back to fsync instead
            %% of fdatasync
            ?INFO("WAL in ~ts: preallocation may not be supported by the file system"
                  " falling back to fsync instead of fdatasync",
                  [System]),
            Conf#conf{pre_allocate = false}
    end;
maybe_pre_allocate(Conf, _Fd, _Max0) ->
    Conf.

close_file(undefined) ->
    ok;
close_file(Fd) when is_pid(Fd) ->
    close_file_result(file:close(Fd));
close_file(Handle) ->
    try ferricstore_wal_nif:close(Handle) of
        ok ->
            ok;
        {error, Reason} ->
            exit({could_not_close, Reason});
        Other ->
            exit({could_not_close, Other})
    catch
        error:badarg ->
            %% Not a NIF handle — try standard file:close
            close_file_fallback(Handle);
        Class:Reason ->
            exit({could_not_close, {Class, Reason}})
    end.

'__close_file_for_test__'(Handle) ->
    close_file(Handle).

close_file_fallback(Handle) ->
    try file:close(Handle) of
        Result ->
            close_file_result(Result)
    catch
        error:badarg ->
            exit({could_not_close, badarg});
        Class:Reason ->
            exit({could_not_close, {Class, Reason}})
    end.

close_file_result(ok) ->
    ok;
close_file_result({error, Reason}) ->
    exit({could_not_close, Reason});
close_file_result(Other) ->
    exit({could_not_close, Other}).

start_batch(#state{conf = #conf{counter = CRef}} = State) ->
    ok = counters:add(CRef, ?C_BATCHES, 1),
    State#state{batch = #batch{}}.

%% PATCHED: flush_pending now ONLY writes data, never syncs.
%% The sync is done asynchronously by maybe_start_async_sync/1.
flush_pending(#state{wal = #wal{fd = Fd},
                     batch = #batch{pending = Pend},
                     conf = #conf{wal_io_module = IoMod}} = State0) ->
    case wal_write(IoMod, Fd, Pend) of
        ok ->
            %% NO sync here — it's done asynchronously
            State0#state{batch = undefined};
        {error, Reason} ->
            throw({stop, {wal_write_failed, Reason}});
        Other ->
            throw({stop, {wal_write_failed, Other}})
    end.

'__wal_write_for_test__'(IoMod, Handle, Data) ->
    wal_write(IoMod, Handle, Data).

wal_write(undefined, Fd, Data) ->
    file:write(Fd, Data);
wal_write(IoMod, Handle, Data) ->
    wal_write_retry(IoMod, Handle, Data, 50, undefined).

wal_write_retry(_IoMod, _Handle, _Data, 0, LastError) ->
    ?WARN("WAL write failed after retries: ~p", [LastError]),
    {error, {retries_exhausted, LastError}};
wal_write_retry(IoMod, Handle, Data, Retries, _LastError) ->
    try IoMod:write(Handle, Data) of
        ok -> ok;
        {error, Reason} ->
            timer:sleep(1),
            wal_write_retry(IoMod, Handle, Data, Retries - 1, Reason);
        Other ->
            {error, {unexpected_result, Other}}
    catch
        Class:Reason ->
            timer:sleep(1),
            wal_write_retry(IoMod, Handle, Data, Retries - 1, {Class, Reason})
    end.

sync(_Fd, none) ->
    ok;
sync(Fd, Meth) ->
    ok = file:Meth(Fd),
    ok.

%% PATCHED: complete_batch queues writers for async notification instead of
%% notifying them inline.
complete_batch(#state{batch = undefined} = State) ->
    State;
complete_batch(#state{batch = #batch{num_writes = 0,
                                     pending = []}} = State) ->
    %% gen_batch_server also batches non-write messages such as query and
    %% async sync completions. Do not turn those empty batches into fdatasync
    %% work; only WAL append batches need durability notification.
    State#state{batch = undefined};
complete_batch(#state{batch = #batch{waiting = Waiting,
                                     num_writes = NumWrites},
                      wal = Wal,
                      conf = Cfg,
                      pending_sync = PendingSync} = State0) ->
    %% PATCHED path: write data synchronously, queue writers for async notification.
    State = flush_pending(State0),
    counters:add(Cfg#conf.counter, ?C_WRITES, NumWrites),

    %% DON'T notify writers yet — queue them for after the next fdatasync.
    %% We capture {Waiting, CurrentRanges} so that notify_all_pending can
    %% properly update the WAL ranges when it processes these.
    %% The WAL end offset lets the async completion release every batch
    %% actually covered by the fdatasync. That keeps the 200us delay useful:
    %% batches that arrive during the delay can be acknowledged by the same
    %% sync, while batches that arrive during fdatasync stay queued.
    NewPending = [{Waiting, Wal#wal.ranges, Wal#wal.file_size} | PendingSync],

    %% Try to start an async sync if not already running.
    maybe_start_async_sync(State#state{pending_sync = NewPending}).

%% PATCHED: start an asynchronous fdatasync if none is in flight.
maybe_start_async_sync(#state{sync_in_flight = true} = State) ->
    %% Already syncing — new entries will be covered by the next sync cycle.
    State;
maybe_start_async_sync(#state{pending_sync = []} = State) ->
    %% Nothing to sync.
    State;
maybe_start_async_sync(#state{conf = #conf{sync_method = none}} = State) ->
    %% sync_method = none: no sync needed, notify immediately.
    notify_all_pending(State);
maybe_start_async_sync(#state{wal = #wal{fd = Fd, filename = Filename},
                               conf = #conf{sync_method = SyncMeth,
                                            wal_io_module = IoMod,
                                            wal_commit_delay_us = MaxDelayUs},
                               sync_next_delay_us = NextDelayUs,
                               pending_sync = PendingSync} = State) ->
    Self = self(),
    StartedAt = erlang:monotonic_time(),
    DelayUs = adaptive_commit_delay_us(MaxDelayUs, length(PendingSync), NextDelayUs),
    SyncTargetFileSize = max_pending_end_file_size(PendingSync),
    case IoMod of
        undefined ->
            spawn_link(fun() ->
                maybe_sleep_us(DelayUs),
                {ok, SyncFd} = file:open(Filename, [raw, write, binary]),
                ok = file:SyncMeth(SyncFd),
                ok = file:close(SyncFd),
                Self ! {wal_async_sync_complete, SyncTargetFileSize}
            end);
        _Mod ->
            Ref = make_ref(),
            ok = wal_sync_async(IoMod, Fd, Self, Ref, DelayUs)
    end,
    State#state{sync_in_flight = true,
                sync_started_at = StartedAt,
                sync_delay_us = DelayUs,
                pending_sync = [],
                syncing_sync = PendingSync}.

wal_sync_async(IoMod, Handle, Pid, Ref, DelayUs) ->
    case erlang:function_exported(IoMod, sync_with_delay, 4) of
        true -> IoMod:sync_with_delay(Handle, Pid, Ref, DelayUs);
        false -> IoMod:sync(Handle, Pid, Ref)
    end.

adaptive_commit_delay_us(MaxDelayUs, PendingBatches)
  when is_integer(MaxDelayUs) ->
    adaptive_commit_delay_us(MaxDelayUs, PendingBatches,
                             min(MaxDelayUs, ?WAL_DELAY_FLOOR_US));
adaptive_commit_delay_us(_MaxDelayUs, _PendingBatches) ->
    0.

adaptive_commit_delay_us(MaxDelayUs, PendingBatches, _NextDelayUs)
  when is_integer(MaxDelayUs), MaxDelayUs =< 0;
       not is_integer(PendingBatches) ->
    0;
adaptive_commit_delay_us(MaxDelayUs, PendingBatches, NextDelayUs)
  when is_integer(MaxDelayUs), MaxDelayUs > 0, PendingBatches =< 1 ->
    min(MaxDelayUs, max(?WAL_DELAY_FLOOR_US, NextDelayUs));
adaptive_commit_delay_us(MaxDelayUs, PendingBatches, NextDelayUs)
  when is_integer(MaxDelayUs), MaxDelayUs > 0, PendingBatches =< 4 ->
    min(MaxDelayUs, max(1000, NextDelayUs));
adaptive_commit_delay_us(MaxDelayUs, PendingBatches, NextDelayUs)
  when is_integer(MaxDelayUs), MaxDelayUs > 0, PendingBatches =< 16 ->
    min(MaxDelayUs, max(3000, NextDelayUs));
adaptive_commit_delay_us(MaxDelayUs, _PendingBatches, _NextDelayUs)
  when is_integer(MaxDelayUs), MaxDelayUs > 0 ->
    MaxDelayUs;
adaptive_commit_delay_us(_MaxDelayUs, _PendingBatches, _NextDelayUs) ->
    0.

'__adaptive_commit_delay_us_for_test__'(MaxDelayUs, PendingBatches) ->
    adaptive_commit_delay_us(MaxDelayUs, PendingBatches).

next_adaptive_commit_delay_us(MaxDelayUs, _CurrentDelayUs, _ReleasedBatches, _QueuedBatches)
  when not is_integer(MaxDelayUs); MaxDelayUs =< 0 ->
    0;
next_adaptive_commit_delay_us(MaxDelayUs, CurrentDelayUs, _ReleasedBatches, QueuedBatches)
  when is_integer(QueuedBatches), QueuedBatches > 0 ->
    %% Queued batches mean writes arrived while fdatasync was already running.
    %% The disk sync itself is giving us enough group-commit opportunity, so
    %% adding extra delay on the next cycle only increases client latency.
    min(MaxDelayUs, min(CurrentDelayUs, ?WAL_DELAY_FLOOR_US));
next_adaptive_commit_delay_us(MaxDelayUs, CurrentDelayUs, _ReleasedBatches, _QueuedBatches) ->
    min(MaxDelayUs, max(?WAL_DELAY_FLOOR_US, CurrentDelayUs div 2)).

'__next_adaptive_commit_delay_us_for_test__'(MaxDelayUs,
                                             CurrentDelayUs,
                                             ReleasedBatches,
                                             QueuedBatches) ->
    next_adaptive_commit_delay_us(MaxDelayUs,
                                  CurrentDelayUs,
                                  ReleasedBatches,
                                  QueuedBatches).

maybe_sleep_us(DelayUs) when is_integer(DelayUs), DelayUs > 0 ->
    timer:sleep(max(1, DelayUs div 1000));
maybe_sleep_us(_DelayUs) ->
    ok.

emit_wal_sync_telemetry(State, Status) ->
    emit_wal_sync_telemetry(State, Status, undefined).

emit_wal_sync_telemetry(#state{sync_started_at = StartedAt,
                               sync_delay_us = DelayUs} = State,
                        Status,
                        SyncedFileSize) ->
    Now = erlang:monotonic_time(),
    DurationUs = case StartedAt of
                     undefined -> 0;
                     _ -> erlang:convert_time_unit(max(Now - StartedAt, 0),
                                                   native,
                                                   microsecond)
                 end,
    {ReleasedBatches, QueuedBatches} = sync_batch_counts(State, SyncedFileSize),
    telemetry:execute([ferricstore, wal, sync],
                      #{duration_us => DurationUs,
                        pending_batches => ReleasedBatches,
                        queued_batches => QueuedBatches,
                        delay_us => DelayUs},
                      #{status => Status}),
    ok.

%% PATCHED: notify writers covered by the just-completed fdatasync.
%% Batches that arrived while the sync was in flight remain in pending_sync
%% and must be covered by a later fdatasync before they are released.
notify_syncing_pending(#state{syncing_sync = []} = State) ->
    State;
notify_syncing_pending(#state{syncing_sync = SyncingPending} = State) ->
    State1 = notify_pending_batches(SyncingPending, State),
    State1#state{syncing_sync = []}.

release_synced_pending(#state{syncing_sync = SyncingSync,
                              pending_sync = PendingSync} = State,
                       SyncedFileSize)
  when is_integer(SyncedFileSize), SyncedFileSize >= 0 ->
    {Covered, Deferred} = split_synced_pending(PendingSync ++ SyncingSync,
                                               SyncedFileSize),
    State1 = notify_pending_batches(Covered, State),
    State1#state{syncing_sync = [],
                 pending_sync = Deferred};
release_synced_pending(State, _SyncedFileSize) ->
    notify_syncing_pending(State).

complete_async_sync(#state{conf = #conf{wal_commit_delay_us = MaxDelayUs},
                           sync_delay_us = CurrentDelayUs} = State0,
                    SyncedFileSize) ->
    {ReleasedBatches, QueuedBatches} = sync_batch_counts(State0, SyncedFileSize),
    NextDelayUs = next_adaptive_commit_delay_us(MaxDelayUs,
                                                CurrentDelayUs,
                                                ReleasedBatches,
                                                QueuedBatches),
    State1 = State0#state{sync_in_flight = false,
                          sync_started_at = undefined,
                          sync_delay_us = 0,
                          sync_next_delay_us = NextDelayUs},
    case SyncedFileSize of
        Pos when is_integer(Pos), Pos >= 0 ->
            release_synced_pending(State1, Pos);
        _ ->
            notify_syncing_pending(State1)
    end.

%% PATCHED: notify all writers queued in pending_sync. Used only for
%% sync_method=none and synchronous drain paths that explicitly synced after
%% all pending writes were already flushed to the WAL I/O layer.
%% Called after fdatasync completes (or immediately for sync_method=none).
notify_all_pending(#state{pending_sync = []} = State) ->
    State;
notify_all_pending(#state{pending_sync = PendingBatches} = State) ->
    State1 = notify_pending_batches(PendingBatches, State),
    State1#state{pending_sync = []}.

notify_pending_batches([], State) ->
    State;
notify_pending_batches(PendingBatches, #state{wal = Wal} = State) ->
    %% Process all queued batches in order (oldest first — reverse the list
    %% since we prepend newest).
    FinalRanges = lists:foldl(
        fun(PendingBatch, AccRanges) ->
            Waiting = pending_waiting(PendingBatch),
            maps:fold(fun(Pid, BatchWriter, Acc) ->
                complete_batch_writer(Pid, BatchWriter, Acc)
            end, AccRanges, Waiting)
        end, Wal#wal.ranges, lists:reverse(PendingBatches)),

    State#state{wal = Wal#wal{ranges = FinalRanges}}.

split_synced_pending(PendingBatches, SyncedFileSize) ->
    lists:partition(
      fun(PendingBatch) ->
              pending_end_file_size(PendingBatch) =< SyncedFileSize
      end, PendingBatches).

sync_batch_counts(#state{syncing_sync = SyncingSync,
                         pending_sync = PendingSync}, SyncedFileSize)
  when is_integer(SyncedFileSize), SyncedFileSize >= 0 ->
    {Covered, Deferred} = split_synced_pending(PendingSync ++ SyncingSync,
                                               SyncedFileSize),
    {length(Covered), length(Deferred)};
sync_batch_counts(#state{syncing_sync = SyncingSync,
                         pending_sync = PendingSync}, _SyncedFileSize) ->
    {length(SyncingSync), length(PendingSync)}.

pending_waiting({Waiting, _BatchRanges, _EndFileSize}) ->
    Waiting;
pending_waiting({Waiting, _BatchRanges}) ->
    Waiting.

pending_end_file_size({_Waiting, _BatchRanges, EndFileSize}) ->
    EndFileSize;
pending_end_file_size({_Waiting, _BatchRanges}) ->
    0.

max_pending_end_file_size([]) ->
    0;
max_pending_end_file_size(PendingBatches) ->
    lists:max([pending_end_file_size(PendingBatch) || PendingBatch <- PendingBatches]).

'__complete_sync_partition_for_test__'(SyncingCount, PendingCount)
  when is_integer(SyncingCount), SyncingCount >= 0,
       is_integer(PendingCount), PendingCount >= 0 ->
    State0 = #state{wal = #wal{ranges = #{}},
                    syncing_sync = make_empty_pending_batches(SyncingCount),
                    pending_sync = make_empty_pending_batches(PendingCount)},
    State = notify_syncing_pending(State0),
    {SyncingCount - length(State#state.syncing_sync),
     length(State#state.pending_sync)}.

'__complete_sync_frontier_for_test__'(SyncingEnds, PendingEnds, SyncedFileSize)
  when is_list(SyncingEnds), is_list(PendingEnds),
       is_integer(SyncedFileSize), SyncedFileSize >= 0 ->
    State0 = #state{wal = #wal{ranges = #{}},
                    syncing_sync = make_pending_batches_with_ends(SyncingEnds),
                    pending_sync = make_pending_batches_with_ends(PendingEnds)},
    {Covered, Deferred} = split_synced_pending(State0#state.pending_sync ++
                                               State0#state.syncing_sync,
                                               SyncedFileSize),
    {length(Covered), [pending_end_file_size(PendingBatch) ||
                       PendingBatch <- Deferred]}.

make_empty_pending_batches(Count) ->
    lists:duplicate(Count, {#{}, #{}, 0}).

make_pending_batches_with_ends(Ends) ->
    [{#{}, #{}, End} || End <- Ends].

complete_batch_writer(Pid, #batch_writer{smallest_live_idx = SmallestIdx,
                                         tid = MtTid,
                                         uid = UId,
                                         seq = Seq0,
                                         term = Term,
                                         old = undefined}, Ranges) ->
    Seq = ra_seq:floor(SmallestIdx, Seq0),
    Pid ! {ra_log_event, {written, Term, Seq}},
    update_ranges(Ranges, UId, MtTid, SmallestIdx, Seq);
complete_batch_writer(Pid, #batch_writer{old = #batch_writer{} = OldBw} = Bw,
                      Ranges0) ->
    Ranges = complete_batch_writer(Pid, OldBw, Ranges0),
    complete_batch_writer(Pid, Bw#batch_writer{old = undefined}, Ranges).

wal2list(File) ->
    Data = open_existing(File),
    dump_records(Data, []).

open_existing(File) ->
    case file:read_file(File) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned, Data/binary>>} ->
            %% the only version currently supported
            Data;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned, _/binary>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion});
        %% PATCHED: empty or truncated file — no records to recover
        {ok, <<>>} ->
            <<>>;
        {ok, _PartialHeader} ->
            <<>>
    end.

open_at_first_record(File) ->
    {ok, Fd} = file:open(File, [read, binary, raw]),
    case file:read(Fd, 5) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned>>} ->
            %% the only version currently supported
            Fd;
        {ok, <<0, 0, 0, 0, _/binary>>} ->
            %% Pre-allocated file filled with zeros. Nothing to recover.
            Fd;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion});
        eof ->
            Fd;
        {ok, _PartialHeader} ->
            Fd
    end.

close_existing(Fd) ->
    case file:close(Fd) of
        ok ->
            ok;
        {error, Reason} ->
            exit({could_not_close, Reason})
    end.

dump_records(<<_:1/unsigned, 0:1/unsigned, _:22/unsigned,
               IdDataLen:16/unsigned, _:IdDataLen/binary,
               _:32/integer,
               0:32/unsigned,
               _Idx:64/unsigned, _Term:64/unsigned,
               _EntryData:0/binary,
               _Rest/binary>>, Entries) ->
    Entries;
dump_records(<<_:1/unsigned, 0:1/unsigned, _Id2:22/unsigned,
               IdDataLen:16/unsigned, _Id:IdDataLen/binary,
               Crc:32/integer,
               EntryDataLen:32/unsigned,
               Idx:64/unsigned, Term:64/unsigned,
               EntryData:EntryDataLen/binary,
               Rest/binary>>, Entries) ->
    % TODO: recover writers info, i.e. last index seen
    case erlang:adler32(<<Idx:64/unsigned, Term:64/unsigned, EntryData/binary>>) of
        Crc ->
            dump_records(Rest, [{Idx, Term, binary_to_term(EntryData)} | Entries]);
        _ ->
            exit({crc_failed_for, Idx, EntryData})
    end;
dump_records(<<_:1/unsigned, 1:1/unsigned, _Id:22/unsigned,
               Crc:32/integer,
               EntryDataLen:32/unsigned,
               Idx:64/unsigned, Term:64/unsigned,
               EntryData:EntryDataLen/binary,
               Rest/binary>>, Entries) ->
    case erlang:adler32(<<Idx:64/unsigned, Term:64/unsigned, EntryData/binary>>) of
        Crc ->
            dump_records(Rest, [{Idx, Term, binary_to_term(EntryData)} | Entries]);
        _ ->
            exit({crc_failed_for, Idx, EntryData})
    end;
dump_records(<<>>, Entries) ->
    Entries.

recover_wal_chunks(#conf{} = Conf, Fd, Writers, Tables, Mode) ->
    Chunk = read_wal_chunk(Fd, Conf#conf.recovery_chunk_size),
    recover_records(Conf, Fd, Chunk, #{}, #recovery{mode = Mode,
                                                    writers = Writers,
                                                    tables = Tables}).
% All zeros indicates end of a pre-allocated wal file
recover_records(_, _Fd, <<0:1/unsigned, 0:1/unsigned, 0:22/unsigned,
                          IdDataLen:16/unsigned, _:IdDataLen/binary,
                          0:32/integer, 0:32/unsigned, _/binary>>,
                _Cache, State) ->
    State;
% First encounter of UId in this file
recover_records(#conf{names = Names} = Conf, Fd,
                <<Trunc:1/unsigned, 0:1/unsigned, IdRef:22/unsigned,
                  IdDataLen:16/unsigned, UId:IdDataLen/binary,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>> = Chunk,
                Cache0, State0) ->
    case ra_directory:is_registered_uid(Names, UId) of
        true ->
            Cache = Cache0#{IdRef => {UId, <<1:1/unsigned, IdRef:22/unsigned>>}},
            SmallestIdx = recover_smallest_idx(Conf, UId, Trunc == 1, Idx),
            case validate_checksum(Checksum, Idx, Term, EntryData) of
                ok when Idx >= SmallestIdx ->
                    State1 = handle_trunc(Conf, Trunc == 1, UId, Idx, State0),
                    case recover_entry(Names, UId,
                                       {Idx, Term, binary_to_term(EntryData)},
                                       SmallestIdx, State1) of
                        {ok, State} ->
                            recover_records(Conf, Fd, Rest, Cache, State);
                        {retry, State} ->
                            recover_records(Conf, Fd, Chunk, Cache, State)
                    end;
                ok ->
                    Writers = State0#recovery.writers,
                    recover_records(Conf, Fd, Rest, Cache,
                                    State0#recovery{writers =
                                                        maps:remove(UId, Writers)});
                error ->
                    System = Conf#conf.system,
                    ?DEBUG("WAL in ~ts: record failed CRC check. If this is the last record"
                           " recovery can resume", [System]),
                    %% if this is the last entry in the wal we can just drop the
                    %% record;
                    ok = is_last_record(Fd, Rest, Conf),
                    State0
            end;
        false ->
            recover_records(Conf, Fd, Rest, Cache0, State0)
    end;
recover_records(#conf{names = Names} = Conf, Fd,
                <<Trunc:1/unsigned, 1:1/unsigned, IdRef:22/unsigned,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>> = Chunk,
                Cache, State0) ->
    case Cache of
        #{IdRef := {UId, _}} ->
            SmallestIdx = recover_smallest_idx(Conf, UId, Trunc == 1, Idx),
            case validate_checksum(Checksum, Idx, Term, EntryData) of
                ok when Idx >= SmallestIdx ->
                    State1 = handle_trunc(Conf, Trunc == 1, UId, Idx, State0),
                    case recover_entry(Names, UId,
                                       {Idx, Term, binary_to_term(EntryData)},
                                       SmallestIdx, State1) of
                        {ok, State} ->
                            recover_records(Conf, Fd, Rest, Cache, State);
                        {retry, State} ->
                            recover_records(Conf, Fd, Chunk, Cache, State)
                    end;
                ok ->
                    recover_records(Conf, Fd, Rest, Cache, State0);
                error ->
                    System = Conf#conf.system,
                    ?DEBUG("WAL in ~ts: record failed CRC check. If this is the last record"
                           " recovery can resume", [System]),
                    %% if this is the last entry in the wal we can just drop the
                    %% record;
                    ok = is_last_record(Fd, Rest, Conf),
                    State0
            end;
        _ ->
            %% if the IdRef is not in the cache this refers to a deleted
            %% UId and we can just move on
            recover_records(Conf, Fd, Rest, Cache, State0)
    end;
recover_records(Conf, Fd, Chunk, Cache, State) ->
    % Not enough remainder to parse a whole record, need to read
    NextChunk = read_wal_chunk(Fd, Conf#conf.recovery_chunk_size),
    case NextChunk of
        <<>> ->
            %% we have reached the end of the file
            State;
        _ ->
            %% append this chunk to the remainder of the last chunk
            Chunk0 = <<Chunk/binary, NextChunk/binary>>,
            recover_records(Conf, Fd, Chunk0, Cache, State)
    end.

recover_smallest_idx(Conf, UId, Trunc, CurIdx) ->
    case Trunc of
        true ->
            max(CurIdx, smallest_live_index(Conf, UId));
        false ->
            smallest_live_index(Conf, UId)
    end.

is_last_record(_Fd, <<0:104, _/binary>>, _) ->
    ok;
is_last_record(Fd, Rest, Conf) ->
    case byte_size(Rest) < 13 of
        true ->
            case read_wal_chunk(Fd, 256) of
                <<>> ->
                    ok;
                Next ->
                    is_last_record(Fd, <<Rest/binary, Next/binary>>, Conf)
            end;
        false ->
            ?ERROR("WAL in ~ts: record failed CRC check during recovery. "
                   "Unable to recover WAL data safely", [Conf#conf.system]),
            throw(wal_checksum_validation_failure)

    end.

read_wal_chunk(Fd, Len) ->
    case file:read(Fd, Len) of
        {ok, <<Data/binary>>} ->
            Data;
        eof ->
            <<>>;
        {error, Reason} ->
            exit({could_not_read_wal_chunk, Reason})
    end.

validate_checksum(0, _, _, _) ->
    % checksum not used
    ok;
validate_checksum(Checksum, Idx, Term, Data) ->
    % building a binary just for the checksum may feel a bit wasteful
    % but this is only called during recovery which should be a rare event
    case erlang:adler32(<<Idx:64/unsigned, Term:64/unsigned, Data/binary>>) of
        Checksum ->
            ok;
        _ ->
            error
    end.

merge_conf_defaults(Conf) ->
    maps:merge(#{segment_writer => ra_log_segment_writer,
                 max_size_bytes => ?WAL_DEFAULT_MAX_SIZE_BYTES,
                 max_entries => undefined,
                 recovery_chunk_size => ?WAL_RECOVERY_CHUNK_SIZE,
                 compute_checksums => true,
                 pre_allocate => false,
                 garbage_collect => false,
                 sync_method => datasync,
                 min_bin_vheap_size => ?MIN_BIN_VHEAP_SIZE,
                 min_heap_size => ?MIN_HEAP_SIZE}, Conf).

to_binary(Term) ->
    term_to_iovec(Term).

should_roll_wal(#state{conf = #conf{max_entries = MaxEntries},
                       wal = #wal{max_size = MaxWalSize,
                                  file_size = FileSize,
                                  entry_count = Count}}) ->
    %% Initially, MaxWalSize was a hard limit for the file size: if FileSize +
    %% DataSize went over that limit, we would use a new file. This was an
    %% issue when DataSize was larger than the limit alone.
    %%
    %% The chosen solution is to only consider the current file size in the
    %% calculation. It means that after DataSize bytes are written, the file
    %% will be larger than the configured maximum size. But at least it will
    %% accept Ra commands larger than the max WAL size.
    FileSize > MaxWalSize orelse case MaxEntries of
                                     undefined -> false;
                                     _ ->
                                         Count + 1 > MaxEntries
                                 end.

smallest_live_index(#conf{ra_log_snapshot_state_tid = Tid}, ServerUId) ->
    ra_log_snapshot_state:smallest(Tid, ServerUId).

update_ranges(Ranges, UId, MtTid = MT_TID, _SmallestIdx, AddSeq) ->
    case Ranges of
        #{UId := [{MT_TID, Seq0} | Seqs]} ->
            %% limit the old range by the add end start as in some resend
            %% cases we may have got back before the prior range.
            Seq = ra_seq:add(AddSeq, Seq0),
            Ranges#{UId => [{MtTid, Seq} | Seqs]};
        #{UId := Seqs} ->
            %% new Tid, need to add a new range record for this
            Ranges#{UId => [{MtTid, AddSeq} | Seqs]};
        _ ->
            Ranges#{UId => [{MtTid, AddSeq}]}
    end.

recover_entry(Names, UId, {Idx, _, _} = Entry, SmallestIdx,
              #recovery{mode = initial,
                        ranges = Ranges0,
                        writers = Writers,
                        tables = Tables} = State) ->
    Mt0 = case Tables of
              #{UId := M} -> M;
              _ ->
                  {ok, M} = ra_log_ets:mem_table_please(Names, UId),
                  M
          end,
    %% always use write_sparse as there is nothing to indicate in the wal
    %% data if an entry was written as such. this way we recover all writes
    %% so should be ok for all types of writes
    PrevIdx = case Writers of
                  #{UId := {in_seq, I}} ->
                      I;
                  _ ->
                      undefined
              end,
    case ra_mt:insert_sparse(Entry, PrevIdx, Mt0) of
        {ok, Mt1} ->
            Ranges = update_ranges(Ranges0, UId, ra_mt:tid(Mt1),
                                   SmallestIdx, [Idx]),
            {ok, State#recovery{ranges = Ranges,
                                writers = Writers#{UId => {in_seq, Idx}},
                                tables = Tables#{UId => Mt1}}};
        {error, overwriting} ->
            %% create successor memtable
            {ok, Mt1} = ra_log_ets:new_mem_table_please(Names, UId, Mt0),
            {retry, State#recovery{tables = Tables#{UId => Mt1},
                                   writers = maps:remove(UId, Writers)}}
    end;
recover_entry(Names, UId, {Idx, Term, _}, SmallestIdx,
              #recovery{mode = post_boot,
                        ranges = Ranges0,
                        writers = Writers,
                        tables = Tables} = State) ->
    Mt0 = case Tables of
              #{UId := M} -> M;
              _ ->
                  {ok, M} = ra_log_ets:mem_table_please(Names, UId),
                  M
          end,
    %% find the tid for the given idxterm
    case ra_mt:tid_for(Idx, Term, Mt0) of
        undefined ->
            %% not found, this entry may already have been flushed
            %% skip, and reset ranges but update writers as we need to
            %% recover the last idx
            Ranges = maps:remove(UId, Ranges0),
            {ok, State#recovery{ranges = Ranges,
                                writers = Writers#{UId => {in_seq, Idx}},
                                tables = Tables#{UId => Mt0}}};
        Tid ->
            Ranges = update_ranges(Ranges0, UId, Tid,
                                   SmallestIdx, [Idx]),
            {ok, State#recovery{ranges = Ranges,
                                writers = Writers#{UId => {in_seq, Idx}},
                                tables = Tables#{UId => Mt0}}}
    end.

handle_trunc(_, false, _UId, _Idx, State) ->
    State;
handle_trunc(#conf{names = Names}, true, UId, Idx,
             #recovery{mode = Mode,
                       ranges = Ranges0,
                       writers = Writers,
                       tables = Tbls} = State) ->
    case Tbls of
        #{UId := Mt0} when Mode == initial ->
            %% only meddle with mem table data in initial mode
            {Specs, Mt} = ra_mt:set_first(Idx-1, Mt0),
            [begin
                 ok = ra_log_ets:execute_delete(Names, UId, Spec)
             end|| Spec <- Specs],
            Ranges = case Ranges0 of
                         #{UId := Seqs0} ->
                             Seqs = [{T, ra_seq:floor(Idx, Seq)}
                                     || {T, Seq} <- Seqs0],
                             Ranges0#{UId => Seqs};
                         _ ->
                             Ranges0
                     end,

            State#recovery{tables = Tbls#{UId => Mt},
                           writers = maps:remove(UId, Writers),
                           ranges = Ranges};
        _ ->
            State#recovery{writers = maps:remove(UId, Writers)}
    end.

named_cast(To, Msg) when is_pid(To) ->
    gen_batch_server:cast(To, Msg),
    {ok, To};
named_cast(Wal, Msg) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            named_cast(Pid, Msg)
    end.
