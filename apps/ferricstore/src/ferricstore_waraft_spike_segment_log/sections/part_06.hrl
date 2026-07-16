%% Included by ferricstore_waraft_spike_segment_log.erl; generated split section 6.

decode_segment_config(Binary) ->
    case decode_external_term_exact(Binary) of
        {ok, #{version := 1, records_per_segment := Value}}
          when is_integer(Value), Value > 0 ->
            {ok, Value};
        {ok, Other} ->
            {error, {bad_segment_config, Other}};
        {error, Reason} ->
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
    case write_file_sync(TmpPath, encode_external_term(Config)) of
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
    BinaryPath = unicode:characters_to_binary(Path),
    try 'Elixir.Ferricstore.Bitcask.NIF':fs_atomic_replace_nofollow(
        BinaryPath,
        Binary,
        ?MAX_SEGMENT_METADATA_BYTES
    ) of
        ok -> ok;
        {error, Reason} -> {error, {atomic_write, Path, Reason}};
        Other -> {error, {atomic_write, Path, Other}}
    catch
        Class:Reason -> {error, {atomic_write_exception, Path, Class, Reason}}
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

decode_segment_record(_Path, Payload) ->
    case decode_external_term_exact(Payload) of
        {ok, Decoded} ->
            {ok, Decoded};
        {error, Reason} ->
            {error, {bad_term, Reason}}
    end.

read_segment_file_nofollow(Path, MaxBytes) ->
    BinaryPath = unicode:characters_to_binary(Path),
    try 'Elixir.Ferricstore.Bitcask.NIF':fs_read_nofollow(BinaryPath, MaxBytes) of
        {ok, Binary} when is_binary(Binary) ->
            {ok, Binary};
        {error, {enoent, _Detail}} ->
            {error, enoent};
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {unexpected_secure_read_result, Other}}
    catch
        Class:Reason ->
            {error, {secure_read_exception, Class, Reason}}
    end.

encode_external_term(Term) ->
    term_to_binary(Term, [deterministic]).

decode_external_term_exact(<<131, 80, _/binary>>) ->
    {error, compressed_external_term};
decode_external_term_exact(Binary) when is_binary(Binary) ->
    try binary_to_term(Binary, [safe, used]) of
        {Term, Used} when Used =:= byte_size(Binary) ->
            {ok, Term};
        {_Term, _Used} ->
            {error, trailing_external_term}
    catch
        _:Reason ->
            {error, Reason}
    end;
decode_external_term_exact(_Other) ->
    {error, invalid_external_term}.

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
