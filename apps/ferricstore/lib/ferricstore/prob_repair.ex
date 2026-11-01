defmodule Ferricstore.ProbRepair do
  @moduledoc """
  Gives a node back a probabilistic key's file (Bloom, Cuckoo, Count-Min,
  Top-K) that it lost, by copying it from a replica that still has it.

  A lost file leaves the key damaged on that node only: its commands answer
  `ERR ... file is missing on this node` there while the other replicas keep
  applying them. When the node notices (at startup, or when a command finds
  the file missing), this process proposes `{:prob_file_repair, key, type,
  id}` through the shard's log. Every replica applies it at the same index:

    * a replica that has the file copies it, with its mutation receipt, into
      `<data_dir>/prob_repair/shard_<n>/` (`stage/4`). Apply runs in log order,
      so the copy is the file's state just before that entry, the same on
      every replica that has one;
    * the node that lost it waits in that apply for a copy, fetches it in
      1 MiB chunks from a replica (`fetch_and_install/6`), checks its SHA-256
      and installs the receipt, then the file. Its next entries apply to the
      copy as usual, so no write is missed or applied twice: the file's own
      mutation token makes a replayed write a no-op.

  The entry changes no replicated state, only node-local files, and it is
  applied from cluster feature level 12, which every member then supports.
  If no replica has the file, or none answers within the wait, the key stays
  damaged (DEL still heals it) and the repair is retried a few times, then
  again after a cooldown.
  """

  use GenServer

  require Logger

  alias Ferricstore.{FS, ProbFile}
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Store.{CompoundKey, Ops, Router}

  @types %{"bloom" => :bloom, "cuckoo" => :cuckoo, "cms" => :cms, "topk" => :topk}
  @level 12
  @chunk_bytes 1_048_576
  @stage_wait_ms 5_000
  @transfer_budget_ms 60_000
  @call_timeout_ms 5_000
  @poll_ms 25
  @local_wait_ms 15_000
  @retry_ms [5_000, 30_000, 120_000]
  @cooldown_ms 600_000
  @staging_ttl_ms 300_000
  @sweep_ms 60_000
  @startup_scan_ms 5_000
  @marker_bytes 4_096
  @max_running 4

  @type type :: :bloom | :cuckoo | :cms | :topk

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Asks this node to repair `key`'s file of `type` on `shard_index`."
  @spec request(non_neg_integer(), binary(), type()) :: :ok
  def request(shard_index, key, type)
      when is_integer(shard_index) and is_binary(key) and type in [:bloom, :cuckoo, :cms, :topk],
      do: GenServer.cast(__MODULE__, {:request, shard_index, key, type})

  def request(_shard_index, _key, _type), do: :ok

  @doc "As `request/3`, for a command that found the file missing through `store`."
  @spec request_for_store(term(), binary(), type()) :: :ok
  def request_for_store(%FerricStore.Instance{name: :default} = ctx, key, type),
    do: request(Router.shard_for(ctx, key), key, type)

  def request_for_store(_store, _key, _type), do: :ok

  @doc false
  @spec staging_dir(binary(), non_neg_integer()) :: binary()
  def staging_dir(data_dir, shard_index),
    do: Path.join([data_dir, "prob_repair", "shard_#{shard_index}"])

  @doc false
  @spec valid_request?(term(), term(), term()) :: boolean()
  def valid_request?(key, type, id),
    do:
      is_binary(key) and type in [:bloom, :cuckoo, :cms, :topk] and is_binary(id) and
        String.match?(id, ~r/\A[0-9a-f]{24}\z/)

  @doc """
  The live probabilistic keys of `shard_index` and those whose file is
  missing on this node, as `{key, type}`.
  """
  @spec missing_files(FerricStore.Instance.t(), non_neg_integer()) ::
          {non_neg_integer(), [{binary(), type()}]}
  def missing_files(ctx, shard_index) do
    keydir = elem(ctx.keydir_refs, shard_index)
    prob_dir = Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index), "prob")
    now = System.os_time(:millisecond)

    :ets.foldl(
      fn
        {<<"T:", _rest::binary>> = storage_key, value, expire_at_ms, _lfu, _fid, _off, _size},
        {count, missing} = acc
        when is_binary(value) and is_integer(expire_at_ms) ->
          with {:ok, type} <- Map.fetch(@types, CompoundKey.type_name(value)),
               true <- expire_at_ms == 0 or expire_at_ms > now do
            key = CompoundKey.extract_redis_key(storage_key)

            if FS.exists?(ProbFile.path(prob_dir, key, Atom.to_string(type))),
              do: {count + 1, missing},
              else: {count + 1, [{key, type} | missing]}
          else
            _not_live_prob -> acc
          end

        _row, acc ->
          acc
      end,
      {0, []},
      keydir
    )
  end

  # --- Apply side: these run inside the state machine's apply ---

  @doc false
  # A replica that has the file copies it and its receipt aside for repair
  # `id`, then publishes a marker holding their sizes and SHA-256s.
  @spec stage(binary(), non_neg_integer(), binary(), binary()) :: :ok
  def stage(data_dir, shard_index, id, path) do
    dir = staging_dir(data_dir, shard_index)
    receipt = path <> ".mutation"

    with :ok <- FS.mkdir_p(dir),
         {:ok, data} <- copy_aside(path, Path.join(dir, id <> ".data")),
         {:ok, receipt_info} <- maybe_copy_receipt(receipt, Path.join(dir, id <> ".receipt")),
         :ok <- write_marker(Path.join(dir, id <> ".ready"), %{data: data, receipt: receipt_info}) do
      :ok
    else
      {:error, reason} ->
        Logger.warning(
          "Prob repair: could not copy #{path} aside for repair #{id}: #{inspect(reason)}"
        )

        stage_none(data_dir, shard_index, id)
    end
  rescue
    error -> apply_side_failure("copy #{path} aside", error)
  catch
    kind, reason -> apply_side_failure("copy #{path} aside", {kind, reason})
  end

  @doc false
  # A replica that has nothing to give for repair `id` says so, so the node
  # that lost the file stops waiting for it.
  @spec stage_none(binary(), non_neg_integer(), binary()) :: :ok
  def stage_none(data_dir, shard_index, id) do
    dir = staging_dir(data_dir, shard_index)

    with :ok <- FS.mkdir_p(dir) do
      _ = write_marker(Path.join(dir, id <> ".none"), :none)
    end

    :ok
  rescue
    error -> apply_side_failure("mark repair #{id}", error)
  catch
    kind, reason -> apply_side_failure("mark repair #{id}", {kind, reason})
  end

  @doc false
  # The node that lost `path` fetches repair `id`'s copy from a replica and
  # installs it. `index` is the repair entry's log index: a replica that has
  # applied past it without a copy has none to give.
  @spec fetch_and_install(
          binary(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          binary(),
          binary()
        ) ::
          :ok | {:error, term()}
  def fetch_and_install(data_dir, shard_index, id, index, path, extension) do
    dir = staging_dir(data_dir, shard_index)
    peers = peers(shard_index)
    # This node has nothing to give either: a peer that lost the file too
    # stops waiting for it instead of waiting out the deadline.
    :ok = stage_none(data_dir, shard_index, id)

    result =
      with [_ | _] <- peers,
           {:ok, peer, info} <-
             await_copy(peers, shard_index, id, index, deadline(@stage_wait_ms)),
           :ok <- FS.mkdir_p(dir),
           :ok <- download(peer, shard_index, id, info, dir),
           :ok <- install(dir, id, info, path, extension) do
        Logger.warning("Prob repair: restored #{path} from #{peer}")
        :ok
      else
        [] -> {:error, :no_peers}
        {:error, _reason} = error -> error
      end

    # Copies and markers stay until the sweep removes them: another node that
    # lost the file may still be fetching from the same repair.
    for suffix <- [".incoming", ".incoming-receipt"], do: FS.rm(Path.join(dir, id <> suffix))

    case result do
      :ok ->
        :ok

      {:error, reason} = error ->
        Logger.warning("Prob repair: #{path} was not restored (repair #{id}): #{inspect(reason)}")
        error
    end
  rescue
    error ->
      apply_side_failure("restore #{path}", error)
      {:error, :crashed}
  catch
    kind, reason ->
      apply_side_failure("restore #{path}", {kind, reason})
      {:error, :crashed}
  end

  # These functions run inside the shard's apply, where an exception would
  # count as a storage failure; a repair that fails leaves the key damaged.
  defp apply_side_failure(what, reason) do
    Logger.error("Prob repair: could not #{what}: #{inspect(reason)}")
    :ok
  end

  # --- Peer side: called over :erpc by the node being repaired ---

  @doc false
  @spec staged_info(non_neg_integer(), binary(), non_neg_integer()) ::
          {:ready, map()} | :none | :gone | :pending
  def staged_info(shard_index, id, index) do
    dir = staging_dir(local_data_dir(), shard_index)

    cond do
      not valid_id?(id) ->
        :none

      FS.exists?(Path.join(dir, id <> ".ready")) ->
        case read_marker(Path.join(dir, id <> ".ready")) do
          {:ok, %{} = info} -> {:ready, info}
          _invalid -> :none
        end

      FS.exists?(Path.join(dir, id <> ".none")) ->
        :none

      applied_past?(shard_index, index) ->
        :gone

      true ->
        :pending
    end
  end

  @doc false
  @spec staged_chunk(
          non_neg_integer(),
          binary(),
          :data | :receipt,
          non_neg_integer(),
          pos_integer()
        ) ::
          {:ok, binary()} | {:error, term()}
  def staged_chunk(shard_index, id, part, offset, length)
      when part in [:data, :receipt] and is_integer(offset) and offset >= 0 and
             is_integer(length) and length > 0 and length <= @chunk_bytes do
    if valid_id?(id) do
      path = Path.join(staging_dir(local_data_dir(), shard_index), "#{id}.#{part}")

      with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
        try do
          case :file.pread(fd, offset, length) do
            {:ok, bytes} -> {:ok, bytes}
            :eof -> {:ok, <<>>}
            {:error, _reason} = error -> error
          end
        after
          :file.close(fd)
        end
      end
    else
      {:error, :invalid_repair_id}
    end
  end

  # --- The repair process ---

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)

    if Keyword.get(opts, :name, __MODULE__) == __MODULE__,
      do: :persistent_term.put({__MODULE__, :data_dir}, ctx.data_dir)

    send(self(), :sweep)
    Process.send_after(self(), :scan, Keyword.get(opts, :startup_scan_ms, @startup_scan_ms))
    {:ok, %{ctx: ctx, opts: opts, jobs: %{}, queue: :queue.new(), cooldown: %{}}}
  end

  @impl true
  def handle_cast({:request, shard_index, key, type}, state) do
    job = {shard_index, key}

    cond do
      Map.has_key?(state.jobs, job) ->
        {:noreply, state}

      cooling?(state, job) ->
        {:noreply, state}

      true ->
        state = %{state | jobs: Map.put(state.jobs, job, {:queued, type, 0})}
        {:noreply, start_queued(%{state | queue: :queue.in(job, state.queue)})}
    end
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case job_for_ref(state, ref) do
      {job, %{type: type, attempt: attempt}} ->
        {:noreply, finish_job(state, job, type, attempt, result)}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case job_for_ref(state, ref) do
      {job, %{type: type, attempt: attempt}} ->
        {:noreply, finish_job(state, job, type, attempt, {:not_repaired, {:crashed, reason}})}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:retry, job, type, attempt}, state) do
    case Map.get(state.jobs, job) do
      :waiting ->
        state = %{state | jobs: Map.put(state.jobs, job, {:queued, type, attempt})}
        {:noreply, start_queued(%{state | queue: :queue.in(job, state.queue)})}

      _running_or_done ->
        {:noreply, state}
    end
  end

  def handle_info(:scan, state) do
    for shard_index <- 0..(state.ctx.shard_count - 1)//1,
        {key, type} <- scan_shard(state.ctx, shard_index),
        do: request(shard_index, key, type)

    {:noreply, state}
  end

  def handle_info(:sweep, state) do
    sweep(Path.join(state.ctx.data_dir, "prob_repair"), state.opts)
    Process.send_after(self(), :sweep, @sweep_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp job_for_ref(state, ref) do
    Enum.find(state.jobs, fn
      {_job, %{ref: ^ref}} -> true
      _other -> false
    end)
  end

  # Runs up to @max_running repairs at a time, in request order: a node that
  # lost many files proposes their repairs a few at a time.
  defp start_queued(state) do
    running = Enum.count(state.jobs, fn {_job, entry} -> is_map(entry) end)

    with true <- running < @max_running,
         {{:value, job}, queue} <- :queue.out(state.queue) do
      state = %{state | queue: queue}

      case Map.get(state.jobs, job) do
        {:queued, type, attempt} -> start_queued(start_job(state, job, type, attempt))
        _other -> start_queued(state)
      end
    else
      _full_or_empty -> state
    end
  end

  defp start_job(state, {shard_index, key} = job, type, attempt) do
    ctx = state.ctx
    task = Task.async(fn -> run(ctx, shard_index, key, type) end)
    %{state | jobs: Map.put(state.jobs, job, %{ref: task.ref, type: type, attempt: attempt})}
  end

  defp finish_job(state, job, type, attempt, result) do
    state
    |> settle_job(job, type, attempt, result)
    |> start_queued()
  end

  defp settle_job(state, {shard_index, key} = job, type, attempt, result) do
    case result do
      :repaired ->
        %{state | jobs: Map.delete(state.jobs, job)}

      {:not_repaired, :not_needed} ->
        %{state | jobs: Map.delete(state.jobs, job)}

      # No replica to copy from (a single node), or a cluster below feature
      # level 12: retrying cannot help, so wait out the cooldown quietly.
      {:not_repaired, reason} when reason in [{:ok, :none}, :inactive] ->
        until = System.monotonic_time(:millisecond) + @cooldown_ms

        %{
          state
          | jobs: Map.delete(state.jobs, job),
            cooldown: Map.put(state.cooldown, job, until)
        }

      {:not_repaired, reason} ->
        case Enum.at(@retry_ms, attempt) do
          nil ->
            Logger.warning(
              "Prob repair: gave up on #{inspect(key)} (shard #{shard_index}) for now: #{inspect(reason)}"
            )

            until = System.monotonic_time(:millisecond) + @cooldown_ms

            %{
              state
              | jobs: Map.delete(state.jobs, job),
                cooldown: Map.put(state.cooldown, job, until)
            }

          delay ->
            Process.send_after(self(), {:retry, job, type, attempt + 1}, delay)
            %{state | jobs: Map.put(state.jobs, job, :waiting)}
        end
    end
  end

  defp cooling?(state, job) do
    case Map.get(state.cooldown, job) do
      until when is_integer(until) -> until > System.monotonic_time(:millisecond)
      nil -> false
    end
  end

  # Proposes the repair entry and waits for this node's copy of the file.
  defp run(ctx, shard_index, key, type) do
    path = ProbFile.path(Ops.prob_dir(ctx, key), key, Atom.to_string(type))

    cond do
      FS.exists?(path) ->
        {:not_repaired, :not_needed}

      (peers = peer_status(shard_index)) != {:ok, :some} ->
        {:not_repaired, peers}

      (level = level_status(ctx, shard_index)) != :active ->
        {:not_repaired, level}

      true ->
        id = Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
        _ = Router.prob_file_repair(ctx, key, type, id)

        if wait_until(fn -> FS.exists?(path) end, deadline(@local_wait_ms)),
          do: :repaired,
          else: {:not_repaired, :no_copy}
    end
  catch
    kind, reason -> {:not_repaired, {kind, reason}}
  end

  defp scan_shard(ctx, shard_index) do
    {_count, missing} = missing_files(ctx, shard_index)
    missing
  rescue
    _not_ready -> []
  end

  # `:active`, `:inactive` (the cluster is below level 12), or `:level_unknown`
  # while the shard cannot be read (retried).
  defp level_status(ctx, shard_index) do
    case Router.read_shard_value(ctx, shard_index, FeatureLevel.key(@level)) do
      {:ok, nil} -> :inactive
      {:ok, marker} -> if marker == FeatureLevel.marker(), do: :active, else: :level_unknown
      _unavailable -> :level_unknown
    end
  rescue
    _not_ready -> :level_unknown
  end

  # `{:ok, :some}`, `{:ok, :none}` (a single node), or `:peers_unknown` while
  # the shard's members are not known yet (retried).
  defp peer_status(shard_index) do
    case Ferricstore.Raft.WARaftBackend.cached_members(shard_index) do
      {:ok, _members, _leader} ->
        if peers(shard_index) == [], do: {:ok, :none}, else: {:ok, :some}

      _unavailable ->
        :peers_unknown
    end
  rescue
    _not_started -> :peers_unknown
  end

  # --- Helpers ---

  defp peers(shard_index) do
    case Ferricstore.Raft.WARaftBackend.cached_members(shard_index) do
      {:ok, members, _leader} ->
        members |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> List.delete(node())

      _unavailable ->
        []
    end
  rescue
    _not_started -> []
  end

  defp await_copy(peers, shard_index, id, index, deadline) do
    answers =
      Enum.map(peers, fn peer ->
        {peer, safe_call(peer, :staged_info, [shard_index, id, index], 1_000)}
      end)

    ready = for {peer, {:ready, info}} <- answers, do: {peer, info}

    cond do
      ready != [] and copies_agree?(ready) ->
        [{peer, info} | _] = ready
        {:ok, peer, info}

      ready != [] ->
        {:error, :copies_disagree}

      Enum.all?(answers, fn {_peer, answer} -> answer in [:none, :gone] end) ->
        {:error, :no_copy}

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(@poll_ms)
        await_copy(peers, shard_index, id, index, deadline)
    end
  end

  # Replicas' files are equal byte for byte (apply is deterministic), so two
  # different copies mean a damaged replica: install neither. Receipts may
  # differ (a restart removes an orphaned one) and are not compared.
  defp copies_agree?(ready) do
    ready |> Enum.map(fn {_peer, info} -> info.data end) |> Enum.uniq() |> length() == 1
  end

  defp download(peer, shard_index, id, info, dir) do
    budget = deadline(@transfer_budget_ms)

    with :ok <-
           download_part(
             peer,
             shard_index,
             id,
             :data,
             info.data,
             Path.join(dir, id <> ".incoming"),
             budget
           ) do
      case info.receipt do
        nil ->
          :ok

        receipt ->
          download_part(
            peer,
            shard_index,
            id,
            :receipt,
            receipt,
            Path.join(dir, id <> ".incoming-receipt"),
            budget
          )
      end
    end
  end

  defp download_part(peer, shard_index, id, part, %{size: size, sha256: sha}, target, budget) do
    _ = FS.rm(target)

    with {:ok, fd} <- :file.open(target, [:write, :raw, :binary, :exclusive]) do
      result =
        try do
          copy_chunks(
            peer,
            shard_index,
            id,
            part,
            fd,
            0,
            size,
            :crypto.hash_init(:sha256),
            budget
          )
        after
          :file.close(fd)
        end

      case result do
        {:ok, ^sha} -> :ok
        {:ok, _other} -> {:error, {:checksum_mismatch, part}}
        {:error, _reason} = error -> error
      end
    end
  end

  defp copy_chunks(_peer, _shard_index, _id, _part, fd, offset, size, hash, _budget)
       when offset >= size do
    case :file.sync(fd) do
      :ok -> {:ok, :crypto.hash_final(hash)}
      {:error, _reason} = error -> error
    end
  end

  defp copy_chunks(peer, shard_index, id, part, fd, offset, size, hash, budget) do
    if System.monotonic_time(:millisecond) >= budget do
      {:error, :transfer_timeout}
    else
      length = min(@chunk_bytes, size - offset)

      case safe_call(
             peer,
             :staged_chunk,
             [shard_index, id, part, offset, length],
             @call_timeout_ms
           ) do
        {:ok, bytes} when byte_size(bytes) == length ->
          case :file.write(fd, bytes) do
            :ok ->
              copy_chunks(
                peer,
                shard_index,
                id,
                part,
                fd,
                offset + length,
                size,
                :crypto.hash_update(hash, bytes),
                budget
              )

            {:error, _reason} = error ->
              error
          end

        {:ok, _short} ->
          {:error, {:short_chunk, part, offset}}

        other ->
          {:error, {:chunk_failed, part, offset, other}}
      end
    end
  end

  # The receipt goes in first: a crash before the file is renamed leaves the
  # key still missing here, and the next repair replaces both.
  defp install(dir, id, info, path, extension) do
    receipt_target = path <> ".mutation"

    receipt_result =
      case info.receipt do
        nil ->
          if FS.exists?(receipt_target), do: FS.rm(receipt_target), else: :ok

        _receipt ->
          FS.rename(Path.join(dir, id <> ".incoming-receipt"), receipt_target)
      end

    with :ok <- receipt_result,
         :ok <- FS.rename(Path.join(dir, id <> ".incoming"), path),
         :ok <- NIF.v2_fsync_dir(Path.dirname(path)),
         :ok <- validate(path, extension) do
      :ok
    end
  end

  defp validate(path, extension) do
    case NIF.prob_file_recover(path, extension) do
      :ok ->
        :ok

      {:error, reason} ->
        _ = FS.rm(path)
        _ = FS.rm(path <> ".mutation")
        {:error, {:invalid_copy, reason}}
    end
  end

  defp copy_aside(source, target) do
    tmp = target <> ".tmp"
    _ = FS.rm(tmp)

    with :ok <- FS.copy_sync_nofollow(source, tmp),
         {:ok, info} <- file_info(tmp),
         :ok <- FS.rename(tmp, target) do
      {:ok, info}
    end
  end

  defp maybe_copy_receipt(receipt, target) do
    if FS.exists?(receipt), do: copy_aside(receipt, target), else: {:ok, nil}
  end

  defp file_info(path) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      try do
        hash_file(fd, :crypto.hash_init(:sha256), 0)
      after
        :file.close(fd)
      end
    end
  end

  defp hash_file(fd, hash, size) do
    case :file.read(fd, @chunk_bytes) do
      {:ok, bytes} -> hash_file(fd, :crypto.hash_update(hash, bytes), size + byte_size(bytes))
      :eof -> {:ok, %{size: size, sha256: :crypto.hash_final(hash)}}
      {:error, _reason} = error -> error
    end
  end

  defp write_marker(path, term),
    do: FS.atomic_replace_nofollow(path, :erlang.term_to_binary(term), @marker_bytes)

  defp read_marker(path) do
    with {:ok, bytes} <- FS.read_nofollow(path, @marker_bytes) do
      {:ok, :erlang.binary_to_term(bytes, [:safe])}
    end
  rescue
    ArgumentError -> {:error, :invalid_marker}
  end

  defp sweep(root, opts) do
    now = System.os_time(:second)

    with true <- FS.dir?(root), {:ok, shard_dirs} <- FS.ls(root) do
      for shard_dir <- shard_dirs,
          dir = Path.join(root, shard_dir),
          FS.dir?(dir),
          {:ok, files} <- [FS.ls(dir)],
          file <- files,
          path = Path.join(dir, file),
          {:ok, %{mtime: mtime}} <- [File.stat(path, time: :posix)],
          now - mtime > div(Keyword.get(opts, :ttl_ms, @staging_ttl_ms), 1_000) do
        FS.rm(path)
      end
    end

    :ok
  rescue
    error ->
      Logger.warning("Prob repair: sweep of #{root} failed: #{Exception.message(error)}")
      :ok
  end

  defp applied_past?(shard_index, index) do
    case Ferricstore.Raft.WARaftBackend.storage_status(shard_index) do
      {:ok, status} -> Keyword.get(status, :last_applied, 0) >= index
      _unavailable -> false
    end
  rescue
    _not_started -> false
  end

  defp local_data_dir do
    :persistent_term.get({__MODULE__, :data_dir}, Application.get_env(:ferricstore, :data_dir))
  end

  defp safe_call(peer, function, args, timeout) do
    :erpc.call(peer, __MODULE__, function, args, timeout)
  catch
    kind, reason -> {:unreachable, {kind, reason}}
  end

  defp valid_id?(id), do: is_binary(id) and String.match?(id, ~r/\A[0-9a-f]{24}\z/)

  defp deadline(ms), do: System.monotonic_time(:millisecond) + ms

  defp wait_until(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(100)
        wait_until(fun, deadline)
    end
  end
end
