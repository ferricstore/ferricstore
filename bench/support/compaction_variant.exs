defmodule FerricstoreBench.CompactionVariant do
  @moduledoc false

  # Diagnostic prototype only: parent-owned admission uses the existing merge
  # semaphore. Production acceptance would need worker ownership/crash coverage.
  def prepare do
    mode = System.get_env("BENCH_COMPACTION_ADMISSION", "parallel")
    path = "apps/ferricstore/lib/ferricstore/store/shard/info.ex"
    source = File.read!(path)

    case mode do
      "parallel" ->
        {mode, source, nil}

      "serialized" ->
        source =
          String.replace(
            source,
            "defp maybe_start_promoted_compaction(",
            "defp do_start_promoted_compaction("
          )

        marker = "      defp do_start_promoted_compaction("
        {offset, _} = :binary.match(source, marker)

        wrapper = """
              defp maybe_start_promoted_compaction(state, redis_key) do
                if state.promoted_compaction_worker == nil and
                     ShardCompound.promoted_compaction_due?(state, redis_key) do
                  case Ferricstore.Merge.Semaphore.acquire(state.index) do
                    :ok ->
                      try do
                        result = do_start_promoted_compaction(state, redis_key)
                        if result.promoted_compaction_worker == nil,
                          do: Ferricstore.Merge.Semaphore.release(state.index)
                        result
                      catch
                        kind, reason ->
                          Ferricstore.Merge.Semaphore.release(state.index)
                          :erlang.raise(kind, reason, __STACKTRACE__)
                      end
                    {:busy, _holder} ->
                      schedule_promoted_compaction_retry(state, redis_key)
                  end
                else
                  do_start_promoted_compaction(state, redis_key)
                end
              end

        """

        source =
          binary_part(source, 0, offset) <>
            wrapper <> binary_part(source, offset, byte_size(source) - offset)

        complete = """
                    %{promoted_compaction_worker: %{job_ref: job_ref, pid: pid} = worker} = state
                  ) do
                Process.demonitor(worker.monitor_ref, [:flush])
        """

        true = String.contains?(source, complete)

        source =
          String.replace(
            source,
            complete,
            complete <> "        Ferricstore.Merge.Semaphore.release(state.index)\n"
          )

        down = """
                release_promoted_compaction_latch_if_owned(
                  Map.get(worker, :latch_token, :none),
                  worker.pid
                )
        """

        true = String.contains?(source, down)

        source =
          String.replace(
            source,
            down,
            "        Ferricstore.Merge.Semaphore.release(state.index)\n" <> down
          )

        root = Path.join([System.tmp_dir!(), "opencode", "compaction-admission-#{System.pid()}"])
        if File.exists?(root), do: raise("compaction prototype directory exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)

        modules =
          Code.compile_string(source, path) ++
            Code.compile_file("apps/ferricstore/lib/ferricstore/store/shard.ex")

        for {module, beam} <- modules do
          beam_path = Path.join(root, "#{module}.beam")
          File.write!(beam_path, beam)
          {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        end

        {mode, source, root}

      other ->
        raise("invalid compaction admission mode: #{other}")
    end
  end
end
