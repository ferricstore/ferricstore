defmodule FerricstoreBench.PromotedReadVariant do
  @moduledoc false
  def prepare do
    variant = System.get_env("BENCH_PROMOTED_READ", "protected")
    source_path = "apps/ferricstore/lib/ferricstore/store/router/part_09.ex"
    source = File.read!(source_path)

    case variant do
      "baseline" ->
        branch = """
                  case promoted_hot_compound_get(ctx, idx, keydir, compound_key) do
                    {:hit, value, lfu} ->
                      sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
                      value

                    :fallback ->
                      fallback_compound_get(ctx, idx, redis_key, compound_key)
                  end
        """

        true = String.contains?(source, branch)

        source =
          String.replace(
            source,
            branch,
            "          fallback_compound_get(ctx, idx, redis_key, compound_key)\n"
          )

        {first, _} = :binary.match(source, "      defp promoted_hot_compound_get(")
        {last, _} = :binary.match(source, "      @doc false\n      @spec stream_type_marker_get")

        source =
          binary_part(source, 0, first) <> binary_part(source, last, byte_size(source) - last)

        compile_variant(variant, source, source_path)

      "protected" ->
        {variant, source, nil}

      "cached" ->
        # Rejected prototype: only load its archived macro into a fresh benchmark
        # VM. Normal runs use the protected workspace implementation.
        source =
          "bench/results/hash-read-final-gate-cached-1.json"
          |> File.read!()
          |> Jason.decode!()
          |> Map.fetch!("router_source")

        true = String.contains?(source, "defp promoted_hot_compound_get(")

        compile_variant(variant, source, source_path)

      other ->
        raise("invalid BENCH_PROMOTED_READ: #{other}")
    end
  end

  def source_identity do
    paths = [
      "apps/ferricstore/lib/ferricstore/commands/hash.ex",
      "apps/ferricstore/lib/ferricstore/api/store.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_backend/batcher.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_backend/hset_cadence.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_backend/sections/leader_wait.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_backend/sections/public_api.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_backend/sections/startup.ex",
      "apps/ferricstore/lib/ferricstore/store/ops.ex",
      "apps/ferricstore/lib/ferricstore/store/promoted_publication.ex",
      "apps/ferricstore/lib/ferricstore/store/publication_epoch.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/compound/ops.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/compound/promoted.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/startup.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine/sections/read_warm.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine/sections/async_apply.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine/sections/compound_apply.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine/sections/apply_dispatch.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine/sections/cross_shard_pending.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine/sections/pending_writes.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_storage/sections/lifecycle.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_storage/sections/snapshot_install.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_storage/sections/snapshot_metadata.ex",
      "apps/ferricstore/lib/ferricstore/store/promotion.ex",
      "apps/ferricstore/lib/ferricstore/store/compaction_plan.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/info.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/compound/separated_compaction.ex",
      "apps/ferricstore/lib/ferricstore/flow/lmdb_writer.ex",
      "apps/ferricstore/lib/ferricstore/flow/lmdb_writer/projection_ops.ex",
      "apps/ferricstore/lib/ferricstore/flow/history_projector.ex",
      "apps/ferricstore/lib/ferricstore/flow/lmdb_rebuilder.ex",
      "apps/ferricstore/lib/ferricstore/flow/lmdb_rebuilder/cold_state.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_segment_reader.ex",
      "apps/ferricstore/lib/ferricstore/flow/query/composite_projection.ex",
      "apps/ferricstore/lib/ferricstore/flow/query/limits.ex",
      "apps/ferricstore/src/ferricstore_waraft_spike_segment_log.erl",
      "apps/ferricstore/lib/ferricstore/operational_guard.ex"
    ]

    paths =
      paths ++
        Enum.map(1..7, fn part ->
          "apps/ferricstore/src/ferricstore_waraft_spike_segment_log/sections/part_0#{part}.hrl"
        end)

    %{
      sha256:
        Map.new(paths, fn path ->
          {path,
           if(File.exists?(path),
             do: Base.encode16(:crypto.hash(:sha256, File.read!(path)), case: :lower),
             else: "archived-candidate-only"
           )}
        end),
      beam_md5:
        Map.new(
          [
            Ferricstore.Store.PromotedPublication,
            Ferricstore.Store.PublicationEpoch,
            Ferricstore.Raft.StateMachine,
            Ferricstore.Raft.WARaftStorage,
            Ferricstore.Store.Promotion,
            Ferricstore.Store.CompactionPlan,
            Ferricstore.Store.Shard.Compound.SeparatedCompaction,
            Ferricstore.Flow.LMDBWriter,
            Ferricstore.Flow.LMDBWriter.ProjectionOps,
            Ferricstore.Flow.HistoryProjector,
            Ferricstore.Flow.LMDBRebuilder,
            Ferricstore.Flow.LMDBRebuilder.ColdState,
            Ferricstore.Raft.WARaftSegmentReader,
            Ferricstore.OperationalGuard,
            :ferricstore_waraft_spike_segment_log
          ]
          |> Enum.filter(&Code.ensure_loaded?/1),
          fn module ->
            {inspect(module), Base.encode16(module.module_info(:md5), case: :lower)}
          end
        ),
      elixir: System.version(),
      otp: System.otp_release(),
      erl_flags: System.get_env("ERL_FLAGS")
    }
  end

  def prepare_single_hset do
    variant = System.get_env("BENCH_SINGLE_HSET", "atomic")
    path = "apps/ferricstore/lib/ferricstore/commands/hash.ex"
    source = File.read!(path)

    case variant do
      "atomic" ->
        {variant, source, nil}

      "legacy" ->
        {first, _} = :binary.match(source, "  defp hset_fields(key, [field, value], store)")
        {last, _} = :binary.match(source, "  defp hset_fields(key, field_value_pairs, store),")

        source =
          binary_part(source, 0, first) <> binary_part(source, last, byte_size(source) - last)

        Code.compiler_options(ignore_module_conflict: true)
        [{Ferricstore.Commands.Hash, beam}] = Code.compile_string(source, path)
        root = Path.join([System.tmp_dir!(), "opencode", "single-hset-legacy-#{System.pid()}"])
        if File.exists?(root), do: raise("single HSET control fixture exists")
        File.mkdir_p!(root)
        beam_path = Path.join(root, "Elixir.Ferricstore.Commands.Hash.beam")
        File.write!(beam_path, beam)

        {:module, Ferricstore.Commands.Hash} =
          :code.load_binary(Ferricstore.Commands.Hash, String.to_charlist(beam_path), beam)

        {variant, source, root}

      other ->
        raise("invalid BENCH_SINGLE_HSET: #{other}")
    end
  end

  defp compile_variant(variant, source, source_path) do
    root =
      Path.join([System.tmp_dir!(), "opencode", "promoted-read-prototype-#{System.pid()}"])

    if File.exists?(root), do: raise("router control fixture exists")
    File.mkdir_p!(root)
    Code.compiler_options(ignore_module_conflict: true)

    modules =
      Code.compile_string(source, source_path) ++
        Code.compile_file("apps/ferricstore/lib/ferricstore/store/router.ex")

    for {module, beam} <- modules do
      path = Path.join(root, "#{module}.beam")
      File.write!(path, beam)
      {:module, ^module} = :code.load_binary(module, String.to_charlist(path), beam)
    end

    true = Code.append_path(root)
    {variant, source, root}
  end
end
