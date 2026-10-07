# Component diagnostic: partial-key cache selection and real cold-source preparation.
defmodule FerricstoreBench.ApplyProjectionCacheScaling do
  alias Ferricstore.Raft.WARaftSegmentReader, as: Reader
  alias Ferricstore.Flow.LMDBRebuilder.ColdState

  def run do
    {:ok, _} = Application.ensure_all_started(:telemetry)
    {:ok, _} = Reader.TableOwner.start_link()
    root = Path.join([System.tmp_dir!(), "opencode", "projection-scaling-#{System.pid()}"])
    if File.exists?(root), do: raise("fixture exists")
    table = :ets.whereis(:ferricstore_waraft_apply_projection_cache)
    sizes = [0, 10_000, 50_000]
    indexes = MapSet.new(1..128)
    storage_root = Path.join([root, "target", "waraft", "ferricstore_waraft_backend.1"])

    try do
      targets =
        for index <- indexes,
            key <- ["state", "sibling"],
            do: {{storage_root, index, key}, "value", 0}

      true = :ets.insert(table, targets)

      rows =
        for size <- sizes do
          :ets.match_delete(table, {{"unrelated-root", :_, :_}, :_, :_})

          if size > 0 do
            noise = for index <- 1..size, do: {{"unrelated-root", index, "noise"}, "noise", 0}
            true = :ets.insert(table, noise)
          end

          selections =
            for mode <- [:per_index, :single_scan] do
              {us, selected} =
                :timer.tc(fn ->
                  for _ <- 1..10, do: select(table, storage_root, indexes, mode)
                end)

              expected =
                Enum.sort(
                  for(
                    {{_, index, key}, value, expiry} <- targets,
                    do: {index, key, value, expiry}
                  )
                )

              true = Enum.all?(selected, &(Enum.sort(&1) == expected))
              %{mode: mode, repetitions: 10, elapsed_us: us}
            end

          data_dir = Path.join(root, "prepare-#{size}")
          ctx = %{data_dir: data_dir}

          entries =
            for index <- 1..128 do
              key = Ferricstore.Flow.Keys.state_key("scaling-#{index}")

              record = %{
                id: "scaling-#{index}",
                type: "scaling",
                state: "queued",
                version: 1,
                attempts: 0,
                fencing_token: 0,
                created_at_ms: 1,
                updated_at_ms: 1,
                next_run_at_ms: 0,
                priority: 0,
                partition_key: nil,
                root_flow_id: "scaling-#{index}"
              }

              value = Ferricstore.Flow.encode_record(record)
              :ok = Reader.put_apply_projection(data_dir, 0, index, [{key, value, 0}])
              {key, value, 0, 0, {:waraft_apply_projection, index}, 0, byte_size(value)}
            end

          {us, decoded} =
            :timer.tc(fn -> ColdState.read_and_decode(entries, data_dir, 0, ctx) end)

          true = length(decoded) == length(entries)
          Reader.clear_apply_projection_cache(data_dir, 0)

          %{
            unrelated_rows: size,
            selections: selections,
            prepare_us: us,
            decoded: length(decoded)
          }
        end

      report = %{
        component_only: true,
        cache_type: :ets.info(table, :type),
        target_indexes: MapSet.size(indexes),
        rows: rows,
        source_sha256:
          Map.new(
            [
              "apps/ferricstore/lib/ferricstore/raft/waraft_segment_reader.ex",
              "apps/ferricstore/lib/ferricstore/flow/lmdb_rebuilder/cold_state.ex"
            ],
            fn path ->
              {path, Base.encode16(:crypto.hash(:sha256, File.read!(path)), case: :lower)}
            end
          )
      }

      File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
      IO.inspect(rows, label: "PROJECTION_SCALING")
    after
      File.rm_rf!(root)
    end
  end

  defp select(table, root, indexes, :per_index) do
    Enum.flat_map(indexes, fn index ->
      :ets.select(table, [
        {{{root, index, :"$1"}, :"$2", :"$3"}, [], [{{index, :"$1", :"$2", :"$3"}}]}
      ])
    end)
  end

  defp select(table, root, indexes, :single_scan) do
    membership = Map.new(indexes, &{&1, true})

    :ets.select(table, [
      {{{root, :"$1", :"$2"}, :"$3", :"$4"}, [{:is_map_key, :"$1", {:const, membership}}],
       [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ])
  end
end

FerricstoreBench.ApplyProjectionCacheScaling.run()
