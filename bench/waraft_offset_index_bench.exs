defmodule WARaftOffsetIndexBench do
  @moduledoc false

  @provider :ferricstore_waraft_spike_segment_log

  def run do
    count = System.get_env("FERRICSTORE_OFFSET_BENCH_RECORDS", "20000") |> String.to_integer()
    repeats = System.get_env("FERRICSTORE_OFFSET_BENCH_READS", "500") |> String.to_integer()

    root =
      Path.join([
        System.tmp_dir!(),
        "ferricstore-offset-bench-#{System.unique_integer([:positive])}",
        "apply_projection_log"
      ])

    try do
      {append_us, :ok} =
        :timer.tc(fn ->
          1..count
          |> Enum.chunk_every(500)
          |> Enum.reduce(:ok, fn indexes, :ok ->
            @provider.write_projection_batches_sync(
              to_charlist(root),
              Enum.map(indexes, fn index ->
                {{:raft_log_pos, index, 0}, [{"key-#{index}", "value-#{index}", 0}]}
              end)
            )
          end)
        end)

      registry = :ferricstore_waraft_segment_offset_registry
      count_entries = :ets.info(registry, :size)
      registry_bytes = :ets.info(registry, :memory) * :erlang.system_info(:wordsize)

      {:ok, {_ordinal, _offset, _size}} = @provider.location_for_index(to_charlist(root), 1)

      {cold_us, _} =
        :timer.tc(fn ->
          Enum.each(1..repeats, fn _ ->
            {:ok, {_ordinal, _offset, _size}} = @provider.location_for_index(to_charlist(root), 1)
          end)
        end)

      {hot_us, _} =
        :timer.tc(fn ->
          Enum.each(1..repeats, fn _ ->
            {:ok, {_ordinal, _offset, _size}} =
              @provider.location_for_index(to_charlist(root), count)
          end)
        end)

      index_path = Path.join([root, "segment_log", "0.idx"])

      sidecar_bytes =
        Path.wildcard(Path.join(root, "segment_log/*.idx"))
        |> Enum.reduce(0, fn path, total -> total + File.stat!(path).size end)

      cached_fd_pread_us =
        if File.regular?(index_path) do
          {:ok, fd} = :file.open(to_charlist(index_path), [:read, :raw, :binary])

          {pread_us, _} =
            :timer.tc(fn ->
              Enum.each(1..repeats, fn _ ->
                {:ok, <<_::binary-size(28)>>} = :file.pread(fd, 28, 28)
              end)
            end)

          :ok = :file.close(fd)
          pread_us / repeats
        end

      {fold_us, {:ok, ^count}} =
        :timer.tc(fn ->
          @provider.fold_disk(to_charlist(root), fn _index, _entry, seen -> seen + 1 end, 0)
        end)

      IO.inspect(%{
        records: count,
        append_ms: div(append_us, 1000),
        validated_fold_ms: div(fold_us, 1000),
        cold_lookup_us: cold_us / repeats,
        cached_fd_pread_us: cached_fd_pread_us,
        hot_lookup_us: hot_us / repeats,
        registry_entries: count_entries,
        registry_bytes: registry_bytes,
        sidecar_bytes: sidecar_bytes
      })
    after
      File.rm_rf!(Path.dirname(root))
    end
  end
end

WARaftOffsetIndexBench.run()
