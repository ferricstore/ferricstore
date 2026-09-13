# After a test build: elixir -pa '_build/test/lib/*/ebin' tools/recovery-second-review-bench.exs
alias Ferricstore.Bitcask.NIF
alias Ferricstore.Flow.{Keys, LMDB, PolicyMigration, PolicyMirrorRecovery}
alias Ferricstore.Store.ColdRead

root = Path.join(System.tmp_dir!(), "recovery-review-bench-#{System.pid()}")
Ferricstore.DataDir.ensure_layout!(root, 1)
shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
path = LMDB.path(shard_path)
log = Path.join(shard_path, "00000.log")
keydir = :ets.new(:recovery_review_bench, [:set])
ctx = %{data_dir: root, keydir_refs: {keydir}, shard_count: 1}

measure = fn name, fun ->
  samples =
    for _ <- 1..5 do
      {us, result} = :timer.tc(fun)
      {us / 1_000, result}
    end

  IO.inspect(%{
    name: name,
    median_ms: samples |> Enum.map(&elem(&1, 0)) |> Enum.sort() |> Enum.at(2),
    result: samples |> hd() |> elem(1)
  })
end

try do
  rows =
    for i <- 1..5_000 do
      state_key = Keys.state_key("flow-#{i}", "review")
      key = Keys.type_catalog_member_key("review", state_key)
      {key, PolicyMigration.encode_catalog("review", state_key, 1), 0}
    end

  {:ok, locations} = NIF.v2_append_batch(log, rows)
  offsets = Enum.map(locations, &elem(&1, 0))
  expected_values = Enum.map(rows, &elem(&1, 1))

  measure.("5000 scalar physical reads", fn ->
    values =
      Enum.map(offsets, fn offset ->
        {:ok, value} = NIF.v2_pread_at(log, offset)
        value
      end)

    true = values == expected_values
    :ok
  end)

  measure.("5000 physical reads in pages of 512", fn ->
    values =
      offsets
      |> Enum.chunk_every(512)
      |> Enum.flat_map(fn chunk ->
        {:ok, values} = NIF.v2_pread_batch(log, chunk)
        values
      end)

    true = values == expected_values
    :ok
  end)

  measure.("5000 key-validated reads in pages of 512", fn ->
    values =
      Enum.zip(rows, offsets)
      |> Enum.map(fn {{key, _value, _expiry}, offset} -> {log, offset, key} end)
      |> Enum.chunk_every(512)
      |> Enum.flat_map(fn chunk ->
        {:ok, values} = ColdRead.pread_batch_keyed(chunk, 10_000)
        values
      end)

    true = values == expected_values
    :ok
  end)

  for {{key, value, expiry}, {offset, size}} <- Enum.zip(rows, locations) do
    :ets.insert(keydir, {key, value, expiry, 0, 0, offset, size})
  end

  measure.("5000 hot catalog repairs", fn ->
    PolicyMirrorRecovery.reconcile_shard(path, keydir, shard_path, 0, ctx)
  end)

  for {{key, _value, expiry}, {offset, size}} <- Enum.zip(rows, locations) do
    :ets.insert(keydir, {key, nil, expiry, 0, 0, offset, size})
  end

  measure.("5000 cold catalog repairs", fn ->
    PolicyMirrorRecovery.reconcile_shard(path, keydir, shard_path, 0, ctx)
  end)

  :ets.delete_all_objects(keydir)
  for i <- 1..500_000, do: :ets.insert(keydir, {"kv:#{i}", "value", 0, 0, 0, 0, 5})

  measure.("500000 unrelated KV rows", fn ->
    PolicyMirrorRecovery.reconcile_shard(path, keydir, shard_path, 0, ctx)
  end)
after
  LMDB.release(path)
  :ets.delete(keydir)
  File.rm_rf!(root)
end
