defmodule Ferricstore.Store.OpsTest do
  @moduledoc false
  # Some LocalTxStore cold-read tests assert ETS warming. Warming is suppressed
  # by the application-wide MemoryGuard skip-promotion flag, which pressure
  # tests mutate globally, so this module cannot run concurrently with them.
  use ExUnit.Case, async: false

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF

  @ops_path Path.expand("../../../lib/ferricstore/store/ops.ex", __DIR__)
  @local_read_path Path.expand("../../../lib/ferricstore/store/ops/local_read.ex", __DIR__)
  @compound_ops_path Path.expand("../../../lib/ferricstore/store/ops/compound.ex", __DIR__)

  describe "prob_dir/2" do
    test "prefers key-specific directory callback over generic prob_dir callback" do
      store = %{
        prob_dir: fn -> "/wrong/prob" end,
        prob_dir_for_key: fn "prob-key" -> "/right/prob" end
      }

      assert Ops.prob_dir(store, "prob-key") == "/right/prob"
    end
  end

  describe "LocalTxStore SET" do
    test "KEEPTTL preserves cold key TTL without reading the old value" do
      ctx = FerricStore.Instance.get(:default)
      key = "ops:local_tx:keepttl:#{System.unique_integer([:positive])}"
      shard_index = Router.shard_for(ctx, key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])
      expire_at_ms = System.os_time(:millisecond) + 60_000

      try do
        :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), 99, 123, 3})

        tx = %LocalTxStore{
          instance_ctx: ctx,
          shard_index: shard_index,
          shard_state: %{
            instance_ctx: ctx,
            keydir: keydir,
            index: shard_index,
            shard_data_path: System.tmp_dir!(),
            data_dir: System.tmp_dir!(),
            promoted_instances: %{}
          }
        }

        assert :ok == Ops.set(tx, key, "new", set_opts(%{keepttl: true}))

        assert [{^key, "new", ^expire_at_ms, _lfu, :pending, 99, 3}] =
                 :ets.lookup(keydir, key)
      after
        :ets.delete(keydir)
      end
    end
  end

  describe "LocalTxStore read-modify-write TTL preservation" do
    test "INCR preserves an existing local transaction TTL" do
      assert_local_tx_rmw_preserves_ttl("1", fn tx, key ->
        assert {:ok, 3} == Ops.incr(tx, key, 2)
        {"3", 3}
      end)
    end

    test "INCR_FLOAT preserves an existing local transaction TTL" do
      assert_local_tx_rmw_preserves_ttl("1.5", fn tx, key ->
        assert {:ok, 2.0} == Ops.incr_float(tx, key, 0.5)
        {"2.0", 2.0}
      end)
    end

    test "APPEND preserves an existing local transaction TTL" do
      assert_local_tx_rmw_preserves_ttl("base", fn tx, key ->
        assert {:ok, 9} == Ops.append(tx, key, "_tail")
        {"base_tail", "base_tail"}
      end)
    end

    test "SETRANGE preserves an existing local transaction TTL" do
      assert_local_tx_rmw_preserves_ttl("abcdef", fn tx, key ->
        assert {:ok, 6} == Ops.setrange(tx, key, 2, "XY")
        {"abXYef", "abXYef"}
      end)
    end
  end

  describe "LocalTxStore EXISTS" do
    test "cold keys are detected from metadata without reading the value" do
      ctx = FerricStore.Instance.get(:default)
      key = "ops:local_tx:exists_cold:#{System.unique_integer([:positive])}"
      shard_index = Router.shard_for(ctx, key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 123, 456, 789})

        tx = local_tx(ctx, shard_index, keydir, %{})

        assert Ops.exists?(tx, key),
               "EXISTS should trust valid cold keydir metadata instead of reading the value"
      after
        :ets.delete(keydir)
      end
    end
  end

  describe "LocalTxStore batch reads" do
    test "local single-key reads do not retry cold pread through value-only fallback" do
      source = File.read!(@ops_path)

      refute source =~ "ShardReads.v2_local_read",
             "LocalTxStore single-key reads should trust ets_lookup_warm/2 so cold misses do not double pread or lose TTL metadata"
    end

    test "local plain batch_get does not fall back to per-key get" do
      source = File.read!(@ops_path)

      refute source =~
               "def batch_get(%LocalTxStore{} = tx, keys), do: Enum.map(keys, &get(tx, &1))",
             "LocalTxStore batch_get must batch local cold reads instead of one waiter per key"
    end

    test "local cold batch reads deduplicate repeated physical locations" do
      source = File.read!(@local_read_path)

      assert source =~ "dedupe_local_batch_cold_reads",
             "LocalTxStore batch reads should pread each repeated cold {path, offset, key} once and fan out the result"
    end

    test "local cold batch reads materialize blob refs with the batch helper" do
      source = File.read!(@local_read_path)
      [_before, section] = String.split(source, "defp read_unique_local_batch_cold", parts: 2)

      [read_body, helper_section] =
        String.split(section, "defp local_materialize_blob_values", parts: 2)

      assert read_body =~ "local_materialize_blob_values",
             "LocalTxStore batch reads should materialize duplicate blob refs once per batch"

      assert helper_section =~ "BlobValue.maybe_materialize_many",
             "LocalTxStore batch reads should use the BlobValue batch materializer"

      refute read_body =~ "local_materialize_blob_value(tx, value)",
             "LocalTxStore batch reads should not materialize blob refs one entry at a time"
    end

    test "batch_get returns ordered cold values and warms matching ETS entries" do
      ctx = FerricStore.Instance.get(:default)

      {shard_index, keys} = same_shard_keys(ctx, "ops:local_tx:plain-batch-cold", 3)

      dir =
        Path.join(
          System.tmp_dir!(),
          "ops_local_tx_plain_batch_#{System.unique_integer([:positive])}"
        )

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(dir)
        path = Path.join(dir, "00000.log")
        File.touch!(path)

        assert {:ok, [{off_a, size_a}, {off_b, size_b}]} =
                 NIF.v2_append_batch_nosync(path, [
                   {Enum.at(keys, 0), "va", 0},
                   {Enum.at(keys, 1), "vb", 0}
                 ])

        :ets.insert(keydir, {Enum.at(keys, 0), nil, 0, LFU.initial(), 0, off_a, size_a})
        :ets.insert(keydir, {Enum.at(keys, 1), nil, 0, LFU.initial(), 0, off_b, size_b})

        tx =
          local_tx(ctx, shard_index, keydir, %{})
          |> put_in([Access.key!(:shard_state), :shard_data_path], dir)

        assert ["va", nil, "vb"] ==
                 Ops.batch_get(tx, [Enum.at(keys, 0), Enum.at(keys, 2), Enum.at(keys, 1)])

        assert [{_, "va", 0, _lfu, 0, ^off_a, ^size_a}] = :ets.lookup(keydir, Enum.at(keys, 0))
        assert [{_, "vb", 0, _lfu, 0, ^off_b, ^size_b}] = :ets.lookup(keydir, Enum.at(keys, 1))
      after
        :ets.delete(keydir)
        File.rm_rf(dir)
      end
    end

    test "batch_get rejects mismatched cold offsets" do
      ctx = FerricStore.Instance.get(:default)

      target = "ops:local_tx:plain-stale:target:#{System.unique_integer([:positive])}"
      other = "ops:local_tx:plain-stale:other:#{System.unique_integer([:positive])}"
      shard_index = Router.shard_for(ctx, target)

      dir =
        Path.join(
          System.tmp_dir!(),
          "ops_local_tx_plain_stale_#{System.unique_integer([:positive])}"
        )

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(dir)
        path = Path.join(dir, "00000.log")
        File.touch!(path)

        assert {:ok, [{other_off, _}, {_target_off, target_size}]} =
                 NIF.v2_append_batch_nosync(path, [
                   {other, "wrong-value", 0},
                   {target, "right-value", 0}
                 ])

        :ets.insert(keydir, {target, nil, 0, LFU.initial(), 0, other_off, target_size})

        tx =
          local_tx(ctx, shard_index, keydir, %{})
          |> put_in([Access.key!(:shard_state), :shard_data_path], dir)

        assert [nil] == Ops.batch_get(tx, [target])
      after
        :ets.delete(keydir)
        File.rm_rf(dir)
      end
    end

    test "batch_get reports per-entry cold read errors and keeps good values" do
      ctx = FerricStore.Instance.get(:default)
      {shard_index, keys} = same_shard_keys(ctx, "ops:local_tx:plain-missing-file", 2)

      dir =
        Path.join(
          System.tmp_dir!(),
          "ops_local_tx_plain_missing_#{System.unique_integer([:positive])}"
        )

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(dir)
        path = Path.join(dir, "00000.log")
        File.touch!(path)
        missing_path = Path.join(dir, "00009.log")

        assert {:ok, [{good_off, good_size}]} =
                 NIF.v2_append_batch_nosync(path, [{Enum.at(keys, 1), "good", 0}])

        :ets.insert(keydir, {Enum.at(keys, 0), nil, 0, LFU.initial(), 9, 0, 8})
        :ets.insert(keydir, {Enum.at(keys, 1), nil, 0, LFU.initial(), 0, good_off, good_size})

        tx =
          local_tx(ctx, shard_index, keydir, %{})
          |> put_in([Access.key!(:shard_state), :shard_data_path], dir)

        handler_id = {:ops_pread_corrupt, self(), make_ref()}
        parent = self()

        :telemetry.attach(
          handler_id,
          [:ferricstore, :bitcask, :pread_corrupt],
          fn event, measurements, metadata, _config ->
            send(parent, {:pread_corrupt, event, measurements, metadata})
          end,
          nil
        )

        try do
          assert [nil, "good"] == Ops.batch_get(tx, keys)

          assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                          %{path: ^missing_path, reason: :missing_file}},
                         1_000
        after
          :telemetry.detach(handler_id)
        end
      after
        :ets.delete(keydir)
        File.rm_rf(dir)
      end
    end
  end

  describe "LocalTxStore promoted compound reads" do
    test "local compound batch reads use one cold pread batch" do
      source = File.read!(@local_read_path)

      assert source =~ "ColdRead.pread_batch_keyed",
             "LocalTxStore compound_batch_get must batch keyed cold reads instead of one waiter per field"

      refute source =~ "Enum.map(compound_keys, &compound_get(tx, redis_key, &1))",
             "LocalTxStore promoted mixed compound_batch_get must partition and batch, not serialize per key"

      refute source =~ "Enum.map(compound_keys, &compound_get_meta(tx, redis_key, &1))",
             "LocalTxStore promoted mixed compound_batch_get_meta must partition and batch, not serialize per key"
    end

    test "promoted type metadata reads cold value from shared shard log" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted-type:#{System.unique_integer([:positive])}"
      type_key = "T:" <> redis_key
      shard_index = Router.shard_for(ctx, redis_key)

      shared_dir =
        Path.join(
          System.tmp_dir!(),
          "ops_local_tx_promoted_shared_#{System.unique_integer([:positive])}"
        )

      dedicated_dir =
        Path.join(
          System.tmp_dir!(),
          "ops_local_tx_promoted_dedicated_#{System.unique_integer([:positive])}"
        )

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(shared_dir)
        File.mkdir_p!(dedicated_dir)
        shared_path = Path.join(shared_dir, "00000.log")
        File.touch!(shared_path)

        assert {:ok, [{off, size}]} =
                 NIF.v2_append_batch_nosync(shared_path, [{type_key, "hash", 0}])

        :ets.insert(keydir, {type_key, nil, 0, LFU.initial(), 0, off, size})

        tx =
          local_tx(ctx, shard_index, keydir, Map.put(%{}, redis_key, %{path: dedicated_dir}))
          |> put_in([Access.key!(:shard_state), :shard_data_path], shared_dir)

        assert "hash" == Ops.compound_get(tx, redis_key, type_key)
      after
        :ets.delete(keydir)
        File.rm_rf(shared_dir)
        File.rm_rf(dedicated_dir)
      end
    end

    test "compound_batch_get returns ordered cold values and warms matching ETS entries" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:batch-cold:#{System.unique_integer([:positive])}"

      keys = [
        "H:" <> redis_key <> <<0>> <> "a",
        "H:" <> redis_key <> <<0>> <> "b",
        "H:" <> redis_key <> <<0>> <> "c"
      ]

      shard_index = Router.shard_for(ctx, redis_key)

      dir =
        Path.join(System.tmp_dir!(), "ops_local_tx_batch_#{System.unique_integer([:positive])}")

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(dir)
        path = Path.join(dir, "00000.log")
        File.touch!(path)

        assert {:ok, [{off_a, size_a}, {off_b, size_b}]} =
                 NIF.v2_append_batch_nosync(path, [
                   {Enum.at(keys, 0), "va", 0},
                   {Enum.at(keys, 1), "vb", 0}
                 ])

        :ets.insert(keydir, {Enum.at(keys, 0), nil, 0, LFU.initial(), 0, off_a, size_a})
        :ets.insert(keydir, {Enum.at(keys, 1), nil, 0, LFU.initial(), 0, off_b, size_b})

        tx =
          local_tx(ctx, shard_index, keydir, %{})
          |> put_in([Access.key!(:shard_state), :shard_data_path], dir)

        assert ["va", nil, "vb"] ==
                 Ops.compound_batch_get(tx, redis_key, [
                   Enum.at(keys, 0),
                   Enum.at(keys, 2),
                   Enum.at(keys, 1)
                 ])

        assert [{_, "va", 0, _lfu, 0, ^off_a, ^size_a}] = :ets.lookup(keydir, Enum.at(keys, 0))
        assert [{_, "vb", 0, _lfu, 0, ^off_b, ^size_b}] = :ets.lookup(keydir, Enum.at(keys, 1))
      after
        :ets.delete(keydir)
        File.rm_rf(dir)
      end
    end

    test "compound_batch_get rejects mismatched cold offsets" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:compound-stale:#{System.unique_integer([:positive])}"
      target = "H:" <> redis_key <> <<0>> <> "target"
      other = "H:" <> redis_key <> <<0>> <> "other"
      shard_index = Router.shard_for(ctx, redis_key)

      dir =
        Path.join(
          System.tmp_dir!(),
          "ops_local_tx_compound_stale_#{System.unique_integer([:positive])}"
        )

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(dir)
        path = Path.join(dir, "00000.log")
        File.touch!(path)

        assert {:ok, [{other_off, _}, {_target_off, target_size}]} =
                 NIF.v2_append_batch_nosync(path, [
                   {other, "wrong-value", 0},
                   {target, "right-value", 0}
                 ])

        :ets.insert(keydir, {target, nil, 0, LFU.initial(), 0, other_off, target_size})

        tx =
          local_tx(ctx, shard_index, keydir, %{})
          |> put_in([Access.key!(:shard_state), :shard_data_path], dir)

        assert [nil] == Ops.compound_batch_get(tx, redis_key, [target])
      after
        :ets.delete(keydir)
        File.rm_rf(dir)
      end
    end

    test "compound_get rejects malformed promoted cold location without calling NIF" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted:#{System.unique_integer([:positive])}"
      compound_key = "H:" <> redis_key <> <<0>> <> "field"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

        tx =
          local_tx(ctx, shard_index, keydir, %{
            redis_key => %{path: System.tmp_dir!()}
          })

        assert nil == Ops.compound_get(tx, redis_key, compound_key)
        assert [] == :ets.lookup(keydir, compound_key)
      after
        :ets.delete(keydir)
      end
    end

    test "compound_get_meta rejects malformed promoted cold location without calling NIF" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted-meta:#{System.unique_integer([:positive])}"
      compound_key = "H:" <> redis_key <> <<0>> <> "field"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

        tx =
          local_tx(ctx, shard_index, keydir, %{
            redis_key => %{path: System.tmp_dir!()}
          })

        assert nil == Ops.compound_get_meta(tx, redis_key, compound_key)
        assert [] == :ets.lookup(keydir, compound_key)
      after
        :ets.delete(keydir)
      end
    end
  end

  describe "LocalTxStore remote compound reads" do
    test "remote read fallbacks emit telemetry instead of exiting during shard restart" do
      ctx = unavailable_remote_ctx()
      keydir = elem(ctx.keydir_refs, 1)
      tx = local_tx(ctx, 1, keydir, %{})
      redis_key = "ops:remote_unavailable"
      compound_key = "H:" <> redis_key <> <<0>> <> "field"
      handler_id = {:ops_shard_unavailable, self(), make_ref()}
      parent = self()

      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        fn event, measurements, metadata, _config ->
          send(parent, {:shard_unavailable, event, measurements, metadata})
        end,
        nil
      )

      try do
        assert nil == Ops.compound_get(tx, redis_key, compound_key)
        assert [nil] == Ops.compound_batch_get(tx, redis_key, [compound_key])
        assert nil == Ops.compound_get_meta(tx, redis_key, compound_key)
        assert [nil] == Ops.compound_batch_get_meta(tx, redis_key, [compound_key])
        assert [] == Ops.compound_scan(tx, redis_key, "H:" <> redis_key <> <<0>>)
        assert 0 == Ops.compound_count(tx, redis_key, "H:" <> redis_key <> <<0>>)

        for request <- [
              :compound_get,
              :compound_batch_get,
              :compound_get_meta,
              :compound_batch_get_meta,
              :compound_scan,
              :compound_count
            ] do
          assert_receive {:shard_unavailable, [:ferricstore, :store, :shard_unavailable],
                          %{count: 1}, %{request: ^request, reason: :noproc, shard_index: 0}},
                         1_000
        end
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  describe "LocalTxStore remote compound writes" do
    test "remote writes route through Router instead of direct shard calls" do
      source = File.read!(@compound_ops_path)

      for request <- [
            "{:compound_put, redis_key, compound_key, value, expire_at_ms}",
            "{:compound_delete, redis_key, compound_key}",
            "{:compound_delete_prefix, redis_key, prefix}"
          ] do
        refute source =~ "GenServer.call(shard, #{request})",
               "remote LocalTxStore compound writes must use Router so Raft/durability routing is preserved"
      end

      assert source =~
               "Router.compound_put(tx.instance_ctx, redis_key, compound_key, value, expire_at_ms)"

      assert source =~ "Router.compound_delete(tx.instance_ctx, redis_key, compound_key)"
      assert source =~ "Router.compound_delete_prefix(tx.instance_ctx, redis_key, prefix)"
    end
  end

  describe "LocalTxStore compound scan performance guards" do
    test "pending prefix merge reads HLC once per scan, not once per pending key" do
      source = File.read!(@local_read_path)
      [_before, section] = String.split(source, "def merge_tx_pending_prefix", parts: 2)
      [function_body | _after] = String.split(section, "defp local_zset_index_state", parts: 2)

      assert function_body =~ "now_ms = HLC.now_ms()"
      assert String.replace(function_body, "now_ms = HLC.now_ms()", "") =~ "exp > now_ms"
      refute String.replace(function_body, "now_ms = HLC.now_ms()", "") =~ "HLC.now_ms()"
    end
  end

  describe "LocalTxStore promoted compound writes" do
    test "promoted field writes carry redis key so persistence uses dedicated storage" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted-write:#{System.unique_integer([:positive])}"
      field_key = "H:" <> redis_key <> <<0>> <> "field"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])
      dedicated_dir = Path.join(System.tmp_dir!(), "ops_local_tx_promoted_write")

      try do
        tx = local_tx(ctx, shard_index, keydir, Map.put(%{}, redis_key, %{path: dedicated_dir}))

        assert :ok = Ops.compound_put(tx, redis_key, field_key, "value", 0)
        assert_receive {:tx_pending_compound_write, ^redis_key, ^field_key, "value", 0}
        refute_receive {:tx_pending_write, ^field_key, "value", 0}
      after
        :ets.delete(keydir)
      end
    end

    test "promoted field deletes carry redis key so persistence deletes dedicated storage" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted-delete:#{System.unique_integer([:positive])}"
      field_key = "H:" <> redis_key <> <<0>> <> "field"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])
      dedicated_dir = Path.join(System.tmp_dir!(), "ops_local_tx_promoted_delete")

      try do
        tx = local_tx(ctx, shard_index, keydir, Map.put(%{}, redis_key, %{path: dedicated_dir}))

        assert :ok = Ops.compound_delete(tx, redis_key, field_key)
        assert_receive {:tx_pending_compound_delete, ^redis_key, ^field_key}
        refute_receive {:tx_pending_delete, ^field_key}
      after
        :ets.delete(keydir)
      end
    end

    test "promoted prefix deletes carry redis key for every dedicated field" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted-prefix-delete:#{System.unique_integer([:positive])}"
      prefix = "H:" <> redis_key <> <<0>>
      field_a = prefix <> "a"
      field_b = prefix <> "b"
      other_key = "H:" <> redis_key <> ":other"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])
      dedicated_dir = Path.join(System.tmp_dir!(), "ops_local_tx_promoted_prefix_delete")

      try do
        tx = local_tx(ctx, shard_index, keydir, Map.put(%{}, redis_key, %{path: dedicated_dir}))
        :ets.insert(keydir, {field_a, "a", 0, LFU.initial(), 0, 0, 1})
        :ets.insert(keydir, {field_b, "b", 0, LFU.initial(), 0, 10, 1})
        :ets.insert(keydir, {other_key, "other", 0, LFU.initial(), 0, 20, 5})

        assert :ok = Ops.compound_delete_prefix(tx, redis_key, prefix)

        assert_receive {:tx_pending_compound_delete, ^redis_key, ^field_a}
        assert_receive {:tx_pending_compound_delete, ^redis_key, ^field_b}
        refute_receive {:tx_pending_delete, ^field_a}
        refute_receive {:tx_pending_delete, ^field_b}
        assert [{^other_key, "other", 0, _lfu, 0, 20, 5}] = :ets.lookup(keydir, other_key)
      after
        Process.delete(:tx_pending_values)
        Process.delete(:tx_deleted_keys)
        :ets.delete(keydir)
      end
    end
  end

  defp set_opts(overrides) do
    Map.merge(
      %{expire_at_ms: 0, nx: false, xx: false, get: false, keepttl: false, has_expiry: false},
      overrides
    )
  end

  defp assert_local_tx_rmw_preserves_ttl(initial_value, mutate_fun) do
    ctx = FerricStore.Instance.get(:default)
    key = "ops:local_tx:rmw_ttl:#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, key)
    keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])
    expire_at_ms = System.os_time(:millisecond) + 60_000

    try do
      Process.put(:tx_pending_values, %{})
      Process.put(:tx_deleted_keys, MapSet.new())
      :ets.insert(keydir, {key, initial_value, expire_at_ms, LFU.initial(), 0, 0, 0})

      tx = local_tx(ctx, shard_index, keydir, %{})
      {expected_ets_value, expected_pending_value} = mutate_fun.(tx, key)

      assert [{^key, ^expected_ets_value, ^expire_at_ms, _lfu, :pending, _fid, _vsize}] =
               :ets.lookup(keydir, key)

      assert %{^key => {^expected_pending_value, ^expire_at_ms}} =
               Process.get(:tx_pending_values)
    after
      Process.delete(:tx_pending_values)
      Process.delete(:tx_deleted_keys)
      :ets.delete(keydir)
    end
  end

  defp local_tx(ctx, shard_index, keydir, promoted_instances) do
    %LocalTxStore{
      instance_ctx: ctx,
      shard_index: shard_index,
      shard_state: %{
        instance_ctx: ctx,
        keydir: keydir,
        index: shard_index,
        shard_data_path: System.tmp_dir!(),
        data_dir: System.tmp_dir!(),
        promoted_instances: promoted_instances
      }
    }
  end

  defp same_shard_keys(ctx, prefix, count) do
    first = "#{prefix}:0:#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, first)

    keys =
      Stream.iterate(1, &(&1 + 1))
      |> Stream.map(fn i -> "#{prefix}:#{i}:#{System.unique_integer([:positive])}" end)
      |> Stream.filter(fn key -> Router.shard_for(ctx, key) == shard_index end)
      |> Enum.take(count - 1)

    {shard_index, [first | keys]}
  end

  defp unavailable_remote_ctx do
    keydir0 = :ets.new(:"ops_remote_unavailable_0_#{System.unique_integer([:positive])}", [:set])
    keydir1 = :ets.new(:"ops_remote_unavailable_1_#{System.unique_integer([:positive])}", [:set])
    :ets.delete(keydir0)

    %FerricStore.Instance{
      name: :"ops_remote_unavailable_#{System.unique_integer([:positive])}",
      data_dir: System.tmp_dir!(),
      data_dir_expanded: System.tmp_dir!(),
      shard_count: 2,
      slot_map: Tuple.duplicate(0, 1024),
      shard_names:
        {:"missing_ops_remote_shard_#{System.unique_integer([:positive])}",
         :"unused_ops_local_shard_#{System.unique_integer([:positive])}"},
      keydir_refs: {keydir0, keydir1},
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(1, []),
      hot_cache_max_value_size: 1024,
      read_sample_rate: 0
    }
  end
end
