defmodule Ferricstore.APIReadFailureTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.ReadResult

  @default_key {FerricStore.Instance, :default}

  defmodule FailingShard do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)
    def init(:ok), do: {:ok, nil}
    def handle_call(_request, _from, state), do: {:reply, {:error, "ERR injected failure"}, state}
  end

  setup do
    original = :persistent_term.get(@default_key, :missing)
    ctx = unavailable_ctx()
    :persistent_term.put(@default_key, ctx)

    on_exit(fn ->
      File.rm_rf(ctx.data_dir)

      if original == :missing do
        :persistent_term.erase(@default_key)
      else
        :persistent_term.put(@default_key, original)
      end
    end)

    {:ok, ctx: ctx}
  end

  test "randomkey propagates key enumeration storage failures" do
    assert ReadResult.failure(:shard_unavailable) == FerricStore.API.Generic.randomkey()
  end

  test "dbsize uses the router count path without materializing all keys" do
    assert ReadResult.failure(:keydir_unavailable) == FerricStore.API.Generic.dbsize()
  end

  test "ratelimit_add does not wrap router failures in a successful tuple" do
    start_supervised!({FailingShard, name: :api_read_failure_missing_shard})

    assert {:error, _reason} = FerricStore.API.Locks.ratelimit_add("limit", 1_000, 10)
  end

  test "cas propagates router failures instead of raising a case-clause error" do
    start_supervised!({FailingShard, name: :api_read_failure_missing_shard})

    assert {:error, _reason} = FerricStore.API.Generic.cas("key", "old", "new")
  end

  test "smismember does not interpret storage failures as present members" do
    assert {:error, _reason} = FerricStore.API.Sets.smismember("set", ["member"])
  end

  test "exists does not turn storage failures into a missing-key result" do
    assert {:error, _reason} = FerricStore.API.Generic.exists("key")
  end

  test "TTL read APIs do not wrap storage failures as successful values" do
    assert {:error, _reason} = FerricStore.API.Generic.ttl("key")
    assert {:error, _reason} = FerricStore.API.Generic.expiretime("key")
    assert {:error, _reason} = FerricStore.API.Generic.pexpiretime("key")
  end

  test "packed_batch_get rejects malformed or non-canonical frames without raising" do
    malformed = [
      <<>>,
      <<1::32>>,
      <<1::32, 2::16, "a">>,
      <<0::32, 0>>,
      <<1::32, 0::16, 0>>
    ]

    Enum.each(malformed, fn payload ->
      assert {:error, "ERR invalid packed batch GET payload"} =
               FerricStore.API.System.packed_batch_get(payload)
    end)
  end

  test "SET rejects conflicting or invalid options before touching storage" do
    assert {:error, "ERR XX and NX options at the same time are not compatible"} =
             FerricStore.API.Strings.set("key", "value", nx: true, xx: true)

    assert {:error, "ERR syntax error"} =
             FerricStore.API.Strings.set("key", "value", ttl: 1_000, pxat: 2_000)

    assert {:error, "ERR syntax error"} =
             FerricStore.API.Strings.set("key", "value", ttl: 1_000, keepttl: true)

    assert {:error, "ERR invalid expire time in 'set' command"} =
             FerricStore.API.Strings.set("key", "value", ttl: -1)
  end

  test "DEL treats an empty key list as a no-op without selecting a shard" do
    assert {:ok, 0} = FerricStore.API.Strings.del([])
  end

  test "GETEX rejects malformed expiry options before touching storage" do
    for opts <- [
          [ttl: 0],
          [ttl: -1],
          [ttl: "1000"]
        ] do
      assert {:error, "ERR invalid expire time in 'getex' command"} =
               FerricStore.API.Strings.getex("key", opts)
    end

    for opts <- [
          [persist: true, ttl: 1_000],
          [persist: "true"],
          [unknown: true],
          %{ttl: 1_000}
        ] do
      assert {:error, "ERR syntax error"} = FerricStore.API.Strings.getex("key", opts)
    end
  end

  test "SETEX and PSETEX validate positive TTLs and value size before storage", %{ctx: ctx} do
    for {fun, command} <- [
          {&FerricStore.API.Strings.setex/3, "setex"},
          {&FerricStore.API.Strings.psetex/3, "psetex"}
        ] do
      expected_error = "ERR invalid expire time in '#{command}' command"

      for ttl <- [0, -1, "1000"] do
        assert {:error, ^expected_error} = fun.("key", ttl, "value")
      end

      oversized = :binary.copy("x", ctx.max_value_size + 1)

      assert {:error, message} = fun.("key", 1, oversized)
      assert message =~ "ERR value too large"
    end
  end

  test "bitmap and HyperLogLog APIs reject invalid collection contents without raising" do
    assert {:error, "ERR bit offset is not an integer or out of range"} =
             FerricStore.API.Bitmap.setbit("key", "not-an-offset", 1)

    assert {:error, "ERR wrong number of arguments for 'pfcount' command"} =
             FerricStore.API.HyperLogLog.pfcount([])
  end

  test "probabilistic stores derive paths from the immutable instance context", %{ctx: ctx} do
    expected =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Path.join("prob")

    prob_store = FerricStore.API.Store.build_prob_store("key")
    topk_store = FerricStore.API.Store.build_topk_store("key")

    assert prob_store.prob_dir.() == expected
    assert prob_store.prob_dir_for_key.("other") == expected
    assert topk_store.prob_dir.() == expected
    assert topk_store.prob_dir_for_key.("other") == expected
  end

  defp unavailable_ctx do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_api_read_failure_#{System.unique_integer([:positive])}"
      )

    keydir = :ets.new(:api_read_failure_keydir, [:set])
    :ets.delete(keydir)

    %FerricStore.Instance{
      name: :api_read_failure,
      data_dir: data_dir,
      data_dir_expanded: Path.expand(data_dir),
      shard_count: 1,
      slot_map: Tuple.duplicate(0, 1_024),
      shard_names: {:api_read_failure_missing_shard},
      keydir_refs: {keydir},
      pressure_flags: :atomics.new(2, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(1, []),
      hot_cache_max_value_size: 1_024,
      max_value_size: 1_048_576,
      read_sample_rate: 0
    }
  end
end
