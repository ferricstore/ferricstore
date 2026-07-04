defmodule FerricStore.SDK.KVTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.KV
  alias FerricStore.SDK.Native.Client

  setup_all do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  setup do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])
    {:ok, client: client}
  end

  test "routes multi-key reads by shard and merges client-order results", %{
    client: client
  } do
    prefix = unique("sdk-multi")
    keys = distinct_route_keys(client, prefix, 3)

    assert :ok = KV.set(client, Enum.at(keys, 0), "a")
    assert :ok = KV.set(client, Enum.at(keys, 1), "b")
    assert :ok = KV.set(client, Enum.at(keys, 2), "c")

    assert {:ok, ["a", "b", "c"]} = KV.mget(client, keys)

    assert {:ok, groups} =
             Client.request_by_keys(client, :mget, keys, fn group_keys ->
               %{"keys" => group_keys}
             end)

    assert Enum.all?(groups, &(&1.route.lane_id == &1.route.shard + 1))
    assert Enum.flat_map(groups, & &1.indexes) |> Enum.sort() == [0, 1, 2]
  end

  test "multi-key reads preserve duplicate key positions", %{client: client} do
    key = "{#{unique("sdk-mget-dup")}}:key"

    assert :ok = KV.set(client, key, "dup-value")

    assert {:ok, ["dup-value", "dup-value", "dup-value"]} =
             KV.mget(client, [key, key, key])
  end

  test "multi-key reads reject shard responses with the wrong value count", %{client: client} do
    key = "{#{unique("sdk-mget-mismatch")}}:key"
    assert {:ok, route} = Client.route(client, key)

    {:ok, conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        reply: ["only-one-value"]
      )

    :sys.replace_state(client, fn state ->
      %{state | connections: Map.put(state.connections, route.endpoint_key, conn)}
    end)

    assert {:error, {:mismatched_mget_response, meta}} = KV.mget(client, [key, key])
    assert meta.expected == 2
    assert meta.actual == 1
    assert meta.indexes == [0, 1]
    assert meta.items == [key, key]
    assert_receive {:fake_mget, [^key, ^key]}, 100
  end

  test "rejects multi-shard writes by default before partial mutation", %{client: client} do
    [a, b] = distinct_route_keys(client, unique("sdk-partial"), 2)

    assert {:error, {:multi_shard_write_requires_explicit_policy, :mset}} =
             KV.mset(client, [{a, "a"}, {b, "b"}])

    assert {:ok, [nil, nil]} = KV.mget(client, [a, b])

    assert :ok = KV.set(client, a, "a")
    assert :ok = KV.set(client, b, "b")

    assert {:error, {:multi_shard_write_requires_explicit_policy, :del}} = KV.del(client, [a, b])
    assert {:ok, ["a", "b"]} = KV.mget(client, [a, b])
  end

  test "supports explicit per-shard multi-shard writes", %{client: client} do
    [a, b] = distinct_route_keys(client, unique("sdk-per-shard"), 2)

    assert :ok = KV.mset(client, [{a, "a"}, {b, "b"}], atomicity: :per_shard)
    assert {:ok, ["a", "b"]} = KV.mget(client, [a, b])
  end

  test "supports hashes, lists, sets, and sorted sets", %{client: client} do
    prefix = unique("sdk-collections")

    hash = "{#{prefix}}:hash"
    assert {:ok, 2} = KV.hset(client, hash, %{"a" => "1", "b" => "2"})
    assert {:ok, "1"} = KV.hget(client, hash, "a")
    assert {:ok, ["1", nil]} = KV.hmget(client, hash, ["a", "missing"])
    assert {:ok, %{"a" => "1", "b" => "2"}} = KV.hgetall(client, hash)

    list = "{#{prefix}}:list"
    assert {:ok, 2} = KV.rpush(client, list, ["a", "b"])
    assert {:ok, 3} = KV.lpush(client, list, "z")
    assert {:ok, ["z", "a", "b"]} = KV.lrange(client, list, 0, -1)
    assert {:ok, "z"} = KV.lpop(client, list)
    assert {:ok, "b"} = KV.rpop(client, list)

    set = "{#{prefix}}:set"
    assert {:ok, 2} = KV.sadd(client, set, ["a", "b"])
    assert {:ok, true} = KV.sismember(client, set, "a")
    assert {:ok, members} = KV.smembers(client, set)
    assert Enum.sort(members) == ["a", "b"]
    assert {:ok, 1} = KV.srem(client, set, "a")

    zset = "{#{prefix}}:zset"
    assert {:ok, 2} = KV.zadd(client, zset, [{2.0, "b"}, {1.0, "a"}])
    assert {:ok, "1.0"} = KV.zscore(client, zset, "a")
    assert {:ok, ["a", "b"]} = KV.zrange(client, zset, 0, -1)
    assert {:ok, ["a", "1.0", "b", "2.0"]} = KV.zrange(client, zset, 0, -1, withscores: true)
    assert {:ok, 1} = KV.zrem(client, zset, "a")
  end

  test "supports CAS, locks, rate limits, and fetch-or-compute", %{client: client} do
    key = "{#{unique("sdk-ops")}}:key"

    assert :ok = KV.set(client, key, "v1")
    assert {:ok, true} = KV.cas(client, key, "v1", "v2")
    assert {:ok, "v2"} = KV.get(client, key)
    assert {:ok, false} = KV.cas(client, key, "v1", "v3")

    lock = key <> ":lock"
    assert {:ok, "OK"} = KV.lock(client, lock, "owner", 5_000)
    assert {:ok, 1} = KV.extend(client, lock, "owner", 5_000)
    assert {:ok, 1} = KV.unlock(client, lock, "owner")

    assert {:ok, _rate} = KV.ratelimit_add(client, key <> ":rate", 1_000, 10)
    assert {:ok, ["compute", _token]} = KV.fetch_or_compute(client, key <> ":compute", 5_000)
  end

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  defp distinct_route_keys(client, prefix, count) do
    1..200
    |> Enum.map(&"{#{prefix}-#{&1}}:key")
    |> Enum.reduce_while(%{}, fn key, acc ->
      {:ok, route} = Client.route(client, key)
      next = Map.put_new(acc, route.shard, key)

      if map_size(next) == count do
        {:halt, next}
      else
        {:cont, next}
      end
    end)
    |> Map.values()
  end

  defmodule FakeConnection do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(opts), do: {:ok, opts}

    @impl true
    def handle_call({:request, _opcode, payload, _lane_id, _timeout}, _from, state) do
      send(state.parent, {:fake_mget, payload["keys"]})
      {:reply, {:ok, state.reply}, state}
    end
  end
end
