defmodule FerricStore.SDK.KV do
  @moduledoc """
  Native FerricStore key/value commands.
  """

  alias FerricStore.SDK.Native.{Client, Opcodes}

  @type client :: GenServer.server()

  @spec get(client(), binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def get(client, key, opts \\ []),
    do: Client.request_by_key(client, Opcodes.get(), key, %{"key" => key}, opts)

  @spec set(client(), binary(), term(), keyword()) :: :ok | {:ok, term()} | {:error, term()}
  def set(client, key, value, opts \\ []) do
    case Client.set(client, key, value, opts) do
      :ok -> :ok
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec del(client(), binary() | [binary()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def del(client, key_or_keys, opts \\ [])

  def del(client, key, opts) when is_binary(key), do: del(client, [key], opts)
  def del(_client, [], _opts), do: {:ok, 0}

  def del(client, keys, opts) when is_list(keys) do
    with :ok <- ensure_multi_key_write_policy(client, :del, keys, opts),
         {:ok, groups} <-
           Client.request_by_keys(client, Opcodes.del(), keys, &%{"keys" => &1}, opts) do
      {:ok, sum_integer_values(groups)}
    end
  end

  @spec mget(client(), [binary()], keyword()) :: {:ok, [term()]} | {:error, term()}
  def mget(client, keys, opts \\ [])

  def mget(_client, [], _opts), do: {:ok, []}

  def mget(client, keys, opts) when is_list(keys) do
    with {:ok, groups} <-
           Client.request_by_keys(client, Opcodes.mget(), keys, &%{"keys" => &1}, opts) do
      {:ok, merge_ordered_values(groups, length(keys))}
    end
  end

  @spec mset(client(), map() | [{binary(), term()}], keyword()) :: :ok | {:error, term()}
  def mset(client, pairs, opts \\ []) do
    pairs = normalize_pairs(pairs)
    keys = Enum.map(pairs, fn {key, _value} -> key end)

    with :ok <- ensure_multi_key_write_policy(client, :mset, keys, opts),
         {:ok, groups} <-
           Client.request_by_items(
             client,
             Opcodes.mset(),
             pairs,
             fn {key, _value} -> key end,
             fn group_pairs -> %{"pairs" => Enum.map(group_pairs, &pair_payload/1)} end,
             opts
           ) do
      ok_groups(groups)
    end
  end

  @spec cas(client(), binary(), term(), term(), keyword()) ::
          {:ok, boolean() | nil} | {:error, term()}
  def cas(client, key, expected, value, opts \\ []) do
    payload =
      %{"key" => key, "expected" => expected, "value" => value}
      |> maybe_put("ttl", Keyword.get(opts, :ttl))

    Client.request_by_key(client, Opcodes.cas(), key, payload, opts)
  end

  @spec lock(client(), binary(), binary(), pos_integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def lock(client, key, owner, ttl_ms, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.lock(),
      key,
      %{"key" => key, "owner" => owner, "ttl_ms" => ttl_ms},
      opts
    )
  end

  @spec unlock(client(), binary(), binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def unlock(client, key, owner, opts \\ []) do
    Client.request_by_key(client, Opcodes.unlock(), key, %{"key" => key, "owner" => owner}, opts)
  end

  @spec extend(client(), binary(), binary(), pos_integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def extend(client, key, owner, ttl_ms, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.extend(),
      key,
      %{"key" => key, "owner" => owner, "ttl_ms" => ttl_ms},
      opts
    )
  end

  @spec ratelimit_add(client(), binary(), pos_integer(), pos_integer(), pos_integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def ratelimit_add(client, key, window_ms, max, count \\ 1, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.ratelimit_add(),
      key,
      %{"key" => key, "window_ms" => window_ms, "max" => max, "count" => count},
      opts
    )
  end

  @spec fetch_or_compute(client(), binary(), pos_integer(), keyword()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_or_compute(client, key, ttl_ms, opts \\ []) do
    payload =
      %{"key" => key, "ttl_ms" => ttl_ms}
      |> maybe_put("hint", Keyword.get(opts, :hint))

    Client.request_by_key(client, Opcodes.fetch_or_compute(), key, payload, opts)
  end

  @spec fetch_or_compute_result(client(), binary(), term(), pos_integer(), keyword()) ::
          :ok | {:error, term()}
  def fetch_or_compute_result(client, key, value, ttl_ms, opts \\ []) do
    client
    |> Client.request_by_key(
      Opcodes.fetch_or_compute_result(),
      key,
      %{"key" => key, "value" => value, "ttl_ms" => ttl_ms},
      opts
    )
    |> ok_value()
  end

  @spec fetch_or_compute_error(client(), binary(), binary(), keyword()) :: :ok | {:error, term()}
  def fetch_or_compute_error(client, key, message, opts \\ []) do
    client
    |> Client.request_by_key(
      Opcodes.fetch_or_compute_error(),
      key,
      %{"key" => key, "message" => message},
      opts
    )
    |> ok_value()
  end

  @spec hset(client(), binary(), map(), keyword()) :: {:ok, term()} | {:error, term()}
  def hset(client, key, fields, opts \\ []) when is_map(fields) do
    Client.request_by_key(client, Opcodes.hset(), key, %{"key" => key, "fields" => fields}, opts)
  end

  @spec hget(client(), binary(), binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def hget(client, key, field, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.hget(),
      key,
      %{"key" => key, "field" => field},
      opts
    )
  end

  @spec hmget(client(), binary(), [binary()], keyword()) :: {:ok, [term()]} | {:error, term()}
  def hmget(client, key, fields, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.hmget(),
      key,
      %{"key" => key, "fields" => fields},
      opts
    )
  end

  @spec hgetall(client(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def hgetall(client, key, opts \\ []) do
    Client.request_by_key(client, Opcodes.hgetall(), key, %{"key" => key}, opts)
  end

  @spec lpush(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def lpush(client, key, values, opts \\ []),
    do: push(client, Opcodes.lpush(), key, values, opts)

  @spec rpush(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def rpush(client, key, values, opts \\ []),
    do: push(client, Opcodes.rpush(), key, values, opts)

  @spec lpop(client(), binary(), pos_integer(), keyword()) :: {:ok, term()} | {:error, term()}
  def lpop(client, key, count \\ 1, opts \\ []),
    do: pop(client, Opcodes.lpop(), key, count, opts)

  @spec rpop(client(), binary(), pos_integer(), keyword()) :: {:ok, term()} | {:error, term()}
  def rpop(client, key, count \\ 1, opts \\ []),
    do: pop(client, Opcodes.rpop(), key, count, opts)

  @spec lrange(client(), binary(), integer(), integer(), keyword()) ::
          {:ok, [term()]} | {:error, term()}
  def lrange(client, key, start, stop, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.lrange(),
      key,
      %{"key" => key, "start" => start, "stop" => stop},
      opts
    )
  end

  @spec sadd(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def sadd(client, key, members, opts \\ []),
    do: set_members(client, Opcodes.sadd(), key, members, opts)

  @spec srem(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def srem(client, key, members, opts \\ []),
    do: set_members(client, Opcodes.srem(), key, members, opts)

  @spec smembers(client(), binary(), keyword()) :: {:ok, [term()]} | {:error, term()}
  def smembers(client, key, opts \\ []) do
    Client.request_by_key(client, Opcodes.smembers(), key, %{"key" => key}, opts)
  end

  @spec sismember(client(), binary(), binary(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def sismember(client, key, member, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.sismember(),
      key,
      %{"key" => key, "member" => member},
      opts
    )
  end

  @spec zadd(client(), binary(), list(), keyword()) :: {:ok, term()} | {:error, term()}
  def zadd(client, key, items, opts \\ []) when is_list(items) do
    Client.request_by_key(
      client,
      Opcodes.zadd(),
      key,
      %{"key" => key, "items" => Enum.map(items, &zitem_payload/1)},
      opts
    )
  end

  @spec zrem(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def zrem(client, key, members, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.zrem(),
      key,
      %{"key" => key, "members" => list_wrap(members)},
      opts
    )
  end

  @spec zrange(client(), binary(), integer(), integer(), keyword()) ::
          {:ok, [term()]} | {:error, term()}
  def zrange(client, key, start, stop, opts \\ []) do
    payload =
      %{"key" => key, "start" => start, "stop" => stop}
      |> maybe_put("withscores", Keyword.get(opts, :withscores))

    Client.request_by_key(client, Opcodes.zrange(), key, payload, opts)
  end

  @spec zscore(client(), binary(), binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def zscore(client, key, member, opts \\ []) do
    Client.request_by_key(
      client,
      Opcodes.zscore(),
      key,
      %{"key" => key, "member" => member},
      opts
    )
  end

  defp push(client, opcode, key, values, opts) do
    Client.request_by_key(
      client,
      opcode,
      key,
      %{"key" => key, "values" => list_wrap(values)},
      opts
    )
  end

  defp pop(client, opcode, key, count, opts) do
    Client.request_by_key(client, opcode, key, %{"key" => key, "count" => count}, opts)
  end

  defp set_members(client, opcode, key, members, opts) do
    Client.request_by_key(
      client,
      opcode,
      key,
      %{"key" => key, "members" => list_wrap(members)},
      opts
    )
  end

  defp normalize_pairs(%{} = pairs), do: Map.to_list(pairs)
  defp normalize_pairs(pairs) when is_list(pairs), do: Enum.map(pairs, &normalize_pair/1)

  defp normalize_pair({key, value}) when is_binary(key), do: {key, value}
  defp normalize_pair([key, value]) when is_binary(key), do: {key, value}
  defp normalize_pair(%{"key" => key, "value" => value}) when is_binary(key), do: {key, value}
  defp normalize_pair(%{key: key, value: value}) when is_binary(key), do: {key, value}
  defp normalize_pair(pair), do: raise(ArgumentError, "invalid mset pair: #{inspect(pair)}")

  defp pair_payload({key, value}), do: %{"key" => key, "value" => value}

  defp zitem_payload([score, member]) when is_number(score) and is_binary(member),
    do: [score, member]

  defp zitem_payload({score, member}) when is_number(score) and is_binary(member),
    do: [score, member]

  defp zitem_payload(%{"score" => score, "member" => member})
       when is_number(score) and is_binary(member),
       do: [score, member]

  defp zitem_payload(%{score: score, member: member}) when is_number(score) and is_binary(member),
    do: [score, member]

  defp zitem_payload(item), do: raise(ArgumentError, "invalid zadd item: #{inspect(item)}")

  defp ensure_multi_key_write_policy(_client, _operation, [_single], _opts), do: :ok
  defp ensure_multi_key_write_policy(_client, _operation, [], _opts), do: :ok

  defp ensure_multi_key_write_policy(client, operation, keys, opts) do
    if Keyword.get(opts, :atomicity) == :per_shard do
      :ok
    else
      ensure_same_shard_write(client, operation, keys)
    end
  end

  defp ensure_same_shard_write(client, operation, keys) do
    keys
    |> Enum.reduce_while({:ok, MapSet.new()}, fn key, {:ok, shards} ->
      case Client.route(client, key) do
        {:ok, route} -> {:cont, {:ok, MapSet.put(shards, route.shard)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, shards} ->
        if MapSet.size(shards) <= 1 do
          :ok
        else
          {:error, {:multi_shard_write_requires_explicit_policy, operation}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp merge_ordered_values(groups, count) do
    tuple = List.to_tuple(List.duplicate(nil, count))

    groups
    |> Enum.reduce(tuple, fn %{indexes: indexes, value: values}, acc ->
      indexes
      |> Enum.zip(values)
      |> Enum.reduce(acc, fn {index, value}, inner -> put_elem(inner, index, value) end)
    end)
    |> Tuple.to_list()
  end

  defp sum_integer_values(groups) do
    Enum.reduce(groups, 0, fn
      %{value: value}, acc when is_integer(value) -> acc + value
      _group, acc -> acc
    end)
  end

  defp ok_groups(groups) do
    if Enum.all?(groups, &(&1.value == "OK")) do
      :ok
    else
      {:ok, Enum.map(groups, & &1.value)}
    end
  end

  defp ok_value({:ok, "OK"}), do: :ok
  defp ok_value({:ok, value}), do: {:ok, value}
  defp ok_value({:error, reason}), do: {:error, reason}

  defp list_wrap(value) when is_list(value), do: value
  defp list_wrap(value), do: [value]

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
