defmodule FerricStore.Pipe do
  @moduledoc """
  Pipeline accumulator for batching multiple FerricStore commands.

  Used with `FerricStore.pipeline/1` to batch multiple operations into a single
  Raft entry per shard. Commands are accumulated in reverse order and on execute,
  converted to command tuples and dispatched through the Coordinator. Single-shard
  pipelines commit in one Raft round-trip; cross-shard pipelines use the
  anchor-shard mechanism.

  Results are normalized to match the FerricStore public API format (e.g.
  `{:ok, value}` for GET, `:ok` for DEL) rather than raw Dispatcher values.

  ## Usage

      FerricStore.pipeline(fn pipe ->
        pipe
        |> FerricStore.Pipe.set("key1", "val1")
        |> FerricStore.Pipe.set("key2", "val2")
        |> FerricStore.Pipe.incr("counter")
      end)

  """

  @type command ::
          {:set, binary(), binary(), keyword()}
          | {:get, binary()}
          | {:del, binary()}
          | {:incr, binary()}
          | {:incr_by, binary(), integer()}
          | {:hset, binary(), map()}
          | {:hget, binary(), binary()}
          | {:lpush, binary(), [binary()]}
          | {:rpush, binary(), [binary()]}
          | {:sadd, binary(), [binary()]}
          | {:zadd, binary(), [{number(), binary()}]}
          | {:expire, binary(), non_neg_integer()}

  @type t :: %__MODULE__{commands: [command()]}

  defstruct commands: []

  @doc "Creates a new empty pipeline."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds a SET command to the pipeline."
  @spec set(t(), binary(), binary(), keyword()) :: t()
  def set(%__MODULE__{} = pipe, key, value, opts \\ []) do
    %{pipe | commands: [{:set, key, value, opts} | pipe.commands]}
  end

  @doc "Adds a GET command to the pipeline."
  @spec get(t(), binary()) :: t()
  def get(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:get, key} | pipe.commands]}
  end

  @doc "Adds a DEL command to the pipeline."
  @spec del(t(), binary()) :: t()
  def del(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:del, key} | pipe.commands]}
  end

  @doc "Adds an INCR command to the pipeline."
  @spec incr(t(), binary()) :: t()
  def incr(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:incr, key} | pipe.commands]}
  end

  @doc "Adds an INCRBY command to the pipeline."
  @spec incr_by(t(), binary(), integer()) :: t()
  def incr_by(%__MODULE__{} = pipe, key, amount) do
    %{pipe | commands: [{:incr_by, key, amount} | pipe.commands]}
  end

  @doc "Adds an HSET command to the pipeline."
  @spec hset(t(), binary(), map()) :: t()
  def hset(%__MODULE__{} = pipe, key, fields) do
    %{pipe | commands: [{:hset, key, fields} | pipe.commands]}
  end

  @doc "Adds an HGET command to the pipeline."
  @spec hget(t(), binary(), binary()) :: t()
  def hget(%__MODULE__{} = pipe, key, field) do
    %{pipe | commands: [{:hget, key, field} | pipe.commands]}
  end

  @doc "Adds an LPUSH command to the pipeline."
  @spec lpush(t(), binary(), [binary()]) :: t()
  def lpush(%__MODULE__{} = pipe, key, elements) do
    %{pipe | commands: [{:lpush, key, elements} | pipe.commands]}
  end

  @doc "Adds an RPUSH command to the pipeline."
  @spec rpush(t(), binary(), [binary()]) :: t()
  def rpush(%__MODULE__{} = pipe, key, elements) do
    %{pipe | commands: [{:rpush, key, elements} | pipe.commands]}
  end

  @doc "Adds a SADD command to the pipeline."
  @spec sadd(t(), binary(), [binary()]) :: t()
  def sadd(%__MODULE__{} = pipe, key, members) do
    %{pipe | commands: [{:sadd, key, members} | pipe.commands]}
  end

  @doc "Adds a ZADD command to the pipeline."
  @spec zadd(t(), binary(), [{number(), binary()}]) :: t()
  def zadd(%__MODULE__{} = pipe, key, score_member_pairs) do
    %{pipe | commands: [{:zadd, key, score_member_pairs} | pipe.commands]}
  end

  @doc "Adds an EXPIRE command to the pipeline."
  @spec expire(t(), binary(), non_neg_integer()) :: t()
  def expire(%__MODULE__{} = pipe, key, ttl_ms) do
    %{pipe | commands: [{:expire, key, ttl_ms} | pipe.commands]}
  end

  @doc """
  Executes all accumulated pipeline commands as a single batch Raft entry
  per shard via the Coordinator.

  This is called internally by `FerricStore.pipeline/1`. Commands are converted
  to command tuples and dispatched through `Ferricstore.Transaction.Coordinator`,
  which groups them by shard and submits each group as a single `{:batch}` or
  `{:tx_execute}` Raft entry. Single-shard pipelines commit in one Raft round-trip;
  cross-shard pipelines use the anchor-shard mechanism.

  Results are returned in the original command order.
  """
  @spec execute(t()) :: [term()] | FerricStore.write_error()
  def execute(%__MODULE__{commands: []}), do: []

  def execute(%__MODULE__{commands: commands}) do
    with :ok <- authorize_commands(commands) do
      ordered = Enum.reverse(commands)
      ctx = FerricStore.Instance.get(:default)

      case classify_batch(ordered) do
        :all_gets ->
          keys = Enum.map(ordered, fn {:get, k} -> k end)
          values = Ferricstore.Store.Router.batch_get(ctx, keys)
          pipeline_get_results(ctx, keys, values)

        :all_sets ->
          kv_pairs = Enum.map(ordered, fn {:set, k, v, _opts} -> {k, v} end)
          execute_batch_sets(ctx, ordered, kv_pairs)

        {:mixed_get_set, _} ->
          execute_mixed_get_set(ctx, ordered)

        :complex ->
          queue = Enum.map(ordered, &to_resp_command/1)
          raw_results = Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)

          ordered
          |> Enum.zip(raw_results)
          |> Enum.map(fn {cmd, raw} -> normalize_result(cmd, raw) end)
      end
    end
  end

  defp authorize_commands(commands) do
    commands
    |> Enum.map(&elem(&1, 1))
    |> Ferricstore.Flow.InternalKey.authorize_public()
  end

  defp classify_batch(commands) do
    classify_batch(commands, nil, MapSet.new(), MapSet.new())
  end

  defp classify_batch([], kind, _written, _read), do: kind || :complex

  defp classify_batch([{:get, key} | rest], kind, written, read) do
    if MapSet.member?(written, key) do
      :complex
    else
      new_kind =
        case kind do
          nil -> :all_gets
          :all_gets -> :all_gets
          :all_sets -> {:mixed_get_set, true}
          {:mixed_get_set, _} -> {:mixed_get_set, true}
          _ -> :complex
        end

      if new_kind == :complex,
        do: :complex,
        else: classify_batch(rest, new_kind, written, MapSet.put(read, key))
    end
  end

  defp classify_batch([{:set, key, _v, opts} | rest], kind, written, read) do
    if opts != [] or MapSet.member?(read, key) do
      :complex
    else
      new_kind =
        case kind do
          nil -> :all_sets
          :all_sets -> :all_sets
          :all_gets -> {:mixed_get_set, true}
          {:mixed_get_set, _} -> {:mixed_get_set, true}
          _ -> :complex
        end

      if new_kind == :complex,
        do: :complex,
        else: classify_batch(rest, new_kind, MapSet.put(written, key), read)
    end
  end

  defp classify_batch(_, _, _, _), do: :complex

  defp execute_batch_sets(ctx, _ordered, kv_pairs) do
    Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)
  end

  defp execute_mixed_get_set(ctx, ordered) do
    indexed = Enum.with_index(ordered)
    get_ops = for {{:get, key}, i} <- indexed, do: {i, key}
    set_ops = for {{:set, key, value, _}, i} <- indexed, do: {i, key, value}

    set_results =
      if set_ops != [] do
        kv_pairs = Enum.map(set_ops, fn {_i, k, v} -> {k, v} end)

        results =
          Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)

        set_ops
        |> Enum.zip(results)
        |> Map.new(fn {{i, _, _}, r} -> {i, r} end)
      else
        %{}
      end

    get_results =
      if get_ops != [] do
        keys = Enum.map(get_ops, &elem(&1, 1))
        values = Ferricstore.Store.Router.batch_get(ctx, keys)
        results = pipeline_get_results(ctx, keys, values)

        get_ops
        |> Enum.zip(results)
        |> Map.new(fn {{i, _}, result} -> {i, result} end)
      else
        %{}
      end

    count = length(ordered)

    for i <- 0..(count - 1) do
      Map.get(get_results, i) || Map.get(set_results, i)
    end
  end

  defp pipeline_get_results(ctx, keys, values) do
    keys
    |> Enum.zip(values)
    |> Enum.map(fn {key, value} -> pipeline_get_result(ctx, key, value) end)
  end

  defp pipeline_get_result(_ctx, _key, value) when value != nil, do: {:ok, value}

  defp pipeline_get_result(ctx, key, nil) do
    if pipeline_compound_data_structure_key?(ctx, key) do
      {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    else
      {:ok, nil}
    end
  end

  defp pipeline_compound_data_structure_key?(ctx, key) do
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    list_meta_key = Ferricstore.Store.CompoundKey.list_meta_key(key)

    Ferricstore.Store.Router.compound_get(ctx, key, type_key) != nil or
      Ferricstore.Store.Router.compound_get(ctx, key, list_meta_key) != nil
  end

  # The Coordinator returns raw Dispatcher results (dispatcher-level values).
  # Pipeline callers expect the same format as FerricStore public API calls.
  # This maps Dispatcher results back to the public API format.
  defp normalize_result({:get, _}, {:error, _} = err), do: err
  defp normalize_result({:get, _}, value), do: {:ok, value}

  defp normalize_result({:hget, _, _}, {:error, _} = err), do: err
  defp normalize_result({:hget, _, _}, value), do: {:ok, value}

  defp normalize_result({:del, _}, {:error, _} = err), do: err
  defp normalize_result({:del, _}, _count), do: :ok

  defp normalize_result({:hset, _, _}, {:error, _} = err), do: err
  defp normalize_result({:hset, _, _}, _count), do: :ok

  defp normalize_result({:lpush, _, _}, {:error, _} = err), do: err
  defp normalize_result({:lpush, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:rpush, _, _}, {:error, _} = err), do: err
  defp normalize_result({:rpush, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:sadd, _, _}, {:error, _} = err), do: err
  defp normalize_result({:sadd, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:zadd, _, _}, {:error, _} = err), do: err
  defp normalize_result({:zadd, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:expire, _, _}, {:error, _} = err), do: err
  defp normalize_result({:expire, _, _}, 1), do: {:ok, true}
  defp normalize_result({:expire, _, _}, 0), do: {:ok, false}

  # SET, INCR, INCR_BY already return the correct format from Dispatcher
  defp normalize_result(_, result), do: result

  defp to_resp_command({:set, key, value, opts}) do
    args = [key, value]
    ast_opts = set_ast_options(opts)

    args =
      case Keyword.get(opts, :ttl) do
        nil -> args
        0 -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      case Keyword.get(opts, :ex) do
        nil -> args
        seconds -> args ++ ["EX", Integer.to_string(seconds)]
      end

    args =
      case Keyword.get(opts, :px) do
        nil -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      if Keyword.get(opts, :nx, false), do: args ++ ["NX"], else: args

    args =
      if Keyword.get(opts, :xx, false), do: args ++ ["XX"], else: args

    {"SET", args, {:set, key, value, ast_opts}}
  end

  defp to_resp_command({:get, key}), do: {"GET", [key], {:get, key}}
  defp to_resp_command({:del, key}), do: {"DEL", [key], {:del, [key]}}
  defp to_resp_command({:incr, key}), do: {"INCR", [key], {:incr, key}}

  defp to_resp_command({:incr_by, key, amount}),
    do: {"INCRBY", [key, Integer.to_string(amount)], {:incrby, key, amount}}

  defp to_resp_command({:hset, key, fields}) do
    flat = Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)
    args = [key | flat]
    {"HSET", args, {:hset, args}}
  end

  defp to_resp_command({:hget, key, field}), do: {"HGET", [key, field], {:hget, key, field}}

  defp to_resp_command({:lpush, key, elements}),
    do: {"LPUSH", [key | elements], {:lpush, [key | elements]}}

  defp to_resp_command({:rpush, key, elements}),
    do: {"RPUSH", [key | elements], {:rpush, [key | elements]}}

  defp to_resp_command({:sadd, key, members}),
    do: {"SADD", [key | members], {:sadd, [key | members]}}

  defp to_resp_command({:zadd, key, pairs}) do
    flat =
      Enum.flat_map(pairs, fn {score, member} ->
        [to_string(score), member]
      end)

    args = [key | flat]

    {"ZADD", args,
     {:zadd, key, [], Enum.map(pairs, fn {score, member} -> {score / 1, member} end)}}
  end

  defp to_resp_command({:expire, key, ttl_ms}) do
    {"PEXPIRE", [key, Integer.to_string(ttl_ms)], {:pexpire, key, ttl_ms}}
  end

  defp set_ast_options(opts) do
    []
    |> maybe_add_set_expiry(opts)
    |> maybe_add_set_flag(opts, :nx)
    |> maybe_add_set_flag(opts, :xx)
  end

  defp maybe_add_set_expiry(acc, opts) do
    cond do
      Keyword.get(opts, :ttl) not in [nil, 0] -> [{:px, Keyword.fetch!(opts, :ttl)} | acc]
      Keyword.has_key?(opts, :ex) -> [{:ex, Keyword.fetch!(opts, :ex)} | acc]
      Keyword.has_key?(opts, :px) -> [{:px, Keyword.fetch!(opts, :px)} | acc]
      true -> acc
    end
  end

  defp maybe_add_set_flag(acc, opts, flag) do
    if Keyword.get(opts, flag, false), do: [flag | acc], else: acc
  end
end
