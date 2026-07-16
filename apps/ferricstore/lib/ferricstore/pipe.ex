defmodule FerricStore.Pipe do
  @moduledoc """
  Pipeline accumulator for batching multiple FerricStore commands.

  Used with `FerricStore.pipeline/1` to batch multiple operations into a single
  Raft entry per shard. Commands are accumulated in reverse order and on execute,
  prepared once and dispatched through the Coordinator. Single-shard pipelines
  commit in one Raft round-trip; cross-shard pipelines submit independent shard
  groups concurrently and are not atomic across shards.

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

  alias Ferricstore.Commands.PreparedAccumulatorCommand
  alias Ferricstore.Store.ReadResult

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

  This is called internally by `FerricStore.pipeline/1`. Commands are prepared
  once and dispatched through `Ferricstore.Transaction.Coordinator`, which groups
  them by shard and submits each group as one `{:tx_execute}` Raft entry.
  Single-shard pipelines commit atomically in one Raft round-trip; cross-shard
  pipelines are ordered within each shard but not atomic across shards.

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

        :complex ->
          with {:ok, queue} <- PreparedAccumulatorCommand.prepare_all(ordered),
               raw_results when is_list(raw_results) <-
                 Ferricstore.Transaction.Coordinator.execute_pipeline(queue, nil) do
            ordered
            |> Enum.zip(raw_results)
            |> Enum.map(fn {cmd, raw} -> normalize_result(cmd, raw) end)
          else
            {:error, _reason} = error -> error
          end
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
          :all_sets -> :complex
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
          :all_gets -> :complex
          _ -> :complex
        end

      if new_kind == :complex,
        do: :complex,
        else: classify_batch(rest, new_kind, MapSet.put(written, key), read)
    end
  end

  defp classify_batch(_, _, _, _), do: :complex

  if Mix.env() == :test do
    @doc false
    def __classify_batch_for_test__(commands), do: classify_batch(commands)
  end

  defp execute_batch_sets(ctx, _ordered, kv_pairs) do
    Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)
  end

  defp pipeline_get_results(ctx, keys, values) do
    keys
    |> Enum.zip(values)
    |> Enum.map(fn {key, value} -> pipeline_get_result(ctx, key, value) end)
  end

  defp pipeline_get_result(ctx, key, value) do
    pipeline_get_result_with_lookup(key, value, fn redis_key, compound_key ->
      Ferricstore.Store.Router.compound_get(ctx, redis_key, compound_key)
    end)
  end

  defp pipeline_get_result_with_lookup(
         _key,
         {:error, {:storage_read_failed, _reason}} = failure,
         _compound_get
       ),
       do: ReadResult.command_error(failure)

  defp pipeline_get_result_with_lookup(_key, value, _compound_get) when not is_nil(value),
    do: {:ok, value}

  defp pipeline_get_result_with_lookup(key, nil, compound_get) do
    case pipeline_compound_data_structure_status(key, compound_get) do
      :compound -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
      :plain -> {:ok, nil}
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp pipeline_compound_data_structure_status(key, compound_get) do
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    list_meta_key = Ferricstore.Store.CompoundKey.list_meta_key(key)

    case compound_get.(key, type_key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        case compound_get.(key, list_meta_key) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          nil -> :plain
          _list_meta -> :compound
        end

      _type_marker ->
        :compound
    end
  end

  if Mix.env() == :test do
    @doc false
    def __pipeline_get_result_for_test__(key, value, compound_get)
        when is_function(compound_get, 2) do
      pipeline_get_result_with_lookup(key, value, compound_get)
    end
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
end
