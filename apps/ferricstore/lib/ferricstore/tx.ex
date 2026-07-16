defmodule FerricStore.Tx do
  @moduledoc """
  Transaction accumulator for executing multiple FerricStore commands atomically.

  Used with `FerricStore.multi/1` to batch multiple operations. Commands are
  accumulated in reverse order and executed sequentially when the transaction
  completes.

  ## Usage

      FerricStore.multi(fn tx ->
        tx
        |> FerricStore.Tx.set("key1", "val1")
        |> FerricStore.Tx.get("key1")
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

  @doc "Creates a new empty transaction."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds a SET command to the transaction."
  @spec set(t(), binary(), binary(), keyword()) :: t()
  def set(%__MODULE__{} = tx, key, value, opts \\ []) do
    %{tx | commands: [{:set, key, value, opts} | tx.commands]}
  end

  @doc "Adds a GET command to the transaction."
  @spec get(t(), binary()) :: t()
  def get(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:get, key} | tx.commands]}
  end

  @doc "Adds a DEL command to the transaction."
  @spec del(t(), binary()) :: t()
  def del(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:del, key} | tx.commands]}
  end

  @doc "Adds an INCR command to the transaction."
  @spec incr(t(), binary()) :: t()
  def incr(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:incr, key} | tx.commands]}
  end

  @doc "Adds an INCRBY command to the transaction."
  @spec incr_by(t(), binary(), integer()) :: t()
  def incr_by(%__MODULE__{} = tx, key, amount) do
    %{tx | commands: [{:incr_by, key, amount} | tx.commands]}
  end

  @doc "Adds an HSET command to the transaction."
  @spec hset(t(), binary(), map()) :: t()
  def hset(%__MODULE__{} = tx, key, fields) do
    %{tx | commands: [{:hset, key, fields} | tx.commands]}
  end

  @doc "Adds an HGET command to the transaction."
  @spec hget(t(), binary(), binary()) :: t()
  def hget(%__MODULE__{} = tx, key, field) do
    %{tx | commands: [{:hget, key, field} | tx.commands]}
  end

  @doc "Adds an LPUSH command to the transaction."
  @spec lpush(t(), binary(), [binary()]) :: t()
  def lpush(%__MODULE__{} = tx, key, elements) do
    %{tx | commands: [{:lpush, key, elements} | tx.commands]}
  end

  @doc "Adds an RPUSH command to the transaction."
  @spec rpush(t(), binary(), [binary()]) :: t()
  def rpush(%__MODULE__{} = tx, key, elements) do
    %{tx | commands: [{:rpush, key, elements} | tx.commands]}
  end

  @doc "Adds a SADD command to the transaction."
  @spec sadd(t(), binary(), [binary()]) :: t()
  def sadd(%__MODULE__{} = tx, key, members) do
    %{tx | commands: [{:sadd, key, members} | tx.commands]}
  end

  @doc "Adds a ZADD command to the transaction."
  @spec zadd(t(), binary(), [{number(), binary()}]) :: t()
  def zadd(%__MODULE__{} = tx, key, score_member_pairs) do
    %{tx | commands: [{:zadd, key, score_member_pairs} | tx.commands]}
  end

  @doc "Adds an EXPIRE command to the transaction."
  @spec expire(t(), binary(), non_neg_integer()) :: t()
  def expire(%__MODULE__{} = tx, key, ttl_ms) do
    %{tx | commands: [{:expire, key, ttl_ms} | tx.commands]}
  end

  @doc """
  Executes all accumulated transaction commands atomically.

  Groups commands by shard. If all target a single shard, dispatches them
  as a batch to the shard GenServer (atomic, no interleaving). If commands
  span multiple shards, returns a CROSSSLOT error.
  """
  @spec execute(t()) :: [term()] | FerricStore.write_error()
  def execute(%__MODULE__{commands: []}), do: []

  def execute(%__MODULE__{commands: commands}) do
    with :ok <- authorize_commands(commands),
         {:ok, queue} <- commands |> Enum.reverse() |> PreparedAccumulatorCommand.prepare_all() do
      Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
    end
  end

  defp authorize_commands(commands) do
    commands
    |> Enum.map(&elem(&1, 1))
    |> Ferricstore.Flow.InternalKey.authorize_public()
  end
end
