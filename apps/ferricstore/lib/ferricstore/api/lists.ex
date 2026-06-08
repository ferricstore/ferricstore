defmodule FerricStore.API.Lists do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Store.Router

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Pushes one or more elements to the left (head) of the list stored at `key`.

  If the key does not exist, a new list is created. Elements are inserted
  left-to-right, so the last element in the list ends up as the leftmost
  element (matching Redis LPUSH semantics).

  ## Examples

      iex> FerricStore.lpush("tasks:queue", ["send_email"])
      {:ok, 1}

      iex> FerricStore.lpush("tasks:queue", ["generate_report", "resize_image"])
      {:ok, 3}

  """
  @spec lpush(key(), [binary()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def lpush(key, elements) when is_list(elements) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lpush, elements})
    |> wrap_result()
  end

  @doc """
  Pushes one or more elements to the right (tail) of the list stored at `key`.

  If the key does not exist, a new list is created.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["send_email"])
      {:ok, 1}

      iex> FerricStore.rpush("tasks:queue", ["generate_report", "resize_image"])
      {:ok, 3}

  """
  @spec rpush(key(), [binary()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def rpush(key, elements) when is_list(elements) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:rpush, elements})
    |> wrap_result()
  end

  @doc """
  Pops one or more elements from the left (head) of the list stored at `key`.

  When `count` is 1 (the default), returns a single element. When `count` is
  greater than 1, returns a list of elements. Returns `{:ok, nil}` if the key
  does not exist or the list is empty.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lpop("tasks:queue")
      {:ok, "task_a"}

      iex> FerricStore.lpop("tasks:queue", 2)
      {:ok, ["task_b", "task_c"]}

      iex> FerricStore.lpop("empty_queue")
      {:ok, nil}

  """
  @spec lpop(key(), pos_integer()) :: {:ok, binary() | [binary()] | nil} | {:error, binary()}
  def lpop(key, count \\ 1) when is_integer(count) and count >= 1 do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lpop, count})
    |> wrap_result()
  end

  @doc """
  Pops one or more elements from the right (tail) of the list stored at `key`.

  When `count` is 1 (the default), returns a single element. When `count` is
  greater than 1, returns a list of elements. Returns `{:ok, nil}` if the key
  does not exist or the list is empty.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.rpop("tasks:queue")
      {:ok, "task_c"}

      iex> FerricStore.rpop("tasks:queue", 2)
      {:ok, ["task_b", "task_a"]}

      iex> FerricStore.rpop("empty_queue")
      {:ok, nil}

  """
  @spec rpop(key(), pos_integer()) :: {:ok, binary() | [binary()] | nil} | {:error, binary()}
  def rpop(key, count \\ 1) when is_integer(count) and count >= 1 do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:rpop, count})
    |> wrap_result()
  end

  @doc """
  Returns elements from the list stored at `key` within the range `start..stop`.

  Both `start` and `stop` are zero-based, inclusive indices. Negative indices
  count from the end of the list (-1 is the last element).

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lrange("tasks:queue", 0, -1)
      {:ok, ["task_a", "task_b", "task_c"]}

      iex> FerricStore.lrange("tasks:queue", 1, 1)
      {:ok, ["task_b"]}

      iex> FerricStore.lrange("nonexistent", 0, -1)
      {:ok, []}

  """
  @spec lrange(key(), integer(), integer()) :: {:ok, [binary()]} | {:error, binary()}
  def lrange(key, start, stop) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lrange, start, stop})
    |> wrap_result()
  end

  @doc """
  Returns the length of the list stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.llen("tasks:queue")
      {:ok, 3}

      iex> FerricStore.llen("nonexistent")
      {:ok, 0}

  """
  @spec llen(key()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def llen(key) do
    ctx = default_ctx()

    Router.list_op(ctx, key, :llen)
    |> wrap_result()
  end

  # ---------------------------------------------------------------------------
  # Sets
  # ---------------------------------------------------------------------------

  @doc """
  Returns the element at `index` in the list stored at `key`.

  Negative indices count from the end (-1 is the last element). Returns
  `{:ok, nil}` for out-of-range indices or nonexistent keys.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lindex("tasks:queue", 0)
      {:ok, "task_a"}

      iex> FerricStore.lindex("tasks:queue", -1)
      {:ok, "task_c"}

      iex> FerricStore.lindex("tasks:queue", 99)
      {:ok, nil}

  """
  @spec lindex(key(), integer()) :: {:ok, binary() | nil} | {:error, binary()}
  def lindex(key, index) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lindex, index})
    |> wrap_result()
  end

  @doc """
  Sets the element at `index` in the list stored at `key`.

  Returns `:ok` on success, or `{:error, reason}` if the index is out of range
  or the key does not exist.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lset("tasks:queue", 1, "task_b_updated")
      :ok

      iex> FerricStore.lset("tasks:queue", 99, "value")
      {:error, "ERR index out of range"}

  """
  @spec lset(key(), integer(), binary()) :: :ok | {:error, binary()}
  def lset(key, index, element) do
    ctx = default_ctx()

    result =
      Router.list_op(ctx, key, {:lset, index, element})

    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Removes occurrences of `element` from the list at `key`.

  The `count` argument controls the direction and number of removals:

    * `count > 0` - Remove up to `count` occurrences scanning from head to tail.
    * `count < 0` - Remove up to `abs(count)` occurrences scanning from tail to head.
    * `count == 0` - Remove all occurrences.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["retry", "send", "retry", "retry"])
      iex> FerricStore.lrem("tasks:queue", 0, "retry")
      {:ok, 3}

      iex> FerricStore.rpush("tasks:queue", ["a", "b", "a"])
      iex> FerricStore.lrem("tasks:queue", 1, "a")
      {:ok, 1}

  """
  @spec lrem(key(), integer(), binary()) :: {:ok, non_neg_integer()}
  def lrem(key, count, element) do
    ctx = default_ctx()

    case Router.list_op(ctx, key, {:lrem, count, element}) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Inserts `element` before or after `pivot` in the list at `key`.

  Returns `{:ok, new_length}` if the pivot was found, or `{:ok, -1}` if the
  pivot was not found. Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.linsert("tasks:queue", :before, "task_b", "task_new")
      {:ok, 4}

      iex> FerricStore.linsert("tasks:queue", :after, "task_c", "task_last")
      {:ok, 5}

      iex> FerricStore.linsert("tasks:queue", :before, "nonexistent", "x")
      {:ok, -1}

  """
  @spec linsert(key(), :before | :after, binary(), binary()) :: {:ok, integer()}
  def linsert(key, direction, pivot, element) when direction in [:before, :after] do
    ctx = default_ctx()

    case Router.list_op(ctx, key, {:linsert, direction, pivot, element}) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Atomically moves an element from one list to another.

  Pops from `from_dir` of `source` and pushes to `to_dir` of `destination`.
  Returns `{:ok, nil}` if the source list is empty or does not exist.

  ## Examples

      iex> FerricStore.rpush("inbox", ["msg_a", "msg_b"])
      iex> FerricStore.lmove("inbox", "processing", :left, :right)
      {:ok, "msg_a"}

      iex> FerricStore.lmove("empty_list", "dst", :left, :right)
      {:ok, nil}

  """
  @spec lmove(key(), key(), :left | :right, :left | :right) :: {:ok, binary() | nil}
  def lmove(source, destination, from_dir, to_dir)
      when from_dir in [:left, :right] and to_dir in [:left, :right] do
    Ferricstore.Commands.List.handle_ast({:lmove, source, destination, from_dir, to_dir}, %{})
    |> wrap_result()
  end

  @doc """
  Finds the position of `element` in the list at `key`.

  Returns the zero-based index of the first match, or `{:ok, nil}` if not
  found. When `:count` is specified, returns a list of indices.

  ## Options

    * `:rank` - Skip the first N-1 matches and return starting from the Nth
      (default: 1). Negative rank searches from tail.
    * `:count` - Return up to N positions. 0 means all. When given, always
      returns a list.
    * `:maxlen` - Limit scan to the first N elements (default: 0, no limit).

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["retry", "send", "retry", "process"])
      iex> FerricStore.lpos("tasks:queue", "retry")
      {:ok, 0}

      iex> FerricStore.lpos("tasks:queue", "retry", count: 0)
      {:ok, [0, 2]}

      iex> FerricStore.lpos("tasks:queue", "missing")
      {:ok, nil}

  """
  @spec lpos(key(), binary(), keyword()) :: {:ok, integer() | [integer()] | nil}
  def lpos(key, element, opts \\ []) do
    ctx = default_ctx()
    rank = Keyword.get(opts, :rank, 1)
    count = Keyword.get(opts, :count)
    maxlen = Keyword.get(opts, :maxlen, 0)

    case Router.list_op(ctx, key, {:lpos, element, rank, count, maxlen}) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Set extended operations
  # ---------------------------------------------------------------------------
end
