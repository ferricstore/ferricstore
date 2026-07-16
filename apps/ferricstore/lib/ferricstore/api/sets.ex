defmodule FerricStore.API.Sets do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Commands.Set
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
  Adds one or more members to the set stored at `key`.

  If the key does not exist, a new set is created. Members that already exist
  in the set are ignored. Returns the count of members actually added.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      {:ok, 3}

      iex> FerricStore.sadd("article:42:tags", ["rust", "performance"])
      {:ok, 1}

  """
  @spec sadd(key(), [binary()]) :: {:ok, non_neg_integer()}
  def sadd(key, members) when is_list(members) do
    store = build_compound_store(key)

    case Set.handle_ast({:sadd, [key | members]}, store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Removes one or more members from the set stored at `key`.

  Members that do not exist in the set are ignored. Returns the count of
  members actually removed.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> FerricStore.srem("article:42:tags", ["rust"])
      {:ok, 1}

      iex> FerricStore.srem("article:42:tags", ["nonexistent"])
      {:ok, 0}

  """
  @spec srem(key(), [binary()]) :: {:ok, non_neg_integer()}
  def srem(key, members) when is_list(members) do
    store = build_compound_store(key)

    case Set.handle_ast({:srem, [key | members]}, store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Returns all members of the set stored at `key`.

  Returns `{:ok, []}` if the key does not exist. The order of returned
  members is not guaranteed.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust"])
      iex> {:ok, members} = FerricStore.smembers("article:42:tags")
      iex> Enum.sort(members)
      ["elixir", "rust"]

      iex> FerricStore.smembers("nonexistent")
      {:ok, []}

  """
  @spec smembers(key()) :: {:ok, [binary()]}
  def smembers(key) do
    store = build_compound_store(key)

    case Set.handle_ast({:smembers, key}, store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Checks whether `member` is a member of the set stored at `key`.

  Returns `{:ok, true}` if the member exists, `{:ok, false}` otherwise.
  Returns `{:ok, false}` if the key does not exist.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust"])
      iex> FerricStore.sismember("article:42:tags", "elixir")
      {:ok, true}

      iex> FerricStore.sismember("article:42:tags", "python")
      {:ok, false}

      iex> FerricStore.sismember("nonexistent", "member")
      {:ok, false}

  """
  @spec sismember(key(), binary()) :: {:ok, boolean()} | {:error, binary()}
  def sismember(key, member) do
    store = build_compound_store(key)

    case Set.handle_ast({:sismember, key, member}, store) do
      {:error, _} = err -> err
      result -> {:ok, result == 1}
    end
  end

  @doc """
  Returns the number of members in the set stored at `key` (the set cardinality).

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> FerricStore.scard("article:42:tags")
      {:ok, 3}

      iex> FerricStore.scard("nonexistent")
      {:ok, 0}

  """
  @spec scard(key()) :: {:ok, non_neg_integer()}
  def scard(key) do
    store = build_compound_store(key)

    case Set.handle_ast({:scard, key}, store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted Sets
  # ---------------------------------------------------------------------------

  @doc """
  Returns the membership status of multiple members in the set at `key`.

  Returns a list of 1s and 0s corresponding to each member, in the same order
  as the input list.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> FerricStore.smismember("article:42:tags", ["elixir", "python", "database"])
      {:ok, [1, 0, 1]}

      iex> FerricStore.smismember("nonexistent", ["a", "b"])
      {:ok, [0, 0]}

  """
  @spec smismember(key(), [binary()]) :: {:ok, [0 | 1]}
  def smismember(key, members) when is_list(members) do
    store = build_compound_store(key)

    case Set.handle_ast({:smismember, [key | members]}, store) do
      {:error, _reason} = error -> error
      results -> {:ok, results}
    end
  end

  @doc """
  Returns one or more random members from the set at `key` without removing them.

  Without `count`, returns a single member or `nil` for empty/nonexistent sets.
  With positive `count`, returns up to `count` unique members. With negative
  `count`, returns `abs(count)` members with possible duplicates.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> {:ok, member} = FerricStore.srandmember("article:42:tags")
      iex> member in ["elixir", "rust", "database"]
      true

      iex> {:ok, members} = FerricStore.srandmember("article:42:tags", 2)
      iex> length(members)
      2

      iex> FerricStore.srandmember("nonexistent")
      {:ok, nil}

  """
  @spec srandmember(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil} | write_error()
  def srandmember(key, count \\ nil) do
    store = build_compound_store(key)

    ast = if is_nil(count), do: {:srandmember, key}, else: {:srandmember, key, count}

    ast
    |> Set.handle_ast(store)
    |> wrap_result()
  end

  @doc """
  Removes and returns one or more random members from the set at `key`.

  Without `count`, returns a single member or `nil` for empty/nonexistent sets.
  With `count`, returns a list of up to `count` removed members.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> {:ok, tag} = FerricStore.spop("article:42:tags")
      iex> tag in ["elixir", "rust", "database"]
      true

      iex> {:ok, tags} = FerricStore.spop("article:42:tags", 2)
      iex> length(tags)
      2

      iex> FerricStore.spop("nonexistent")
      {:ok, nil}

  """
  @spec spop(key(), non_neg_integer() | nil) :: {:ok, binary() | [binary()] | nil} | write_error()
  def spop(key, count \\ nil) do
    default_ctx()
    |> Router.spop(key, count)
    |> wrap_result()
  end

  @doc """
  Returns the set difference: members in the first set that are not in any of the other sets.

  Handles cross-shard keys transparently. Returns `{:ok, []}` if the first
  key does not exist.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> {:ok, diff} = FerricStore.sdiff(["frontend:tags", "backend:tags"])
      iex> Enum.sort(diff)
      ["react", "tailwind"]

  """
  @spec sdiff([key()]) :: {:ok, [binary()]} | {:error, binary()}
  def sdiff([]), do: {:ok, []}

  def sdiff(keys) when is_list(keys) do
    result = Set.handle_ast({:sdiff, keys}, build_compound_store(hd(keys)))
    wrap_result(result)
  end

  @doc """
  Returns the set intersection: members common to all given sets.

  Handles cross-shard keys transparently. Returns `{:ok, []}` if any key
  does not exist.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sinter(["frontend:tags", "backend:tags"])
      {:ok, ["elixir"]}

  """
  @spec sinter([key()]) :: {:ok, [binary()]} | {:error, binary()}
  def sinter([]), do: {:ok, []}

  def sinter(keys) when is_list(keys) do
    result = Set.handle_ast({:sinter, keys}, build_compound_store(hd(keys)))
    wrap_result(result)
  end

  @doc """
  Returns the set union: all unique members across all given sets.

  Handles cross-shard keys transparently.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> {:ok, union} = FerricStore.sunion(["frontend:tags", "backend:tags"])
      iex> Enum.sort(union)
      ["elixir", "postgres", "react"]

  """
  @spec sunion([key()]) :: {:ok, [binary()]} | {:error, binary()}
  def sunion([]), do: {:ok, []}

  def sunion(keys) when is_list(keys) do
    result = Set.handle_ast({:sunion, keys}, build_compound_store(hd(keys)))
    wrap_result(result)
  end

  @doc """
  Computes the set difference of the given keys and stores the result in `destination`.

  Any existing value at `destination` is overwritten. Returns the number of
  elements in the resulting set.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sdiffstore("frontend_only:tags", ["frontend:tags", "backend:tags"])
      {:ok, 2}

  """
  @spec sdiffstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sdiffstore(destination, keys) when is_list(keys) do
    result = Set.handle_ast({:sdiffstore, [destination | keys]}, %{})

    wrap_result(result)
  end

  @doc """
  Computes the set intersection of the given keys and stores the result in `destination`.

  Any existing value at `destination` is overwritten. Returns the number of
  elements in the resulting set.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sinterstore("shared:tags", ["frontend:tags", "backend:tags"])
      {:ok, 1}

  """
  @spec sinterstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sinterstore(destination, keys) when is_list(keys) do
    result = Set.handle_ast({:sinterstore, [destination | keys]}, %{})

    wrap_result(result)
  end

  @doc """
  Computes the set union of the given keys and stores the result in `destination`.

  Any existing value at `destination` is overwritten. Returns the number of
  elements in the resulting set.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sunionstore("all:tags", ["frontend:tags", "backend:tags"])
      {:ok, 3}

  """
  @spec sunionstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sunionstore(destination, keys) when is_list(keys) do
    result = Set.handle_ast({:sunionstore, [destination | keys]}, %{})

    wrap_result(result)
  end

  @doc """
  Returns the cardinality of the intersection of all given sets.

  More efficient than `sinter/1` when you only need the count, not the
  actual members.

  ## Options

    * `:limit` - Stop counting after reaching this limit (0 means no limit,
      default: 0). Useful for early termination on large sets.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres", "tailwind"])
      iex> FerricStore.sintercard(["frontend:tags", "backend:tags"])
      {:ok, 2}

      iex> FerricStore.sintercard(["frontend:tags", "backend:tags"], limit: 1)
      {:ok, 1}

  """
  @spec sintercard([key()], keyword()) :: {:ok, non_neg_integer()}
  def sintercard(keys, opts \\ []) when is_list(keys) do
    limit = Keyword.get(opts, :limit, 0)

    store =
      case keys do
        [first | _] -> build_compound_store(first)
        [] -> build_compound_store("")
      end

    result = Set.handle_ast({:sintercard, keys, limit}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Sorted Set extended operations
  # ---------------------------------------------------------------------------
end
