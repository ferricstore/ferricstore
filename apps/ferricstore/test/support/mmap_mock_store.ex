defmodule Ferricstore.Test.ProbMockStore do
  @moduledoc """
  Creates a mock store with stateless NIF-backed callbacks for
  probabilistic data structures (Bloom, Cuckoo, CMS, TopK).

  Each call to `make_cuckoo/0`, `make_cms/0`, or `make_bloom/0` returns
  a store map with a `prob_dir` callback and registry callbacks.
  """

  @doc """
  Creates a mock store with a cuckoo_registry backed by an Agent and temp dir.
  """
  @spec make_cuckoo() :: map()
  def make_cuckoo do
    tmp_dir = make_tmp_dir("cuckoo")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    registry = %{
      get: fn key ->
        Agent.get(pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(tmp_dir, "#{safe}.cuckoo")
      end,
      dir: tmp_dir
    }

    # Note: exists? is intentionally omitted so the handler falls back
    # to File.exists? on the base64-encoded path.
    %{
      cuckoo_registry: registry,
      prob_dir: fn -> tmp_dir end
    }
  end

  @doc """
  Creates a mock store with a cms_registry backed by an Agent and temp dir.
  """
  @spec make_cms() :: map()
  def make_cms do
    tmp_dir = make_tmp_dir("cms")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    registry = %{
      get: fn key ->
        Agent.get(pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(tmp_dir, "#{safe}.cms")
      end,
      dir: tmp_dir
    }

    get_fn = fn key ->
      Agent.get(pid, fn state ->
        case Map.get(state, key) do
          {_resource, _meta} -> :exists
          nil -> nil
        end
      end)
    end

    # Note: exists? is intentionally omitted so the handler falls back
    # to File.exists? on the base64-encoded path, which works with the
    # stateless pread/pwrite NIF architecture.
    %{
      cms_registry: registry,
      get: get_fn,
      prob_dir: fn -> tmp_dir end
    }
  end

  @doc """
  Creates a mock store with a bloom_registry backed by an Agent and temp dir.
  """
  @spec make_bloom() :: map()
  def make_bloom do
    tmp_dir = make_tmp_dir("bloom")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    registry = %{
      get: fn key ->
        Agent.get(pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(tmp_dir, "#{safe}.bloom")
      end,
      dir: tmp_dir
    }

    # Note: exists? is intentionally omitted so the handler falls back
    # to File.exists? on the base64-encoded path.
    %{
      bloom_registry: registry,
      prob_dir: fn -> tmp_dir end
    }
  end

  defp make_tmp_dir(prefix) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_test_#{prefix}_#{:os.getpid()}_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    dir
  end
end

# Backward-compatible alias for the old name
defmodule Ferricstore.Test.MmapMockStore do
  @moduledoc false
  defdelegate make_cuckoo(), to: Ferricstore.Test.ProbMockStore
  defdelegate make_cms(), to: Ferricstore.Test.ProbMockStore
  defdelegate make_bloom(), to: Ferricstore.Test.ProbMockStore
end
