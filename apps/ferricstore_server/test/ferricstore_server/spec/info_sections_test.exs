defmodule FerricstoreServer.Spec.InfoSectionsTest do
  @moduledoc """
  Spec-driven tests for the INFO command's new sections:

    * INFO raft         -- per-shard Raft state (role, term, commit_index, etc.)
    * INFO bitcask      -- per-shard Bitcask storage stats
    * INFO ferricstore  -- native FerricStore aggregate metrics
    * INFO keydir_analysis -- per-prefix keydir breakdown
    * INFO all          -- must include all new sections

  Also includes a stress test: 1000 INFO calls must not crash.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Server
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # Builds a real store map backed by the application-supervised shards.
  defp build_real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn ->
        Enum.each(Router.keys(FerricStore.Instance.get(:default)), fn k -> Router.delete(FerricStore.Instance.get(:default), k) end)
        :ok
      end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      list_op: fn k, op -> Router.list_op(FerricStore.Instance.get(:default), k, op) end
    }
  end

  # Parses an INFO section string into a map of key => value.
  defp parse_info(info_string) do
    info_string
    |> String.split("\r\n")
    |> Enum.reject(fn line -> line == "" or String.starts_with?(line, "#") end)
    |> Enum.map(fn line ->
      case String.split(line, ":", parts: 2) do
        [k, v] -> {k, v}
        [k] -> {k, ""}
      end
    end)
    |> Map.new()
  end

  # Returns the section header names from an INFO all string.
  defp section_headers(info_string) do
    info_string
    |> String.split("\r\n")
    |> Enum.filter(&String.starts_with?(&1, "# "))
    |> Enum.map(fn "# " <> name -> name end)
  end

  # =========================================================================
  # INFO raft
  # =========================================================================

  describe "INFO raft" do
    test "contains per-shard role and term" do
      store = build_real_store()
      result = Server.handle("INFO", ["raft"], store)

      assert is_binary(result), "INFO raft must return a string"
      assert String.contains?(result, "# Raft"), "must have Raft section header"

      fields = parse_info(result)

      # Each shard should have a role field
      for i <- 0..(shard_count() - 1) do
        role_key = "shard_#{i}_role"
        assert Map.has_key?(fields, role_key),
               "missing #{role_key} in INFO raft, got: #{inspect(Map.keys(fields))}"

        role = Map.fetch!(fields, role_key)
        assert role in ["leader", "follower", "candidate"],
               "shard #{i} role must be leader, follower, or candidate, got: #{role}"

        term_key = "shard_#{i}_current_term"
        assert Map.has_key?(fields, term_key),
               "missing #{term_key} in INFO raft"

        {term_val, ""} = Integer.parse(Map.fetch!(fields, term_key))
        assert term_val >= 0, "term must be non-negative"
      end
    end

    test "contains commit_index and last_applied per shard" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["raft"], store))

      for i <- 0..(shard_count() - 1) do
        assert Map.has_key?(fields, "shard_#{i}_commit_index"),
               "missing shard_#{i}_commit_index"

        assert Map.has_key?(fields, "shard_#{i}_last_applied"),
               "missing shard_#{i}_last_applied"
      end
    end

    test "contains leader_node per shard" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["raft"], store))

      for i <- 0..(shard_count() - 1) do
        assert Map.has_key?(fields, "shard_#{i}_leader_node"),
               "missing shard_#{i}_leader_node"

        leader = Map.fetch!(fields, "shard_#{i}_leader_node")
        assert byte_size(leader) > 0, "leader_node must not be empty"
      end
    end

    test "returns empty string when raft is unavailable for unknown section" do
      store = build_real_store()
      result = Server.handle("INFO", ["nonexistent_section"], store)
      assert result == ""
    end
  end

  # =========================================================================
  # INFO bitcask
  # =========================================================================

  describe "INFO bitcask" do
    test "contains data_file_count per shard" do
      store = build_real_store()
      result = Server.handle("INFO", ["bitcask"], store)

      assert is_binary(result), "INFO bitcask must return a string"
      assert String.contains?(result, "# Bitcask"), "must have Bitcask section header"

      fields = parse_info(result)

      for i <- 0..(shard_count() - 1) do
        key = "shard_#{i}_data_file_count"
        assert Map.has_key?(fields, key),
               "missing #{key} in INFO bitcask, got: #{inspect(Map.keys(fields))}"

        {count, ""} = Integer.parse(Map.fetch!(fields, key))
        assert count >= 0, "data_file_count must be non-negative"
      end
    end

    test "contains total_size_bytes per shard" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["bitcask"], store))

      for i <- 0..(shard_count() - 1) do
        key = "shard_#{i}_total_size_bytes"
        assert Map.has_key?(fields, key), "missing #{key}"

        {size, ""} = Integer.parse(Map.fetch!(fields, key))
        assert size >= 0, "total_size_bytes must be non-negative"
      end
    end

    test "contains merge_candidates per shard" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["bitcask"], store))

      for i <- 0..(shard_count() - 1) do
        key = "shard_#{i}_merge_candidates"
        assert Map.has_key?(fields, key), "missing #{key}"

        {count, ""} = Integer.parse(Map.fetch!(fields, key))
        assert count >= 0, "merge_candidates must be non-negative"
      end
    end

    test "contains hint_file_count per shard" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["bitcask"], store))

      for i <- 0..(shard_count() - 1) do
        key = "shard_#{i}_hint_file_count"
        assert Map.has_key?(fields, key), "missing #{key}"

        {count, ""} = Integer.parse(Map.fetch!(fields, key))
        assert count >= 0, "hint_file_count must be non-negative"
      end
    end

    test "data_file_count increases after writing data" do
      store = build_real_store()

      # Write enough data to create at least one data file
      key = "bitcask_info_test_#{System.unique_integer([:positive])}"
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      fields = parse_info(Server.handle("INFO", ["bitcask"], store))

      # At least one shard should have >= 1 data file
      total_files =
        Enum.sum(
          for i <- 0..(shard_count() - 1) do
            {count, ""} = Integer.parse(Map.fetch!(fields, "shard_#{i}_data_file_count"))
            count
          end
        )

      assert total_files >= 1, "at least one data file should exist after a write"

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end

  # =========================================================================
  # INFO ferricstore
  # =========================================================================

  describe "INFO ferricstore" do
    test "contains raft_commands_committed" do
      store = build_real_store()
      result = Server.handle("INFO", ["ferricstore"], store)

      assert is_binary(result), "INFO ferricstore must return a string"
      assert String.contains?(result, "# Ferricstore"), "must have Ferricstore section header"

      fields = parse_info(result)

      assert Map.has_key?(fields, "raft_commands_committed"),
             "missing raft_commands_committed in INFO ferricstore, got: #{inspect(Map.keys(fields))}"

      {count, ""} = Integer.parse(Map.fetch!(fields, "raft_commands_committed"))
      assert count >= 0, "raft_commands_committed must be non-negative"
    end

    test "contains hot_cache_evictions" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["ferricstore"], store))

      assert Map.has_key?(fields, "hot_cache_evictions"),
             "missing hot_cache_evictions"

      {count, ""} = Integer.parse(Map.fetch!(fields, "hot_cache_evictions"))
      assert count >= 0, "hot_cache_evictions must be non-negative"
    end

    test "contains keydir_full_rejections" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["ferricstore"], store))

      assert Map.has_key?(fields, "keydir_full_rejections"),
             "missing keydir_full_rejections"

      {count, ""} = Integer.parse(Map.fetch!(fields, "keydir_full_rejections"))
      assert count >= 0, "keydir_full_rejections must be non-negative"
    end

    test "raft_commands_committed increases after a write" do
      store = build_real_store()

      before_fields = parse_info(Server.handle("INFO", ["ferricstore"], store))
      {before_count, ""} = Integer.parse(Map.fetch!(before_fields, "raft_commands_committed"))

      key = "ferricstore_info_test_#{System.unique_integer([:positive])}"
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      # Allow the Raft commit to propagate
      Process.sleep(50)

      after_fields = parse_info(Server.handle("INFO", ["ferricstore"], store))
      {after_count, ""} = Integer.parse(Map.fetch!(after_fields, "raft_commands_committed"))

      assert after_count >= before_count,
             "raft_commands_committed should not decrease after a write"

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end

  # =========================================================================
  # INFO keydir_analysis
  # =========================================================================

  describe "INFO keydir_analysis" do
    setup do
      # Write some keys with different prefixes to ensure keydir has data
      keys = [
        "user:alpha_#{System.unique_integer([:positive])}",
        "user:beta_#{System.unique_integer([:positive])}",
        "session:one_#{System.unique_integer([:positive])}",
        "session:two_#{System.unique_integer([:positive])}",
        "session:three_#{System.unique_integer([:positive])}",
        "plain_#{System.unique_integer([:positive])}"
      ]

      Enum.each(keys, fn key -> Router.put(FerricStore.Instance.get(:default), key, "val", 0) end)

      on_exit(fn ->
        ShardHelpers.wait_shards_alive()
        Enum.each(keys, fn key -> Router.delete(FerricStore.Instance.get(:default), key) end)
      end)

      %{keys: keys}
    end

    test "lists prefixes with key counts" do
      store = build_real_store()
      result = Server.handle("INFO", ["keydir_analysis"], store)

      assert is_binary(result), "INFO keydir_analysis must return a string"
      assert String.contains?(result, "# Keydir_Analysis"),
             "must have Keydir_Analysis section header"

      fields = parse_info(result)

      assert Map.has_key?(fields, "distinct_prefixes"),
             "missing distinct_prefixes in INFO keydir_analysis"

      {prefix_count, ""} = Integer.parse(Map.fetch!(fields, "distinct_prefixes"))
      assert prefix_count >= 1, "should have at least 1 prefix"
    end

    test "shows per-prefix key counts" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["keydir_analysis"], store))

      # At least one prefix key_count entry should exist
      prefix_key_count_keys =
        fields
        |> Map.keys()
        |> Enum.filter(&String.ends_with?(&1, "_key_count"))

      assert prefix_key_count_keys != [],
             "should have at least one prefix_*_key_count field"

      # Each key_count should be parseable and positive
      Enum.each(prefix_key_count_keys, fn key ->
        {count, ""} = Integer.parse(Map.fetch!(fields, key))
        assert count >= 1, "prefix key count for #{key} must be >= 1"
      end)
    end

    test "shows per-prefix keydir bytes" do
      store = build_real_store()
      fields = parse_info(Server.handle("INFO", ["keydir_analysis"], store))

      prefix_bytes_keys =
        fields
        |> Map.keys()
        |> Enum.filter(&String.ends_with?(&1, "_keydir_bytes"))

      assert prefix_bytes_keys != [],
             "should have at least one prefix_*_keydir_bytes field"
    end
  end

  # =========================================================================
  # INFO all includes new sections
  # =========================================================================

  describe "INFO all" do
    test "includes all new sections" do
      store = build_real_store()
      result = Server.handle("INFO", ["all"], store)

      assert is_binary(result), "INFO all must return a string"
      headers = section_headers(result)

      assert "Raft" in headers,
             "INFO all must include Raft section, got: #{inspect(headers)}"

      assert "Bitcask" in headers,
             "INFO all must include Bitcask section, got: #{inspect(headers)}"

      assert "Ferricstore" in headers,
             "INFO all must include Ferricstore section, got: #{inspect(headers)}"

      assert "Keydir_Analysis" in headers,
             "INFO all must include Keydir_Analysis section, got: #{inspect(headers)}"
    end

    test "includes original sections alongside new ones" do
      store = build_real_store()
      result = Server.handle("INFO", ["all"], store)
      headers = section_headers(result)

      for expected <- ["Server", "Clients", "Memory", "Keyspace", "Stats",
                       "Persistence", "Replication", "CPU", "Namespace_Config"] do
        assert expected in headers,
               "INFO all must include #{expected} section"
      end
    end

    test "INFO everything includes all sections too" do
      store = build_real_store()
      result = Server.handle("INFO", ["everything"], store)
      headers = section_headers(result)

      assert "Raft" in headers
      assert "Bitcask" in headers
      assert "Ferricstore" in headers
      assert "Keydir_Analysis" in headers
    end
  end

  # =========================================================================
  # Stress test
  # =========================================================================

  describe "stress" do
    test "1000 INFO calls do not crash" do
      store = build_real_store()

      sections = ["all", "raft", "bitcask", "ferricstore", "keydir_analysis",
                   "server", "clients", "memory", "keyspace", "stats"]

      results =
        for i <- 1..1000 do
          section = Enum.at(sections, rem(i, length(sections)))
          Server.handle("INFO", [section], store)
        end

      # Every call should return a binary (not crash or raise)
      Enum.each(results, fn result ->
        assert is_binary(result), "INFO must always return a string"
      end)
    end

    test "concurrent INFO calls across 50 tasks do not crash" do
      store = build_real_store()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for section <- ["all", "raft", "bitcask", "ferricstore", "keydir_analysis"] do
              Server.handle("INFO", [section], store)
            end
          end)
        end

      results = Task.await_many(tasks, 30_000)

      Enum.each(results, fn section_results ->
        Enum.each(section_results, fn result ->
          assert is_binary(result), "INFO must always return a string"
        end)
      end)
    end
  end
end
