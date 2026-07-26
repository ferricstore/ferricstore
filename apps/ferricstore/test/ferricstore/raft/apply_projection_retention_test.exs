defmodule Ferricstore.Raft.ApplyProjectionRetentionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.WARaftStorage.ApplyProjectionRetention

  test "keeps small exact retention sets in memory and orders batches deterministically" do
    root = tmp_root("memory")
    {:ok, retention} = ApplyProjectionRetention.open(root, 10)

    {:ok, retention} =
      ApplyProjectionRetention.put_many(retention, [
        {{2, "b"}, {"b", "value-b", 0}},
        {{1, "z"}, {"z", "value-z", 20}},
        {{1, "a"}, {"a", "value-a", 10}},
        {{2, "b"}, {"b", "value-b", 0}}
      ])

    assert {:ok, retention} = ApplyProjectionRetention.finish(retention)
    assert ApplyProjectionRetention.mode(retention) == :memory

    assert {:ok,
            [
              {{:raft_log_pos, 1, 0}, [{"a", "value-a", 10}, {"z", "value-z", 20}]},
              {{:raft_log_pos, 2, 0}, [{"b", "value-b", 0}]}
            ]} = ApplyProjectionRetention.memory_batches(retention)

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "rejects conflicting claims for one physical projection reference" do
    root = tmp_root("memory-conflict")
    {:ok, retention} = ApplyProjectionRetention.open(root, 10)

    {:ok, retention} =
      ApplyProjectionRetention.put(retention, {{1, "a"}, {"a", "first", 0}})

    assert {:error,
            {:conflicting_apply_projection_retention, {1, "a"}, {"a", "first", 0},
             {"a", "second", 0}}} =
             ApplyProjectionRetention.put(
               retention,
               {{1, "a"}, {"a", "second", 0}}
             )

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "spills above the memory ceiling and pages exact batches from disk" do
    root = tmp_root("disk")

    {:ok, retention} =
      ApplyProjectionRetention.open(root, 10,
        max_memory_entries: 1,
        max_memory_bytes: 64,
        flush_items: 1,
        flush_bytes: 64
      )

    {:ok, retention} =
      ApplyProjectionRetention.put_many(retention, [
        {{3, "c"}, {"c", "value-c", 30}},
        {{1, "b"}, {"b", "value-b", 20}},
        {{1, "a"}, {"a", "value-a", 10}},
        {{2, "d"}, {"d", "value-d", 40}}
      ])

    assert {:ok, retention} = ApplyProjectionRetention.finish(retention)
    assert ApplyProjectionRetention.mode(retention) == :disk
    assert ApplyProjectionRetention.memory_entry_count(retention) == 0

    assert {:ok, first, cursor, false} =
             ApplyProjectionRetention.page(retention, "", 2, 1_024)

    assert first == [
             {{:raft_log_pos, 1, 0},
              [
                {"a", "value-a", 10},
                {"b", "value-b", 20}
              ]}
           ]

    assert {:ok, second, _cursor, true} =
             ApplyProjectionRetention.page(retention, cursor, 2, 1_024)

    assert second == [
             {{:raft_log_pos, 2, 0}, [{"d", "value-d", 40}]},
             {{:raft_log_pos, 3, 0}, [{"c", "value-c", 30}]}
           ]

    path = ApplyProjectionRetention.path(retention)
    assert File.dir?(path)
    assert :ok = ApplyProjectionRetention.cleanup(retention)
    refute File.exists?(path)
  end

  test "preserves an empty retained value through the disk codec" do
    root = tmp_root("empty-value")

    {:ok, retention} =
      ApplyProjectionRetention.open(root, 10,
        max_memory_entries: 0,
        max_memory_bytes: 0,
        flush_items: 1,
        flush_bytes: 1
      )

    assert {:ok, retention} =
             ApplyProjectionRetention.put(retention, {{1, "empty"}, {"empty", "", 0}})

    assert {:ok, retention} = ApplyProjectionRetention.finish(retention)

    assert {:ok, [{{:raft_log_pos, 1, 0}, [{"empty", "", 0}]}], _cursor, true} =
             ApplyProjectionRetention.page(retention, "", 1, 1_024)

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "does not split one Raft index across retention pages" do
    root = tmp_root("index-boundary")

    {:ok, retention} =
      ApplyProjectionRetention.open(root, 10,
        max_memory_entries: 0,
        max_memory_bytes: 0,
        flush_items: 1,
        flush_bytes: 1
      )

    {:ok, retention} =
      ApplyProjectionRetention.put_many(retention, [
        {{1, "a"}, {"a", "value-a", 0}},
        {{1, "b"}, {"b", "value-b", 0}},
        {{1, "c"}, {"c", "value-c", 0}},
        {{2, "d"}, {"d", "value-d", 0}}
      ])

    assert {:ok, retention} = ApplyProjectionRetention.finish(retention)

    assert {:ok,
            [
              {{:raft_log_pos, 1, 0},
               [
                 {"a", "value-a", 0},
                 {"b", "value-b", 0},
                 {"c", "value-c", 0}
               ]}
            ], cursor, false} = ApplyProjectionRetention.page(retention, "", 2, 1_024)

    assert {:ok, [{{:raft_log_pos, 2, 0}, [{"d", "value-d", 0}]}], _cursor, true} =
             ApplyProjectionRetention.page(retention, cursor, 2, 1_024)

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "allows one atomic Raft index to exceed the page byte target" do
    root = tmp_root("atomic-page-overflow")

    {:ok, retention} =
      ApplyProjectionRetention.open(root, 10,
        max_memory_entries: 0,
        max_memory_bytes: 0,
        flush_items: 1,
        flush_bytes: 1,
        max_atomic_page_bytes: 1_024
      )

    value = :binary.copy("v", 96)

    {:ok, retention} =
      ApplyProjectionRetention.put_many(retention, [
        {{1, "a"}, {"a", value, 0}},
        {{1, "b"}, {"b", value, 0}},
        {{2, "c"}, {"c", "next", 0}}
      ])

    assert {:ok, retention} = ApplyProjectionRetention.finish(retention)

    assert {:ok,
            [
              {{:raft_log_pos, 1, 0}, [{"a", ^value, 0}, {"b", ^value, 0}]}
            ], cursor, false} = ApplyProjectionRetention.page(retention, "", 2, 64)

    assert {:ok, [{{:raft_log_pos, 2, 0}, [{"c", "next", 0}]}], _cursor, true} =
             ApplyProjectionRetention.page(retention, cursor, 2, 64)

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "rejects an atomic Raft index above the hard byte ceiling" do
    root = tmp_root("atomic-page-hard-limit")

    {:ok, retention} =
      ApplyProjectionRetention.open(root, 10,
        max_memory_entries: 0,
        max_memory_bytes: 0,
        flush_items: 1,
        flush_bytes: 1,
        max_atomic_page_bytes: 128
      )

    value = :binary.copy("v", 96)

    assert {:ok, retention} =
             ApplyProjectionRetention.put(retention, {{1, "a"}, {"a", value, 0}})

    assert {:ok, retention} = ApplyProjectionRetention.finish(retention)

    assert {:error, :apply_projection_retention_page_too_large} =
             ApplyProjectionRetention.page(retention, "", 1, 64)

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "detects a conflicting duplicate after the first claim has flushed" do
    root = tmp_root("disk-conflict")

    {:ok, retention} =
      ApplyProjectionRetention.open(root, 10,
        max_memory_entries: 0,
        max_memory_bytes: 0,
        flush_items: 1,
        flush_bytes: 1
      )

    {:ok, retention} =
      ApplyProjectionRetention.put(retention, {{1, "a"}, {"a", "first", 0}})

    assert {:error,
            {:conflicting_apply_projection_retention, {1, "a"}, {"a", "first", 0},
             {"a", "second", 0}}} =
             ApplyProjectionRetention.put(
               retention,
               {{1, "a"}, {"a", "second", 0}}
             )

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  test "rejects malformed or out-of-range retained entries before storage" do
    root = tmp_root("invalid")
    {:ok, retention} = ApplyProjectionRetention.open(root, 3)

    assert {:error, :invalid_apply_projection_retention_entry} =
             ApplyProjectionRetention.put(retention, {{3, "a"}, {"a", "value", 0}})

    assert {:error, :invalid_apply_projection_retention_entry} =
             ApplyProjectionRetention.put(retention, {{1, "a"}, {"other", "value", 0}})

    assert {:error, :invalid_apply_projection_retention_entry} =
             ApplyProjectionRetention.put(retention, :invalid)

    assert :ok = ApplyProjectionRetention.cleanup(retention)
  end

  defp tmp_root(suffix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "apply-projection-retention-#{suffix}-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    root
  end
end
