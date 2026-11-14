defmodule Ferricstore.Flow.Query.QueryRowCodecFormatTest do
  # The data format is node-global, so these tests do not run concurrently.
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Digest, Keys, Locator}
  alias Ferricstore.Flow.Query.QueryRowCodec

  setup do
    previous = Digest.format()
    on_exit(fn -> Digest.put_format(previous) end)
    :ok
  end

  test "keeps the persisted QueryRow bytes stable while optimizing construction" do
    # The row binds its state key, whose digest length follows the data format.
    for {format, golden} <- [
          {1, "3215d2c4b601a25ec373a07fa7a0aee0901533700cf5e4ec815d18db9f871a3b"},
          {2, "25645853748400a832d37c4cf42d41d828f2dc1f2db95c339c3031719dccc451"}
        ] do
      :ok = Digest.put_format(format)
      state_key = Keys.state_key("run-1", "tenant-a")
      assert {:ok, encoded} = QueryRowCodec.encode(state_key, record(), locator(), 0)

      assert byte_size(encoded) == 268
      assert Base.encode16(:crypto.hash(:sha256, encoded), case: :lower) == golden
    end
  end

  # Same record and locator as QueryRowCodecTest; the golden hashes depend on them.
  defp record do
    %{
      id: "run-1",
      type: "invoice",
      state: "queued",
      version: 7,
      priority: -2,
      partition_key: "tenant-a",
      created_at_ms: 100,
      updated_at_ms: 200,
      next_run_at_ms: 300,
      lease_deadline_ms: nil,
      attempts: 2,
      run_state: "ready",
      max_active_ms: 10_000,
      state_enter_seq: 42,
      history_max_events: 100_000,
      history_hot_max_events: 1_000,
      parent_flow_id: "parent",
      root_flow_id: "root",
      correlation_id: "correlation",
      attributes: %{"tier" => "gold", "labels" => ["urgent", "finance"]},
      indexed_attributes: ["tier"],
      state_meta: %{
        "created" => %{"source" => "api"},
        "queued" => %{"worker" => "worker-1", "score" => 1.5}
      },
      indexed_state_meta: "worker"
    }
  end

  defp locator do
    Locator.new!(
      flow_id: "run-1",
      kind: :state,
      version: 7,
      raft_index: 99,
      file_id: 3,
      offset: 128,
      value_size: 512,
      frame_size: nil,
      checksum: :binary.copy(<<1>>, 32),
      expire_at_ms: nil,
      segment_generation: 4
    )
  end
end
