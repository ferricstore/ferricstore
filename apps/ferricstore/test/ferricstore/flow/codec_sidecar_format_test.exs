defmodule Ferricstore.Flow.CodecSidecarFormatTest do
  # The data format is node-global, so these tests do not run concurrently.
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Codec.Support
  alias Ferricstore.Flow.Digest

  setup do
    previous = Digest.format()
    on_exit(fn -> Digest.put_format(previous) end)
    :ok
  end

  defp in_format(format, fun) do
    :ok = Digest.put_format(format)
    fun.()
  end

  # A record that fills every part of the sidecar.
  defp full_record do
    %{
      id: "flow-sidecar",
      type: "email",
      state: "running",
      version: 3,
      attempts: 1,
      fencing_token: 7,
      created_at_ms: 100,
      updated_at_ms: 110,
      next_run_at_ms: 2_000,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: 100,
      history_max_events: 1_000,
      retention_ttl_ms: 86_400_000,
      max_active_ms: 300_000,
      terminal_retention_until_ms: nil,
      partition_key: "tenant-a",
      payload_ref: "f:{f:AAAAAAAAAAAAAAAAAAAAAA}:v:p:flow-sidecar:1",
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "flow-sidecar",
      correlation_id: "corr",
      result_ref: nil,
      error_ref: nil,
      lease_owner: "worker-1",
      lease_token: "lease-1",
      lease_deadline_ms: 2_000,
      run_state: "running",
      child_groups: %{
        "batch" => %{
          "children" => %{"child-1" => "running", "child-2" => "completed"},
          "child_partitions" => %{"child-1" => "tenant-a"}
        }
      },
      value_refs: %{
        "doc" => %{ref: "f:{f:AAAAAAAAAAAAAAAAAAAAAA}:v:n:flow-sidecar:doc:1", version: 1}
      },
      attributes: %{"region" => "us-east-1", "tenant" => "acme", "tier" => "gold"},
      indexed_attributes: ["region"],
      state_meta: %{"running" => %{"version" => 2, "owner" => "risk"}},
      indexed_state_meta: "version",
      incarnation: 17_179_869_184,
      state_enter_seq: 17_179_869_191,
      governance_limit: %{
        scope: "flow-running:test",
        shard_id: 3,
        enforcement: :strict_global,
        reservation_id: "flr1:1:batch:1"
      }
    }
  end

  test "data format 2 stores the record sidecar as a compact term that decodes like JSON" do
    record = full_record()
    json = in_format(1, fn -> Flow.encode_record(record) end)
    term = in_format(2, fn -> Flow.encode_record(record) end)

    assert json =~ "__state_enter_seq__"
    refute term =~ "__state_enter_seq__"
    assert byte_size(term) <= byte_size(json) - 40

    in_format(2, fn ->
      assert Flow.encode_record_elixir(record) == term
      assert Flow.decode_record(term) == Flow.decode_record(json)
      assert Flow.decode_record_meta(term) == Flow.decode_record_meta(json)
    end)

    # A format 1 node keeps writing JSON, which older releases read, and reads both.
    in_format(1, fn ->
      assert Flow.encode_record_elixir(record) == json
      assert Flow.decode_record(term) == Flow.decode_record(json)
    end)
  end

  test "an empty sidecar keeps the form native claims recognize" do
    record = %{full_record() | child_groups: %{}, governance_limit: nil}

    record =
      Map.drop(record, [
        :value_refs,
        :attributes,
        :indexed_attributes,
        :state_meta,
        :indexed_state_meta,
        :incarnation,
        :state_enter_seq,
        :governance_limit
      ])

    assert in_format(1, fn -> Flow.encode_record(record) end) ==
             in_format(2, fn -> Flow.encode_record(record) end)
  end

  test "term sidecars decode to exactly what the JSON form decodes to" do
    samples = [
      %{"__attributes__" => %{"a" => "b"}, "__incarnation__" => 1},
      %{"group" => %{"children" => %{"c" => "running"}}},
      %{"atoms" => :pending, "atom_key" => %{status: :open}, "bools" => [true, false, nil]},
      %{"floats" => [1.5, -0.0, 1.0e300, 2.0], "big" => 2 ** 70, "negative" => -12},
      %{"unicode" => "naïve 🚀", "nested" => [%{"x" => [1, [2, %{"y" => "z"}]]}]},
      %{1 => "integer key", "__state_enter_seq__" => 0}
    ]

    in_format(2, fn ->
      for groups <- samples do
        {:ok, decoded, ""} =
          groups
          |> Support.encode_child_groups()
          |> IO.iodata_to_binary()
          |> Support.decode_child_groups()

        assert decoded == groups |> Jason.encode!() |> Jason.decode!()
      end
    end)
  end

  test "values JSON cannot encode fail the same way in both formats" do
    for value <- [{:tuple, 1}, <<255, 254>>] do
      [format_1, format_2] =
        for format <- [1, 2] do
          in_format(format, fn ->
            try do
              Support.encode_child_groups(%{"x" => value})
              :encoded
            rescue
              error -> error.__struct__
            end
          end)
        end

      assert format_1 != :encoded
      assert format_2 == format_1
    end
  end

  test "decoding a term sidecar costs well under half the reductions of JSON" do
    record = full_record()
    json = in_format(1, fn -> Flow.encode_record(record) end)
    term = in_format(2, fn -> Flow.encode_record(record) end)

    reductions = fn encoded ->
      {:reductions, before} = Process.info(self(), :reductions)
      for _ <- 1..200, do: Flow.decode_record(encoded)
      {:reductions, after_decode} = Process.info(self(), :reductions)
      after_decode - before
    end

    json_cost = reductions.(json)
    term_cost = reductions.(term)
    assert term_cost * 2 < json_cost, "term #{term_cost} vs JSON #{json_cost} reductions"
  end
end
