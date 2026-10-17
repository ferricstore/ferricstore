defmodule Ferricstore.Raft.CommandCodecTest do
  # The data format is node-global, so these tests do not run concurrently.
  use ExUnit.Case, async: false

  import Bitwise

  alias Ferricstore.Flow.Digest
  alias Ferricstore.Raft.ApplyContext
  alias Ferricstore.Raft.CommandCodec
  alias Ferricstore.Raft.CommandStamp
  alias Ferricstore.TermCodec

  setup do
    previous = Digest.format()
    on_exit(fn -> Digest.put_format(previous) end)
    :ok
  end

  defp in_format(format, fun) do
    :ok = Digest.put_format(format)
    fun.()
  end

  # A Flow create as the commit path stamps it.
  defp flow_create_command do
    create =
      {:flow_create, "f:{f:DnT1mg2pVjCaL7F-hwZ6zA}:s:order-2000",
       %{
         attributes: %{"region" => "us-east-1", "tenant" => "acme"},
         id: "order-2000",
         type: "invoice.process",
         state: "queued",
         partition_key: "tenant-0",
         payload: %{"amount" => 4200, "currency" => "USD", "order_id" => 123},
         now_ms: 1_791_865_224_929,
         run_at_ms: 1_791_865_224_929,
         policy_ref: %{type: "invoice.process", digest: :crypto.hash(:sha256, "p"), generation: 0},
         policy_reference_captured: true
       }}

    fenced =
      {:flow_policy_fence,
       [{"f:{f}:policy:invoice.process", TermCodec.encode({:flow_policy_v1, 0, %{}}), 0}], create}

    {fenced, %{hlc_ts: {1_791_865_232_761, 2}, wall_time_ms: 1_791_865_232_761}}
    |> ApplyContext.wrap_command(ApplyContext.default())
  end

  test "data format 2 writes Raft commands compactly and they decode to the same command" do
    command = flow_create_command()
    plain = TermCodec.encode(command)

    {:ttb, compact} = in_format(2, fn -> CommandStamp.to_ttb(command) end)

    refute binary_part(compact, 0, 1) == <<131>>
    assert byte_size(compact) * 10 < byte_size(plain) * 6
    assert {:ok, ^command} = CommandStamp.decode_ttb(compact)

    # Format 1 writes exactly what older releases write; both forms decode anywhere.
    assert {:ttb, ^plain} = in_format(1, fn -> CommandStamp.to_ttb(command) end)
    assert {:ok, ^command} = in_format(1, fn -> CommandStamp.decode_ttb(compact) end)
    assert {:ok, ^command} = in_format(2, fn -> CommandStamp.decode_ttb(plain) end)
  end

  test "the default apply context becomes one dictionary reference" do
    encoded = TermCodec.encode(ApplyContext.encode(ApplyContext.default()))

    assert in_format(2, fn -> CommandCodec.encode(encoded) end) == <<0xFC, 1, 2, 0>>
  end

  test "the version 1 dictionary is frozen" do
    # Replicas and replay expand old entries with this dictionary forever. If
    # this fails, the dictionary changed: restore it and add a new version.
    command = {{:put, "key", "value", 0}, %{hlc_ts: {5, 0}, wall_time_ms: 5}}

    assert in_format(2, fn -> CommandCodec.encode(TermCodec.encode(command)) end) ==
             <<252, 1, 104, 2, 104, 4, 1, 129, 109, 0, 0, 0, 3, 107, 101, 121, 109, 0, 0, 0, 5,
               118, 97, 108, 117, 101, 97, 0, 116, 0, 0, 0, 2, 1, 5, 104, 2, 97, 5, 97, 0, 1, 0,
               97, 5>>
  end

  test "any term round-trips exactly, compact or not" do
    :rand.seed(:exsss, {1, 2, 3})

    for _ <- 1..3_000 do
      term = random_term(0)
      external = TermCodec.encode(term)
      encoded = in_format(2, fn -> CommandCodec.encode(external) end)

      assert {:ok, ^external} = CommandCodec.decode(encoded)
      assert encoded == external or byte_size(encoded) < byte_size(external)
    end
  end

  test "terms the codec does not cover, and large commands, stay in the plain form" do
    for term <- [
          {:put, self(), make_ref()},
          {:put, "key", :binary.copy("v", 70_000), 0},
          {:put, "key", Enum.reduce(1..200, :value, &{&2, &1}), 0}
        ] do
      external = TermCodec.encode(term)
      assert in_format(2, fn -> CommandCodec.encode(external) end) == external
    end
  end

  test "a corrupt compact entry is refused, not misread" do
    {:ttb, compact} = in_format(2, fn -> CommandStamp.to_ttb(flow_create_command()) end)

    for corrupt <- [
          binary_part(compact, 0, byte_size(compact) - 1),
          compact <> <<106>>,
          <<0xFC, 99>> <> binary_part(compact, 2, byte_size(compact) - 2),
          <<0xFC, 1, 3, 255, 255>>,
          <<0xFC, 1, 2, 255>>,
          <<0xFC>>
        ] do
      assert {:error, :invalid_preencoded_command} = CommandStamp.decode_ttb(corrupt)
    end
  end

  test "the default instance writes compact log entries that read back as its commands" do
    id = "command-codec-#{System.unique_integer([:positive])}"
    type = id <> "-type"
    partition = "tenant-a"

    assert :ok = FerricStore.set(id, "value")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: partition,
               worker: "codec-worker",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: partition,
               now_ms: 1_002
             )

    entries =
      for shard_index <- 0..(FerricStore.Instance.get(:default).shard_count - 1),
          {:ttb, bytes} <- log_commands(shard_index),
          {:ok, command} = CommandStamp.decode_ttb(bytes),
          :binary.match(:erlang.term_to_binary(command), id) != :nomatch,
          do: {bytes, command}

    # The SET, the create, the claim (it names the type) and the complete.
    assert length(entries) >= 4
    assert Enum.all?(entries, fn {<<first, _::binary>>, _command} -> first == 0xFC end)
    assert Enum.any?(entries, fn {_bytes, command} -> inspect(command) =~ ":flow_complete" end)
  end

  # Reads a shard's Raft log by index up to its last appended entry, as WARaft
  # does. The default instance keeps appending in the background (expiry and
  # schedules left by earlier tests), and a scan of the segment files can reach
  # a record whose append is still in flight: on macOS CI runners that is a CRC
  # mismatch at the tail.
  defp log_commands(shard_index) do
    log_module = :ferricstore_waraft_spike_segment_log
    table = :ferricstore_waraft_backend
    partition = shard_index + 1

    log =
      {:raft_log, :wa_raft_log.registered_name(table, partition), table, table, partition,
       log_module}

    for index <- log_module.first_index(log)..log_module.last_index(log)//1,
        {:ok, {_term, {_ref, command}}} <- [log_module.get(log, index)],
        do: command
  end

  defp random_term(depth) do
    leaf? = depth >= 4 or :rand.uniform(3) == 1

    if leaf?, do: random_leaf(), else: random_container(depth + 1)
  end

  defp random_leaf do
    case :rand.uniform(12) do
      1 -> Enum.random([:put, :flow_create, :hlc_ts, :wall_time_ms, true, false, nil, :ok])
      2 -> String.to_atom("atom_#{:rand.uniform(50)}")
      3 -> :rand.uniform(255) - 1
      4 -> :rand.uniform(1 <<< 31) - (1 <<< 30)
      5 -> (:rand.uniform(1 <<< 60) + (1 <<< 40)) * Enum.random([1, -1])
      6 -> :rand.uniform(1 <<< 300)
      7 -> :rand.uniform() * 1.0e10 - 5.0e9
      8 -> :crypto.strong_rand_bytes(:rand.uniform(40) - 1)
      9 -> <<:rand.uniform(255)::size(:rand.uniform(7))>>
      10 -> Enum.map(1..:rand.uniform(5), fn _ -> :rand.uniform(255) end)
      11 -> []
      12 -> "f:{f:#{:rand.uniform(1000)}}:s:id"
    end
  end

  defp random_container(depth) do
    items = fn n -> Enum.map(1..n, fn _ -> random_term(depth) end) end

    case :rand.uniform(6) do
      1 -> List.to_tuple(items.(:rand.uniform(6)))
      2 -> List.to_tuple(Enum.map(1..(256 + :rand.uniform(4)), fn _ -> random_leaf() end))
      3 -> items.(:rand.uniform(6))
      4 -> items.(:rand.uniform(3)) ++ random_leaf()
      5 -> Map.new(items.(:rand.uniform(5)), &{&1, random_term(depth)})
      6 -> Map.new(1..(33 + :rand.uniform(5)), &{Enum.random([&1, :put, "k#{&1}"]), &1})
    end
  end
end
