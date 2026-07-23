defmodule Ferricstore.Raft.BlobCommandTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey}

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_blob_command_#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 1)
    on_exit(fn -> File.rm_rf!(root) end)

    ctx = %{
      data_dir: root,
      blob_side_channel_threshold_bytes: 128
    }

    %{ctx: ctx, root: root}
  end

  test "prepares large single put as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    payload = :binary.copy("P", 1024)

    assert {:ok, {:put_blob_ref, "k", encoded_ref, 0}} =
             BlobCommand.prepare(ctx, 0, {:put, "k", payload, 0}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "protected prepare keeps pre-apply blob refs out of GC", %{ctx: ctx, root: root} do
    Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)
    on_exit(fn -> Process.delete(:ferricstore_blob_store_segment_gc_grace_ms) end)

    payload = :binary.copy("G", 1024)

    assert {:ok, {:put_blob_ref, "k", encoded_ref, 0}, protection} =
             BlobCommand.prepare_protected(ctx, 0, {:put, "k", payload, 0}, single_member?: true)

    assert protection != nil
    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, {path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)
    assert File.exists?(path)

    assert {:ok, %{deleted_files: 0}} = BlobStore.sweep_unreferenced(root, 0, [])
    assert File.exists?(path)

    assert :ok = BlobStore.unprotect(protection)
    assert {:ok, %{deleted_files: 1}} = BlobStore.sweep_unreferenced(root, 0, [])
    refute File.exists?(path)
  end

  test "protected prepare expires abandoned pre-apply refs", %{ctx: ctx, root: root} do
    Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)
    Process.put(:ferricstore_blob_store_protection_ttl_ms, 0)

    on_exit(fn ->
      Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
      Process.delete(:ferricstore_blob_store_protection_ttl_ms)
    end)

    payload = :binary.copy("E", 1024)

    assert {:ok, {:put_blob_ref, "k", encoded_ref, 0}, protection} =
             BlobCommand.prepare_protected(ctx, 0, {:put, "k", payload, 0}, single_member?: true)

    assert protection != nil
    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, {path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)
    assert File.exists?(path)

    assert {:ok, %{deleted_files: 1}} = BlobStore.sweep_unreferenced(root, 0, [])
    refute File.exists?(path)
  end

  test "prepares large conditional set as a pre-externalized blob ref", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("S", 1024)
    opts = %{nx: true, xx: false, get: false, keepttl: false}

    assert {:ok, {:set_blob_ref, "k", encoded_ref, 0, ^opts}} =
             BlobCommand.prepare(ctx, 0, {:set, "k", payload, 0, opts}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares large getset as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    payload = :binary.copy("T", 1024)

    assert {:ok, {:getset_blob_ref, "k", encoded_ref}} =
             BlobCommand.prepare(ctx, 0, {:getset, "k", payload}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares large append as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    suffix = :binary.copy("A", 1024)

    assert {:ok, {:append_blob_ref, "k", encoded_ref}} =
             BlobCommand.prepare(ctx, 0, {:append, "k", suffix}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^suffix} = BlobStore.get(root, 0, ref)
  end

  test "prepares large setrange as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    patch = :binary.copy("R", 1024)

    assert {:ok, {:setrange_blob_ref, "k", 2, encoded_ref}} =
             BlobCommand.prepare(ctx, 0, {:setrange, "k", 2, patch}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^patch} = BlobStore.get(root, 0, ref)
  end

  test "prepares large cas as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    new_value = :binary.copy("C", 1024)

    assert {:ok, {:cas_blob_ref, "k", "old", encoded_ref, nil}} =
             BlobCommand.prepare(ctx, 0, {:cas, "k", "old", new_value, nil}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^new_value} = BlobStore.get(root, 0, ref)
  end

  test "prepares large fetch-or-compute publish without losing lease consumption", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("F", 1024)
    owner_ref = "fetch-owner"

    assert {:ok, {:fetch_or_compute_publish_blob_ref, "cache-key", encoded_ref, 0, ^owner_ref}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:fetch_or_compute_publish, "cache-key", payload, 0, owner_ref},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares large compound put as a pre-externalized blob ref", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("H", 1024)
    compound_key = CompoundKey.hash_field("hash", "field")

    assert {:ok, {:compound_put_blob_ref, ^compound_key, encoded_ref, 0}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:compound_put, compound_key, payload, 0},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "leaves zset score compound puts inline", %{ctx: ctx} do
    payload = :binary.copy("9", 1024)
    compound_key = CompoundKey.zset_member("zset", "member")
    command = {:compound_put, compound_key, payload, 0}

    assert {:ok, ^command} = BlobCommand.prepare(ctx, 0, command, single_member?: true)
    refute BlobCommand.side_channel_candidate?(ctx, command)
  end

  test "prepares mixed put batch without duplicating small values", %{ctx: ctx, root: root} do
    payload = :binary.copy("B", 1024)

    assert {:ok,
            {:put_blob_batch,
             [
               {"small", "v", 0, :value},
               {"large", encoded_ref, 0, :blob_ref}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:put_batch, [{"small", "v", 0}, {"large", payload, 0}]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares compound batch put with mixed inline and blob values", %{
    ctx: ctx,
    root: root
  } do
    redis_key = "hash"
    small = CompoundKey.hash_field(redis_key, "small")
    large = CompoundKey.hash_field(redis_key, "large")
    payload = :binary.copy("B", 1024)

    assert {:ok,
            {:compound_blob_batch_put, ^redis_key,
             [
               {^small, "v", 0, :value},
               {^large, encoded_ref, 0, :blob_ref}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:compound_batch_put, redis_key, [{small, "v", 0}, {large, payload, 0}]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "leaves zset score compound batches inline", %{ctx: ctx} do
    redis_key = "zset"
    member = CompoundKey.zset_member(redis_key, "member")
    payload = :binary.copy("9", 1024)
    command = {:compound_batch_put, redis_key, [{member, payload, 0}]}

    assert {:ok, ^command} = BlobCommand.prepare(ctx, 0, command, single_member?: true)
    refute BlobCommand.side_channel_candidate?(ctx, command)
  end

  test "prepares generic Ra batches by replacing large puts with blob refs", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("G", 1024)
    set_payload = :binary.copy("S", 1024)
    getset_payload = :binary.copy("T", 1024)
    append_suffix = :binary.copy("A", 1024)
    setrange_patch = :binary.copy("R", 1024)
    cas_value = :binary.copy("C", 1024)
    hash_value = :binary.copy("H", 1024)
    hash_field = CompoundKey.hash_field("hash", "field")
    opts = %{nx: true, xx: false, get: false, keepttl: false}

    assert {:ok,
            {:batch,
             [
               {:append, "log", "x"},
               {:put_blob_ref, "k", encoded_ref, 0},
               {:set_blob_ref, "s", set_encoded_ref, 0, ^opts},
               {:getset_blob_ref, "g", getset_encoded_ref},
               {:append_blob_ref, "a", append_encoded_ref},
               {:setrange_blob_ref, "r", 4, setrange_encoded_ref},
               {:cas_blob_ref, "c", "old", cas_encoded_ref, nil},
               {:compound_put_blob_ref, ^hash_field, hash_encoded_ref, 0}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch,
                [
                  {:append, "log", "x"},
                  {:put, "k", payload, 0},
                  {:set, "s", set_payload, 0, opts},
                  {:getset, "g", getset_payload},
                  {:append, "a", append_suffix},
                  {:setrange, "r", 4, setrange_patch},
                  {:cas, "c", "old", cas_value, nil},
                  {:compound_put, hash_field, hash_value, 0}
                ]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, set_ref} = BlobRef.decode(set_encoded_ref)
    assert {:ok, getset_ref} = BlobRef.decode(getset_encoded_ref)
    assert {:ok, append_ref} = BlobRef.decode(append_encoded_ref)
    assert {:ok, setrange_ref} = BlobRef.decode(setrange_encoded_ref)
    assert {:ok, cas_ref} = BlobRef.decode(cas_encoded_ref)
    assert {:ok, hash_ref} = BlobRef.decode(hash_encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
    assert {:ok, ^set_payload} = BlobStore.get(root, 0, set_ref)
    assert {:ok, ^getset_payload} = BlobStore.get(root, 0, getset_ref)
    assert {:ok, ^append_suffix} = BlobStore.get(root, 0, append_ref)
    assert {:ok, ^setrange_patch} = BlobStore.get(root, 0, setrange_ref)
    assert {:ok, ^cas_value} = BlobStore.get(root, 0, cas_ref)
    assert {:ok, ^hash_value} = BlobStore.get(root, 0, hash_ref)
  end

  test "prepares generic Ra batches with one blob segment fsync", %{ctx: ctx, root: root} do
    parent = self()

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
      send(parent, {:blob_fsync_file, path})
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_fsync_file_hook) end)

    payload_a = :binary.copy("A", 1024)
    payload_b = :binary.copy("B", 1024)

    assert {:ok,
            {:batch, [{:put_blob_ref, "a", encoded_a, 0}, {:put_blob_ref, "b", encoded_b, 0}]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch, [{:put, "a", payload_a, 0}, {:put, "b", payload_b, 0}]},
               single_member?: true
             )

    assert {:ok, ref_a} = BlobRef.decode(encoded_a)
    assert {:ok, ref_b} = BlobRef.decode(encoded_b)
    assert {:ok, {segment_path, _offset_a, _size_a}} = BlobStore.file_ref(root, 0, ref_a)
    assert {:ok, {^segment_path, _offset_b, _size_b}} = BlobStore.file_ref(root, 0, ref_b)

    assert_received {:blob_fsync_file, ^segment_path}
    refute_received {:blob_fsync_file, _}
  end

  test "prepares Flow transition payloads as pre-externalized value refs", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("F", 1024)

    command =
      {:flow_transition_many, nil,
       %{
         records: [
           %{
             id: "flow-1",
             from_state: "running",
             to_state: "next",
             fencing_token: 3,
             payload: payload
           }
         ]
       }}

    assert BlobCommand.side_channel_candidate?(ctx, command)

    assert {:ok,
            {:flow_transition_many, nil,
             %{
               records: [
                 %{
                   payload: marker
                 }
               ]
             }}} = BlobCommand.prepare(ctx, 0, command, single_member?: true)

    assert_flow_blob_marker(root, marker, payload)
  end

  test "prepares idempotent Flow create payloads as pre-externalized value refs", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("I", 1024)

    command =
      {:flow_create, nil,
       %{
         id: "flow-idempotent",
         type: "bench",
         state: "queued",
         partition_key: "tenant-a",
         payload: payload,
         idempotent: true
       }}

    assert BlobCommand.side_channel_candidate?(ctx, command)

    assert {:ok, {:flow_create, nil, %{payload: marker, idempotent: true}}} =
             BlobCommand.prepare(ctx, 0, command, single_member?: true)

    assert_flow_blob_marker(root, marker, payload)
  end

  test "prepares atomic invocation Flow payloads without changing catalog admission", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("A", 1024)
    catalog = %{namespace: "invocations", subject: "invocation-1", value: "record"}

    command =
      {:flow_create_with_catalog, "state-key", catalog,
       %{
         id: "invocation-1",
         type: "invocation",
         state: "queued",
         partition_key: "tenant-a",
         payload: payload
       }}

    assert BlobCommand.side_channel_candidate?(ctx, command)

    assert {:ok, {:flow_create_with_catalog, "state-key", ^catalog, %{payload: marker}}} =
             BlobCommand.prepare(ctx, 0, command, single_member?: true)

    assert_flow_blob_marker(root, marker, payload)
  end

  test "keeps idempotent Flow named value maps inline to preserve digest semantics", %{ctx: ctx} do
    value = :binary.copy("D", 1024)

    command =
      {:flow_create, nil,
       %{
         id: "flow-idempotent-values",
         type: "bench",
         state: "queued",
         partition_key: "tenant-a",
         values: %{"doc" => value},
         idempotent: true
       }}

    refute BlobCommand.side_channel_candidate?(ctx, command)
  end

  test "prepares Flow named value puts as pre-externalized value refs", %{ctx: ctx, root: root} do
    value = :binary.copy("V", 1024)

    command =
      {:flow_named_value_put, nil,
       %{id: "flow-1", partition_key: "tenant-a", name: "doc", value: value}}

    assert BlobCommand.side_channel_candidate?(ctx, command)

    assert {:ok, {:flow_named_value_put, nil, %{value: marker}}} =
             BlobCommand.prepare(ctx, 0, command, single_member?: true)

    assert_flow_blob_marker(root, marker, value)
  end

  test "prepares Flow named values maps as pre-externalized value refs", %{ctx: ctx, root: root} do
    value = :binary.copy("M", 1024)

    command =
      {:flow_create, nil,
       %{
         id: "flow-1",
         type: "bench",
         partition_key: "tenant-a",
         values: %{"doc" => value, "small" => "ok"}
       }}

    assert BlobCommand.side_channel_candidate?(ctx, command)

    assert {:ok, {:flow_create, nil, %{values: %{"doc" => marker, "small" => "ok"}}}} =
             BlobCommand.prepare(ctx, 0, command, single_member?: true)

    assert_flow_blob_marker(root, marker, value)
  end

  test "prepares Flow create, complete, and fail values in one generic batch", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("P", 1024)
    result = :binary.copy("R", 1024)
    error = :binary.copy("E", 1024)

    command =
      {:batch,
       [
         {:flow_create, nil, %{id: "flow-create", type: "bench", payload: payload}},
         {:flow_complete, nil,
          %{id: "flow-complete", lease_token: "lease", fencing_token: 1, result: result}},
         {:flow_fail, nil,
          %{id: "flow-fail", lease_token: "lease", fencing_token: 1, error: error}}
       ]}

    assert BlobCommand.side_channel_candidate?(ctx, command)

    assert {:ok,
            {:batch,
             [
               {:flow_create, nil, %{payload: payload_marker}},
               {:flow_complete, nil, %{result: result_marker}},
               {:flow_fail, nil, %{error: error_marker}}
             ]}} = BlobCommand.prepare(ctx, 0, command, single_member?: true)

    assert_flow_blob_marker(root, payload_marker, payload)
    assert_flow_blob_marker(root, result_marker, result)
    assert_flow_blob_marker(root, error_marker, error)
  end

  test "prepares nested put_batch inside generic Ra batches", %{ctx: ctx, root: root} do
    payload = :binary.copy("N", 1024)

    assert {:ok,
            {:batch,
             [
               {:append, "log", "x"},
               {:put_blob_ref, "large", encoded_ref, 0},
               {:put, "small", "v", 0}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch,
                [
                  {:append, "log", "x"},
                  {:put_batch, [{"large", payload, 0}, {"small", "v", 0}]}
                ]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "leaves commands unchanged when blob side-channel is disabled", %{ctx: ctx} do
    ctx = %{ctx | blob_side_channel_threshold_bytes: 0}
    command = {:put, "k", :binary.copy("P", 1024), 0}

    assert {:ok, ^command} = BlobCommand.prepare(ctx, 0, command, single_member?: true)
  end

  test "candidate check only selects commands that can externalize", %{ctx: ctx} do
    ref_shaped =
      "x"
      |> BlobRef.from_segment(0, 0)
      |> BlobRef.encode!()

    refute BlobCommand.side_channel_candidate?(ctx, {:put, "small", "v", 0})
    assert BlobCommand.side_channel_candidate?(ctx, {:put, "large", :binary.copy("L", 1024), 0})
    assert BlobCommand.side_channel_candidate?(ctx, {:put, "ref", ref_shaped, 0})

    refute BlobCommand.side_channel_candidate?(ctx, {:put_batch, [{"small", "v", 0}]})

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:set, "large", :binary.copy("S", 1024), 0, %{}}
           )

    assert BlobCommand.side_channel_candidate?(ctx, {:getset, "large", :binary.copy("T", 1024)})
    assert BlobCommand.side_channel_candidate?(ctx, {:append, "large", :binary.copy("A", 1024)})

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:setrange, "large", 0, :binary.copy("R", 1024)}
           )

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:cas, "large", "old", :binary.copy("C", 1024), nil}
           )

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:compound_put, CompoundKey.hash_field("hash", "field"), :binary.copy("H", 1024), 0}
           )

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:compound_batch_put, "hash",
              [{CompoundKey.hash_field("hash", "field"), :binary.copy("H", 1024), 0}]}
           )

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:batch, [{:append, "k", "x"}, {:put, "large", :binary.copy("L", 1024), 0}]}
           )
  end

  test "leaves commands unchanged for multi-member Raft groups", %{ctx: ctx} do
    command = {:put, "k", :binary.copy("P", 1024), 0}

    assert {:ok, ^command} = BlobCommand.prepare(ctx, 0, command, single_member?: false)
  end

  test "ref-shaped user bytes are externalized as payload, not reused as a pointer", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("R", 1024)
    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    encoded_ref = BlobRef.encode!(ref)

    assert {:ok, {:put_blob_ref, "k", stored_ref, 0}} =
             BlobCommand.prepare(ctx, 0, {:put, "k", encoded_ref, 0}, single_member?: true)

    assert stored_ref != encoded_ref
    assert {:ok, stored_blob_ref} = BlobRef.decode(stored_ref)
    assert {:ok, ^encoded_ref} = BlobStore.get(root, 0, stored_blob_ref)
  end

  defp assert_flow_blob_marker(root, marker, expected_value) do
    assert {:ok, encoded_ref} = BlobCommand.flow_blob_value_ref(marker)
    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, encoded_value} = BlobStore.get(root, 0, ref)
    assert Ferricstore.Flow.decode_value(encoded_value) == expected_value
  end
end
