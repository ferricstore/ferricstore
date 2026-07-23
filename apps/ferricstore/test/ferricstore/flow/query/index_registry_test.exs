defmodule Ferricstore.Flow.Query.IndexRegistryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.RegistrySnapshot
  alias Ferricstore.TermCodec
  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.Query.IndexRegistry

  defmodule AlternateMetadataExtension do
    @behaviour MetadataExtension

    @impl true
    def configure(_opts), do: {:ok, %{mode: :dedicated, generation: 2, fields: []}}

    @impl true
    def bind_write(_operation, _trusted_context, _snapshot),
      do: {:error, :flow_scope_required}

    @impl true
    def bind_query(_source, _trusted_context, _snapshot),
      do: {:error, :flow_scope_required}
  end

  setup do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_registry_#{suffix}")
    instance_name = :"query_registry_instance_#{suffix}"
    server_name = :"query_registry_server_#{suffix}"

    {:ok, metadata_snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    ctx = %{
      name: instance_name,
      data_dir: data_dir,
      shard_count: 2,
      flow_metadata_snapshot: metadata_snapshot
    }

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{ctx: ctx, server_name: server_name}
  end

  test "fails closed when the immutable metadata snapshot is absent", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)

    try do
      assert {:error, :query_index_metadata_snapshot_required} =
               IndexRegistry.start_link(
                 instance_ctx: Map.delete(ctx, :flow_metadata_snapshot),
                 name: server_name
               )
    after
      Process.flag(:trap_exit, previous_trap_exit)
    end
  end

  test "rejects restart under a different metadata schema with the same scope width", context do
    %{ctx: ctx, server_name: server_name} = context
    pid = start_registry!(ctx, server_name)
    GenServer.stop(pid)

    {:ok, different_snapshot} =
      MetadataExtension.configure(AlternateMetadataExtension, [])

    assert different_snapshot.mode == ctx.flow_metadata_snapshot.mode

    assert {:ok, 0} = MetadataExtension.fixed_scope_bytes(different_snapshot)
    assert {:ok, 0} = MetadataExtension.fixed_scope_bytes(ctx.flow_metadata_snapshot)
    refute different_snapshot.schema_digest == ctx.flow_metadata_snapshot.schema_digest

    previous_trap_exit = Process.flag(:trap_exit, true)

    try do
      assert {:error, :query_index_metadata_schema_mismatch} =
               IndexRegistry.start_link(
                 instance_ctx: %{ctx | flow_metadata_snapshot: different_snapshot},
                 name: server_name
               )
    after
      Process.flag(:trap_exit, previous_trap_exit)
    end

    assert {:error, :query_index_registry_unavailable} = IndexRegistry.snapshot(ctx, 0)
  end

  test "persists monotonic checkpoints and never exposes a partial build", context do
    %{ctx: ctx, server_name: server_name} = context
    pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{epoch: 1, indexes: indexes}} =
             IndexRegistry.snapshot(ctx, 0)

    assert indexes != []
    assert Enum.all?(indexes, &(&1.state == :building))
    assert indexes |> Enum.map(& &1.build_id) |> Enum.uniq() |> length() == 1
    index = hd(indexes)
    build_id = index.build_id
    fence_build!(server_name, build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, build_id, 0,
               phase: :backfill,
               cursor: "page-2",
               fenced: true,
               scanned_records: 100,
               written_entries: 80,
               written_bytes: 4_096
             )

    assert {:error, :non_monotonic_query_index_checkpoint} =
             IndexRegistry.checkpoint_build(server_name, build_id, 0,
               phase: :backfill,
               cursor: "page-1",
               fenced: true,
               scanned_records: 99,
               written_entries: 79,
               written_bytes: 4_095
             )

    assert :ok =
             IndexRegistry.complete_build_shard(server_name, build_id, 0)

    assert {:ok, %{entries: first_shard_statuses}} =
             IndexRegistry.build_status(server_name, build_id)

    assert Enum.all?(first_shard_statuses, &(&1.state == :building))
    assert Enum.all?(first_shard_statuses, &(&1.checkpoints[0].phase == :done))

    GenServer.stop(pid)
    pid = start_registry!(ctx, server_name)

    assert {:ok, %{entries: resumed_statuses}} = IndexRegistry.build_status(server_name, build_id)
    assert Enum.all?(resumed_statuses, &(&1.state == :building))
    assert Enum.all?(resumed_statuses, &(&1.checkpoints[0].phase == :done))

    assert {:error, :query_index_not_validated} =
             IndexRegistry.activate_build(server_name, build_id)

    fence_build!(server_name, build_id, 1)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, build_id, 1,
               phase: :backfill,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok = IndexRegistry.complete_build_shard(server_name, build_id, 1)

    assert {:ok, %{state: :validating}} =
             IndexRegistry.status(server_name, index.definition.id, index.definition.version)

    complete_validation!(server_name, build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, build_id)

    assert {:ok, %RegistrySnapshot{indexes: active_indexes}} = IndexRegistry.snapshot(ctx, 1)

    assert Enum.any?(active_indexes, fn registered ->
             registered.definition.id == index.definition.id and registered.state == :active
           end)

    GenServer.stop(pid)
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %{state: :active}} =
             IndexRegistry.status(server_name, index.definition.id, index.definition.version)
  end

  test "journals progress without rewriting the registry snapshot and replays it on restart",
       context do
    %{ctx: ctx, server_name: server_name} = context
    pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    snapshot_path = IndexRegistry.snapshot_path(ctx)
    snapshot_before = File.read!(snapshot_path)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert File.read!(snapshot_path) == snapshot_before
    journal = File.read!(IndexRegistry.journal_path(ctx))
    assert byte_size(journal) > 0
    assert byte_size(journal) < byte_size(snapshot_before)

    GenServer.stop(pid)
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %{checkpoints: %{0 => %{phase: :snapshot, fenced: true}}}} =
             IndexRegistry.build_status(server_name, index.build_id)
  end

  test "repairs an incomplete final journal frame after replaying durable progress", context do
    %{ctx: ctx, server_name: server_name} = context
    pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    journal_path = IndexRegistry.journal_path(ctx)
    durable_journal = File.read!(journal_path)
    GenServer.stop(pid)
    File.write!(journal_path, durable_journal <> <<0, 0, 0>>)

    _pid = start_registry!(ctx, server_name)

    assert File.read!(journal_path) == durable_journal

    assert {:ok, %{checkpoints: %{0 => %{phase: :snapshot, fenced: true}}}} =
             IndexRegistry.build_status(server_name, index.build_id)
  end

  test "fails startup closed for a complete journal frame with a corrupt checksum", context do
    %{ctx: ctx, server_name: server_name} = context
    pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    journal_path = IndexRegistry.journal_path(ctx)
    <<header::binary-size(36), byte, rest::binary>> = File.read!(journal_path)
    GenServer.stop(pid)
    File.write!(journal_path, <<header::binary, Bitwise.bxor(byte, 1), rest::binary>>)

    previous_trap_exit = Process.flag(:trap_exit, true)

    try do
      assert {:error, {:invalid_query_index_registry_journal, :checksum_mismatch}} =
               IndexRegistry.start_link(instance_ctx: ctx, name: server_name)
    after
      Process.flag(:trap_exit, previous_trap_exit)
    end

    assert {:error, :query_index_registry_unavailable} = IndexRegistry.snapshot(ctx, 0)
  end

  test "overview exposes bounded lifecycle progress without resume cursors", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: [registered | _]}} =
             IndexRegistry.snapshot(ctx, 0)

    fence_build!(server_name, registered.build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, registered.build_id, 0,
               phase: :backfill,
               cursor: "tenant-secret-resume-key",
               fenced: true,
               scanned_records: 12,
               written_entries: 8,
               written_bytes: 512
             )

    assert {:ok, %{epoch: 1, catalog_version: 3, indexes: indexes}} =
             IndexRegistry.overview(server_name)

    status =
      Enum.find(indexes, fn index ->
        index.id == registered.definition.id and index.version == registered.definition.version
      end)

    assert status.source == :runs
    assert status.state == :building
    refute status.queryable
    assert status.build_id == registered.build_id
    assert status.workloads == registered.definition.workloads

    assert status.fields ==
             Enum.map(registered.definition.fields, fn {field, direction, encoding} ->
               %{name: field, direction: direction, encoding: encoding}
             end)

    assert status.build == %{
             completed_shards: 0,
             current_phases: [:pending, :backfill],
             phase_counts: %{pending: 1, backfill: 1},
             scanned_records: 12,
             total_shards: 2,
             written_bytes: 512,
             written_entries: 8
           }

    assert status.validation == %{
             checked_entries: 0,
             checked_records: 0,
             completed_shards: 0,
             current_phases: [:pending],
             failure_reason: nil,
             mismatches: 0,
             phase_counts: %{pending: 2},
             status: :pending,
             total_shards: 2,
             validated_at_ms: nil
           }

    assert status.retirement == %{status: :not_applicable}
    refute inspect(status) =~ "tenant-secret-resume-key"
    refute inspect(status) =~ "cursor"
  end

  test "overview reports validation failures and retirement work", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [registered | _]}} = IndexRegistry.snapshot(ctx, 0)

    complete_build!(server_name, registered.build_id, ctx.shard_count)

    assert :ok =
             IndexRegistry.validation_failed(server_name, registered.build_id,
               checked_records: 20,
               checked_entries: 19,
               mismatches: 2,
               reason: :source_index_mismatch
             )

    assert :ok =
             IndexRegistry.checkpoint_retirement(
               server_name,
               registered.definition.id,
               registered.definition.version,
               0,
               phase: :index,
               cursor: "",
               deleted_entries: 0,
               deleted_bytes: 0,
               rewritten_reverse_rows: 0
             )

    assert :ok =
             IndexRegistry.checkpoint_retirement(
               server_name,
               registered.definition.id,
               registered.definition.version,
               0,
               phase: :index,
               cursor: "tenant-secret-retirement-cursor",
               deleted_entries: 7,
               deleted_bytes: 700,
               rewritten_reverse_rows: 0
             )

    assert {:ok, %{indexes: indexes}} = IndexRegistry.overview(server_name)

    status =
      Enum.find(indexes, fn index ->
        index.id == registered.definition.id and index.version == registered.definition.version
      end)

    assert status.state == :failed
    refute status.queryable
    assert status.validation.status == :failed
    assert status.validation.failure_reason == :source_index_mismatch
    assert status.validation.checked_records == 20
    assert status.validation.checked_entries == 19
    assert status.validation.mismatches == 2

    assert status.retirement.status == :pending
    assert status.retirement.current_phases == [:fence, :index]
    assert status.retirement.phase_counts == %{fence: 1, index: 1}
    assert status.retirement.deleted_entries == 7
    assert status.retirement.deleted_bytes == 700
    refute inspect(status) =~ "tenant-secret-retirement-cursor"
    refute inspect(status) =~ "cursor"
  end

  test "checkpoints every index in one catalog build atomically", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    build_id = hd(indexes).build_id
    fence_build!(server_name, build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, build_id, 0,
               phase: :backfill,
               cursor: "shared-page",
               fenced: true,
               scanned_records: 50,
               written_entries: 75,
               written_bytes: 8_192
             )

    assert {:ok, %{entries: statuses, checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(server_name, build_id)

    assert length(statuses) == length(indexes)
    assert checkpoint.cursor == "shared-page"

    assert Enum.all?(statuses, fn status ->
             status.checkpoints[0] == checkpoint
           end)
  end

  test "completion clears the resume cursor for every index in the build", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    build_id = hd(indexes).build_id
    fence_build!(server_name, build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, build_id, 0,
               phase: :backfill,
               cursor: "page-2",
               fenced: true,
               scanned_records: 100,
               written_entries: 80,
               written_bytes: 4_096
             )

    assert :ok =
             IndexRegistry.complete_build_shard(server_name, build_id, 0,
               cursor: "final-page",
               fenced: true,
               scanned_records: 120,
               written_entries: 96,
               written_bytes: 5_120
             )

    assert {:ok, %{entries: statuses, checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(server_name, build_id)

    assert checkpoint.phase == :done
    assert checkpoint.cursor == ""
    assert Enum.all?(statuses, &(&1.checkpoints[0] == checkpoint))
  end

  test "only the completion API may write done and it requires backfill", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    done = [
      phase: :done,
      cursor: "",
      scanned_records: 0,
      written_entries: 0,
      written_bytes: 0
    ]

    assert {:error, :invalid_query_index_checkpoint_transition} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0, done)

    assert {:error, :query_index_backfill_not_complete} =
             IndexRegistry.complete_build_shard(server_name, index.build_id, 0)

    assert {:ok, %{entries: statuses}} =
             IndexRegistry.build_status(server_name, index.build_id)

    assert Enum.all?(statuses, &(&1.state == :building))
    assert Enum.all?(statuses, &(Map.get(&1.checkpoints, 0) == nil))
  end

  test "rejects lifecycle counters outside the unsigned 64-bit contract", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert {:error, :invalid_query_index_checkpoint} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0x1_0000_0000_0000_0000,
               written_entries: 0,
               written_bytes: 0
             )
  end

  test "requires the initial build fence to preserve zero progress", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert {:error, :query_index_build_not_fenced} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 1,
               written_entries: 0,
               written_bytes: 0
             )

    assert {:ok, %{checkpoints: checkpoints}} =
             IndexRegistry.build_status(server_name, index.build_id)

    refute Map.has_key?(checkpoints, 0)
  end

  test "validation completion cannot change counters after cleanup", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)
    advance_validation_to_cleanup!(server_name, index.build_id, 0, 10, 20)
    definition_count = validation_definition_count!(server_name, index.build_id)

    assert {:error, :non_monotonic_query_index_validation_checkpoint} =
             IndexRegistry.complete_validation_shard(server_name, index.build_id, 0,
               cursor: "",
               fenced: true,
               definition_position: definition_count,
               checked_records: 11,
               checked_entries: 20,
               mismatches: 0
             )

    assert {:ok, %{validation_checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(server_name, index.build_id)

    assert checkpoint.phase == :cleanup
    assert checkpoint.checked_records == 10
  end

  test "rejects aggregate validation counter overflow without completing the shard", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)

    Enum.each(0..1, fn shard_index ->
      count = if shard_index == 0, do: 0xFFFF_FFFF_FFFF_FFFF, else: 1
      advance_validation_to_cleanup!(server_name, index.build_id, shard_index, count, count)
    end)

    assert :ok = IndexRegistry.complete_validation_shard(server_name, index.build_id, 0)

    assert {:error, :query_index_validation_counter_overflow} =
             IndexRegistry.complete_validation_shard(server_name, index.build_id, 1)

    assert {:ok, %{state: :validating, validation: validation}} =
             IndexRegistry.status(
               server_name,
               index.definition.id,
               index.definition.version
             )

    assert validation.status == :pending
    assert validation.checkpoints[0].phase == :done
    assert validation.checkpoints[1].phase == :cleanup
  end

  test "requires the durable fence checkpoint before build or validation can advance", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert {:error, :query_index_build_not_fenced} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :backfill,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    fence_build!(server_name, index.build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :backfill,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok = IndexRegistry.complete_build_shard(server_name, index.build_id, 0)
    fence_build!(server_name, index.build_id, 1)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 1,
               phase: :backfill,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok = IndexRegistry.complete_build_shard(server_name, index.build_id, 1)
    definition_count = validation_definition_count!(server_name, index.build_id)

    assert {:error, :query_index_validation_not_fenced} =
             IndexRegistry.checkpoint_validation(server_name, index.build_id, 0,
               phase: :cleanup,
               cursor: "",
               fenced: true,
               definition_position: definition_count,
               checked_records: 0,
               checked_entries: 0,
               mismatches: 0
             )
  end

  test "rejects validation callbacks that skip phases or index definitions", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)
    definition_count = validation_definition_count!(server_name, index.build_id)

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :source,
               definition_position: 0
             )

    assert {:error, :non_monotonic_query_index_validation_checkpoint} =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :index,
               definition_position: 1
             )

    assert {:error, :non_monotonic_query_index_validation_checkpoint} =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :cleanup,
               definition_position: definition_count
             )

    assert {:ok, %{validation_checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(server_name, index.build_id)

    assert checkpoint.phase == :source
    assert checkpoint.definition_position == 0
  end

  test "requires empty cursors when validation advances to another definition", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :source,
               definition_position: 0
             )

    assert {:error, :invalid_query_index_validation_checkpoint} =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :index,
               definition_position: 0,
               cursor: "uncommitted-page"
             )

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :index,
               definition_position: 0
             )

    assert {:error, :invalid_query_index_validation_checkpoint} =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :index,
               definition_position: 1,
               cursor: "next-index-page"
             )
  end

  test "durably resets an index validation shard after a concurrent fanout change", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :source,
               definition_position: 0
             )

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :source,
               definition_position: 0,
               cursor: "source-page",
               checked_records: 7
             )

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :index,
               definition_position: 0,
               checked_records: 7
             )

    assert :ok = IndexRegistry.restart_validation_shard(server_name, index.build_id, 0)

    assert {:ok, %{validation_checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(server_name, index.build_id)

    assert checkpoint == %{
             phase: :source,
             cursor: "",
             fenced: false,
             definition_position: 0,
             checked_records: 0,
             checked_entries: 0,
             mismatches: 0,
             counter_runs: []
           }

    GenServer.stop(Process.whereis(server_name))
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %{validation_checkpoints: %{0 => ^checkpoint}}} =
             IndexRegistry.build_status(server_name, index.build_id)
  end

  test "fails startup closed for an impossible pending validation position", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)

    assert :ok =
             checkpoint_validation(server_name, index.build_id, 0,
               phase: :source,
               definition_position: 0
             )

    GenServer.stop(pid)
    path = IndexRegistry.snapshot_path(ctx)

    {:ok, {tag, version, metadata_contract, epoch, catalog_version, digest, entries}} =
      path |> File.read!() |> TermCodec.decode()

    skipped = %{
      phase: :cleanup,
      cursor: "",
      fenced: true,
      definition_position: 0,
      checked_records: 0,
      checked_entries: 0,
      mismatches: 0,
      counter_runs: []
    }

    entries =
      Enum.map(entries, fn entry ->
        put_in(entry, [:validation, :checkpoints, 0], skipped)
      end)

    File.write!(
      path,
      TermCodec.encode({
        tag,
        version,
        metadata_contract,
        epoch,
        catalog_version,
        digest,
        entries
      })
    )

    case IndexRegistry.start_link(instance_ctx: ctx, name: server_name) do
      {:ok, unexpected_pid} ->
        Process.unlink(unexpected_pid)
        GenServer.stop(unexpected_pid)
        flunk("registry accepted an impossible validation phase position")

      result ->
        assert {:error, {:invalid_query_index_registry_snapshot, :invalid_validation_group}} =
                 result
    end
  end

  test "fails startup closed when an active index loses a shard completion proof", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    pid = start_registry!(ctx, server_name)
    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    complete_build!(server_name, index.build_id, ctx.shard_count)
    complete_validation!(server_name, index.build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, index.build_id)
    GenServer.stop(pid)

    path = IndexRegistry.snapshot_path(ctx)

    {:ok, {tag, version, metadata_contract, epoch, catalog_version, digest, entries}} =
      path |> File.read!() |> TermCodec.decode()

    [first | rest] = entries
    entries = [%{first | checkpoints: Map.delete(first.checkpoints, 1)} | rest]

    File.write!(
      path,
      TermCodec.encode({
        tag,
        version,
        metadata_contract,
        epoch,
        catalog_version,
        digest,
        entries
      })
    )

    case IndexRegistry.start_link(instance_ctx: ctx, name: server_name) do
      {:ok, unexpected_pid} ->
        Process.unlink(unexpected_pid)
        GenServer.stop(unexpected_pid)
        flunk("registry published an active index without complete shard coverage")

      result ->
        assert {:error, {:invalid_query_index_registry_snapshot, :invalid_lifecycle_progress}} =
                 result
    end
  end

  test "fails startup closed for an impossible retirement checkpoint", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    catalog_path = Path.join(ctx.data_dir, "retirement-snapshot-catalog.json")
    write_catalog!(catalog_path, 1, [catalog_index("removed", 1)])
    pid = start_registry!(ctx, server_name, catalog_path)
    GenServer.stop(pid)

    write_catalog!(catalog_path, 2, [catalog_index("current", 1)])
    pid = start_registry!(ctx, server_name, catalog_path)
    GenServer.stop(pid)
    path = IndexRegistry.snapshot_path(ctx)

    {:ok, {tag, version, metadata_contract, epoch, catalog_version, digest, entries}} =
      path |> File.read!() |> TermCodec.decode()

    entries =
      Enum.map(entries, fn
        %{definition: %{id: "removed"}} = entry ->
          checkpoint = %{
            phase: :cleanup,
            cursor: "unfinished-page",
            deleted_entries: 1,
            deleted_bytes: 1,
            rewritten_reverse_rows: 1
          }

          put_in(entry, [:retirement, :checkpoints, 0], checkpoint)

        entry ->
          entry
      end)

    File.write!(
      path,
      TermCodec.encode({
        tag,
        version,
        metadata_contract,
        epoch,
        catalog_version,
        digest,
        entries
      })
    )

    assert {:error, {:invalid_query_index_registry_snapshot, :invalid_entry}} =
             IndexRegistry.start_link(
               instance_ctx: ctx,
               name: server_name,
               catalog_path: catalog_path
             )
  end

  test "catalog removal of an unfinished build remains restartable for cleanup", context do
    %{ctx: ctx, server_name: server_name} = context
    catalog_path = Path.join(ctx.data_dir, "catalog.json")
    write_catalog!(catalog_path, 1, [catalog_index("removed", 1)])
    pid = start_registry!(ctx, server_name, catalog_path)
    GenServer.stop(pid)

    write_catalog!(catalog_path, 2, [catalog_index("current", 1)])
    pid = start_registry!(ctx, server_name, catalog_path)

    assert {:ok, %{state: :failed, validation: %{reason: :catalog_removed}}} =
             IndexRegistry.status(server_name, "removed", 1)

    GenServer.stop(pid)
    _pid = start_registry!(ctx, server_name, catalog_path)

    assert {:ok, %{state: :failed, retirement: %{status: :pending}}} =
             IndexRegistry.status(server_name, "removed", 1)

    complete_retirement!(server_name, "removed", 1, ctx.shard_count)
    assert {:error, :query_index_not_found} = IndexRegistry.status(server_name, "removed", 1)

    GenServer.stop(Process.whereis(server_name))
    _pid = start_registry!(ctx, server_name, catalog_path)
    assert {:error, :query_index_not_found} = IndexRegistry.status(server_name, "removed", 1)
  end

  test "catalog reconciliation rejects aggregate validation counter overflow", context do
    %{ctx: ctx, server_name: server_name} = context
    catalog_path = Path.join(ctx.data_dir, "overflow-catalog.json")
    write_catalog!(catalog_path, 1, [catalog_index("overflow", 1)])
    pid = start_registry!(ctx, server_name, catalog_path)

    assert {:ok, %RegistrySnapshot{indexes: [index]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, index.build_id, ctx.shard_count)

    Enum.each(0..(ctx.shard_count - 1), fn shard_index ->
      advance_validation_to_cleanup!(
        server_name,
        index.build_id,
        shard_index,
        0xFFFF_FFFF_FFFF_FFFF,
        0xFFFF_FFFF_FFFF_FFFF
      )
    end)

    GenServer.stop(pid)
    write_catalog!(catalog_path, 2, [catalog_index("replacement", 1)])

    previous_trap_exit = Process.flag(:trap_exit, true)

    result =
      try do
        IndexRegistry.start_link(
          instance_ctx: ctx,
          name: server_name,
          catalog_path: catalog_path
        )
      after
        Process.flag(:trap_exit, previous_trap_exit)
      end

    assert {:error, :query_index_validation_counter_overflow} = result
  end

  @tag capture_log: true
  test "publication failure terminates and reloads the already durable transition", context do
    %{ctx: ctx, server_name: server_name} = context
    pid = start_registry!(ctx, server_name)
    monitor = Process.monitor(pid)

    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    missing_table = :"missing_query_registry_cache_#{System.unique_integer([:positive])}"
    :sys.replace_state(pid, &%{&1 | cache_table: missing_table})

    assert {:error, :query_index_registry_publish_failed} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert_receive {:DOWN, ^monitor, :process, ^pid, :query_index_registry_publish_failed}, 1_000

    _pid = start_registry!(ctx, server_name)

    assert {:ok, %{checkpoints: %{0 => %{phase: :snapshot, fenced: true}}}} =
             IndexRegistry.status(server_name, index.definition.id, index.definition.version)
  end

  test "rejects a regressing backfill cursor even when counters increase", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    fence_build!(server_name, index.build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :backfill,
               cursor: "page-2",
               fenced: true,
               scanned_records: 10,
               written_entries: 10,
               written_bytes: 100
             )

    assert {:error, :non_monotonic_query_index_checkpoint} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :backfill,
               cursor: "page-1",
               fenced: true,
               scanned_records: 20,
               written_entries: 20,
               written_bytes: 200
             )
  end

  test "rejects a checkpoint cursor that cannot be an LMDB key", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    fence_build!(server_name, index.build_id, 0)

    assert {:error, :invalid_query_index_checkpoint} =
             IndexRegistry.checkpoint_build(server_name, index.build_id, 0,
               phase: :backfill,
               cursor: String.duplicate("x", 512),
               fenced: true,
               scanned_records: 1,
               written_entries: 0,
               written_bytes: 0
             )
  end

  test "fails startup closed when the durable snapshot is corrupt", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    path = IndexRegistry.snapshot_path(ctx)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "corrupt")

    assert {:error, {:invalid_query_index_registry_snapshot, :decode_failed}} =
             IndexRegistry.start_link(
               instance_ctx: ctx,
               name: server_name
             )

    assert {:error, :query_index_registry_unavailable} = IndexRegistry.snapshot(ctx, 0)
  end

  test "fails startup closed when the durable epoch exceeds unsigned 64-bit", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    pid = start_registry!(ctx, server_name)
    GenServer.stop(pid)

    path = IndexRegistry.snapshot_path(ctx)

    {:ok, {tag, version, metadata_contract, _epoch, catalog_version, digest, entries}} =
      path |> File.read!() |> TermCodec.decode()

    File.write!(
      path,
      TermCodec.encode({
        tag,
        version,
        metadata_contract,
        0x1_0000_0000_0000_0000,
        catalog_version,
        digest,
        entries
      })
    )

    assert {:error, {:invalid_query_index_registry_snapshot, :decode_failed}} =
             IndexRegistry.start_link(instance_ctx: ctx, name: server_name)
  end

  test "refuses a symlinked durable registry snapshot", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    pid = start_registry!(ctx, server_name)
    GenServer.stop(pid)

    path = IndexRegistry.snapshot_path(ctx)
    target = path <> ".target"
    File.rename!(path, target)
    File.ln_s!(target, path)

    assert {:error, {:query_index_registry_read_failed, {:symlink, _message}}} =
             IndexRegistry.start_link(instance_ctx: ctx, name: server_name)
  end

  test "keeps the old version active until its replacement is validated and activated", context do
    %{ctx: ctx, server_name: server_name} = context
    catalog_path = Path.join(ctx.data_dir, "catalog.json")
    write_catalog!(catalog_path, 1, [catalog_index("online", 1)])

    pid = start_registry!(ctx, server_name, catalog_path)
    assert {:ok, %RegistrySnapshot{indexes: [old]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, old.build_id, ctx.shard_count)

    complete_validation!(server_name, old.build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, old.build_id)
    GenServer.stop(pid)

    write_catalog!(catalog_path, 2, [catalog_index("online", 2)])
    _pid = start_registry!(ctx, server_name, catalog_path)

    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    assert Enum.find(indexes, &(&1.definition.version == 1)).state == :active
    replacement = Enum.find(indexes, &(&1.definition.version == 2))
    assert replacement.state == :building

    complete_build!(server_name, replacement.build_id, ctx.shard_count)

    complete_validation!(server_name, replacement.build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, replacement.build_id)

    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    assert Enum.find(indexes, &(&1.definition.version == 1)).state == :retiring
    assert Enum.find(indexes, &(&1.definition.version == 2)).state == :active
  end

  test "validates and activates every index in one catalog build atomically", context do
    %{ctx: ctx, server_name: server_name} = context
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    build_id = hd(indexes).build_id

    complete_build!(server_name, build_id, ctx.shard_count)
    complete_validation!(server_name, build_id, ctx.shard_count)

    assert :ok = IndexRegistry.activate_build(server_name, build_id)

    assert {:ok, %RegistrySnapshot{epoch: epoch, indexes: active}} =
             IndexRegistry.snapshot(ctx, 0)

    assert Enum.all?(active, &(&1.state == :active))
    assert Enum.all?(active, &(&1.coverage.validation == :passed))

    GenServer.stop(Process.whereis(server_name))
    _pid = start_registry!(ctx, server_name)

    assert {:ok, %RegistrySnapshot{epoch: ^epoch, indexes: restarted}} =
             IndexRegistry.snapshot(ctx, 0)

    assert Enum.all?(restarted, &(&1.state == :active))
  end

  test "failed validation rolls the whole candidate build back without moving the active version",
       context do
    %{ctx: ctx, server_name: server_name} = context
    catalog_path = Path.join(ctx.data_dir, "catalog.json")
    write_catalog!(catalog_path, 1, [catalog_index("online", 1)])

    pid = start_registry!(ctx, server_name, catalog_path)
    assert {:ok, %RegistrySnapshot{indexes: [old]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, old.build_id, ctx.shard_count)
    complete_validation!(server_name, old.build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, old.build_id)
    GenServer.stop(pid)

    write_catalog!(catalog_path, 2, [catalog_index("online", 2)])
    _pid = start_registry!(ctx, server_name, catalog_path)
    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    candidate = Enum.find(indexes, &(&1.definition.version == 2))
    complete_build!(server_name, candidate.build_id, ctx.shard_count)

    assert :ok =
             IndexRegistry.validation_failed(server_name, candidate.build_id,
               checked_records: 17,
               checked_entries: 19,
               mismatches: 1,
               reason: :entry_value_mismatch
             )

    assert {:ok, %RegistrySnapshot{indexes: rolled_back}} = IndexRegistry.snapshot(ctx, 0)
    assert Enum.find(rolled_back, &(&1.definition.version == 1)).state == :active
    assert Enum.find(rolled_back, &(&1.definition.version == 2)).state == :failed
  end

  test "retirement checkpoints survive restart and remove the obsolete registry entry only at the end",
       context do
    %{ctx: ctx, server_name: server_name} = context
    catalog_path = Path.join(ctx.data_dir, "catalog.json")
    write_catalog!(catalog_path, 1, [catalog_index("online", 1)])

    pid = start_registry!(ctx, server_name, catalog_path)
    assert {:ok, %RegistrySnapshot{indexes: [old]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(server_name, old.build_id, ctx.shard_count)
    complete_validation!(server_name, old.build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, old.build_id)
    GenServer.stop(pid)

    write_catalog!(catalog_path, 2, [catalog_index("online", 2)])
    pid = start_registry!(ctx, server_name, catalog_path)
    assert {:ok, %RegistrySnapshot{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    candidate = Enum.find(indexes, &(&1.definition.version == 2))
    complete_build!(server_name, candidate.build_id, ctx.shard_count)
    complete_validation!(server_name, candidate.build_id, ctx.shard_count)
    assert :ok = IndexRegistry.activate_build(server_name, candidate.build_id)

    assert {:error, :non_monotonic_query_index_retirement_checkpoint} =
             IndexRegistry.checkpoint_retirement(server_name, "online", 1, 0,
               phase: :cleanup,
               cursor: "",
               deleted_entries: 10,
               deleted_bytes: 1_000
             )

    assert :ok =
             IndexRegistry.checkpoint_retirement(server_name, "online", 1, 0,
               phase: :index,
               cursor: "",
               deleted_entries: 0,
               deleted_bytes: 0
             )

    assert :ok =
             IndexRegistry.checkpoint_retirement(server_name, "online", 1, 0,
               phase: :index,
               cursor: "row-10",
               deleted_entries: 10,
               deleted_bytes: 1_000
             )

    GenServer.stop(pid)
    _pid = start_registry!(ctx, server_name, catalog_path)

    assert {:ok, %{retirement: %{checkpoints: %{0 => checkpoint}}}} =
             IndexRegistry.status(server_name, "online", 1)

    assert checkpoint.cursor == "row-10"

    complete_retirement!(server_name, "online", 1, ctx.shard_count)
    assert {:error, :query_index_not_found} = IndexRegistry.status(server_name, "online", 1)
    assert {:ok, %{state: :active}} = IndexRegistry.status(server_name, "online", 2)
  end

  test "rejects catalog rollback and same-version catalog mutation", context do
    %{ctx: ctx, server_name: server_name} = context
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    catalog_path = Path.join(ctx.data_dir, "catalog.json")
    write_catalog!(catalog_path, 2, [catalog_index("stable", 1)])

    pid = start_registry!(ctx, server_name, catalog_path)
    GenServer.stop(pid)

    write_catalog!(catalog_path, 1, [catalog_index("stable", 1)])

    assert {:error, :query_index_catalog_version_regressed} =
             IndexRegistry.start_link(
               instance_ctx: ctx,
               name: server_name,
               catalog_path: catalog_path
             )

    write_catalog!(catalog_path, 2, [catalog_index("stable", 1, ["WF-OTHER-001"])])

    assert {:error, :query_index_catalog_changed_without_version} =
             IndexRegistry.start_link(
               instance_ctx: ctx,
               name: server_name,
               catalog_path: catalog_path
             )
  end

  defp start_registry!(ctx, server_name, catalog_path \\ nil) do
    opts =
      [instance_ctx: ctx, name: server_name]
      |> then(fn opts ->
        if catalog_path, do: Keyword.put(opts, :catalog_path, catalog_path), else: opts
      end)

    {:ok, pid} =
      IndexRegistry.start_link(opts)

    Process.unlink(pid)
    pid
  end

  defp complete_build!(server, build_id, shard_count) do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      fence_build!(server, build_id, shard_index)

      assert :ok =
               IndexRegistry.checkpoint_build(server, build_id, shard_index,
                 phase: :backfill,
                 cursor: "",
                 fenced: true,
                 scanned_records: 0,
                 written_entries: 0,
                 written_bytes: 0
               )

      assert :ok = IndexRegistry.complete_build_shard(server, build_id, shard_index)
    end)
  end

  defp fence_build!(server, build_id, shard_index) do
    assert :ok =
             IndexRegistry.checkpoint_build(server, build_id, shard_index,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )
  end

  defp checkpoint_validation(server, build_id, shard_index, progress) do
    IndexRegistry.checkpoint_validation(
      server,
      build_id,
      shard_index,
      Keyword.merge(
        [
          cursor: "",
          fenced: true,
          checked_records: 0,
          checked_entries: 0,
          mismatches: 0
        ],
        progress
      )
    )
  end

  defp validation_definition_count!(server, build_id) do
    assert {:ok, %{entries: entries}} = IndexRegistry.build_status(server, build_id)
    length(entries)
  end

  defp complete_validation!(server, build_id, shard_count) do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      advance_validation_to_cleanup!(server, build_id, shard_index, 10, 10)
      assert :ok = IndexRegistry.complete_validation_shard(server, build_id, shard_index)
    end)
  end

  defp advance_validation_to_cleanup!(
         server,
         build_id,
         shard_index,
         checked_records,
         checked_entries
       ) do
    assert {:ok, %{entries: entries}} = IndexRegistry.build_status(server, build_id)
    definition_count = length(entries)

    assert :ok =
             checkpoint_validation(server, build_id, shard_index,
               phase: :source,
               definition_position: 0
             )

    Enum.each(0..(definition_count - 1), fn definition_position ->
      assert :ok =
               checkpoint_validation(server, build_id, shard_index,
                 phase: :index,
                 definition_position: definition_position
               )
    end)

    Enum.each(0..(definition_count - 1), fn definition_position ->
      assert :ok =
               checkpoint_validation(server, build_id, shard_index,
                 phase: :counter,
                 definition_position: definition_position
               )
    end)

    assert :ok =
             checkpoint_validation(server, build_id, shard_index,
               phase: :cleanup,
               definition_position: definition_count,
               checked_records: checked_records,
               checked_entries: checked_entries
             )
  end

  defp complete_retirement!(server, id, version, shard_count) do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      complete_retirement_shard!(server, id, version, shard_index)
    end)
  end

  defp complete_retirement_shard!(server, id, version, shard_index) do
    assert {:ok, %{retirement: %{checkpoints: checkpoints}}} =
             IndexRegistry.status(server, id, version)

    checkpoint =
      Map.get(checkpoints, shard_index, %{
        phase: :fence,
        cursor: "",
        deleted_entries: 0,
        deleted_bytes: 0,
        rewritten_reverse_rows: 0
      })

    case checkpoint.phase do
      :fence ->
        assert :ok =
                 IndexRegistry.checkpoint_retirement(
                   server,
                   id,
                   version,
                   shard_index,
                   Map.to_list(%{checkpoint | phase: :index})
                 )

        complete_retirement_shard!(server, id, version, shard_index)

      :index ->
        assert :ok =
                 IndexRegistry.checkpoint_retirement(
                   server,
                   id,
                   version,
                   shard_index,
                   Map.to_list(%{checkpoint | phase: :reverse, cursor: ""})
                 )

        complete_retirement_shard!(server, id, version, shard_index)

      :reverse ->
        assert :ok =
                 IndexRegistry.checkpoint_retirement(
                   server,
                   id,
                   version,
                   shard_index,
                   Map.to_list(%{checkpoint | phase: :cleanup, cursor: ""})
                 )

        complete_retirement_shard!(server, id, version, shard_index)

      :cleanup ->
        assert {:ok, completion} =
                 IndexRegistry.complete_retirement_shard(server, id, version, shard_index)

        assert completion in [:pending, :complete]
    end
  end

  defp catalog_index(id, version, workloads \\ ["WF-LIST-001"]) do
    %{
      "id" => id,
      "version" => version,
      "source" => "runs",
      "workloads" => workloads,
      "fields" => [
        %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
        %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
      ]
    }
  end

  defp write_catalog!(path, version, indexes) do
    File.mkdir_p!(Path.dirname(path))

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => version,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => indexes
      })
    )
  end
end
