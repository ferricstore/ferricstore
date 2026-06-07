for part <- 1..19 do
  Code.require_file("waraft_backend_test/sections/part_#{part |> Integer.to_string() |> String.pad_leading(2, "0")}.exs", __DIR__)
end

for part <- 1..2 do
  Code.require_file("waraft_backend_test/sections/helpers_part_#{part |> Integer.to_string() |> String.pad_leading(2, "0")}.exs", __DIR__)
end

defmodule Ferricstore.Raft.WARaftBackendTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Raft.WARaftStorage
  alias Ferricstore.Store.{BlobRef, BlobStore}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router

  defmodule LabelCounter do
    @moduledoc false

    def new_label(nil, _command), do: 1
    def new_label(:undefined, _command), do: 1
    def new_label(label, _command) when is_integer(label), do: label + 1
  end

  defmodule OversizedLabel do
    @moduledoc false

    def new_label(_label, _command), do: :binary.copy("x", 1_048_576)
  end

  def handle_test_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_blocked, event, measurements, metadata})
  end

  def handle_segment_log_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_log_telemetry, event, measurements, metadata})
  end

  def handle_namespace_batcher_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_namespace_batcher_flush, event, measurements, metadata})
  end

  def handle_payload_fsync_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_payload_fsync_telemetry, event, measurements, metadata})
  end

  def handle_blob_prepare_failed_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_blob_prepare_failed, event, measurements, metadata})
  end

  def handle_commit_timeout_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_commit_timeout, event, measurements, metadata})
  end

  def handle_storage_startup_phase_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_startup_phase, event, measurements, metadata})
  end

  def handle_storage_apply_phase_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_apply_phase, event, measurements, metadata})
  end

  def handle_segment_projection_checkpoint_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_projection_checkpoint, event, measurements, metadata})
  end

  def handle_segment_projection_trim_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_projection_trim, event, measurements, metadata})
  end

  def handle_store_unavailable_telemetry(event, measurements, metadata, parent) do
    send(parent, {:store_unavailable, event, measurements, metadata})
  end

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-backend-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    Ferricstore.DataDir.ensure_layout!(root, 1)
    Ferricstore.Store.ActiveFile.init(1)

    ctx = build_ctx(root)

    on_exit(fn ->
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      File.rm_rf!(root)
    end)

    %{root: root, ctx: ctx}
  end


  use Ferricstore.Raft.WARaftBackendTest.Sections.Part01
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part02
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part03
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part04
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part05
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part06
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part07
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part08
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part09
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part10
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part11
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part12
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part13
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part14
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part15
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part16
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part17
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part18
  use Ferricstore.Raft.WARaftBackendTest.Sections.Part19

  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart01
  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart02
end
