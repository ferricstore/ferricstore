defmodule Ferricstore.Raft.WARaftBackendTest.LabelCounter do
  @moduledoc false

  def new_label(nil, _command), do: 1
  def new_label(:undefined, _command), do: 1
  def new_label(label, _command) when is_integer(label), do: label + 1
end

defmodule Ferricstore.Raft.WARaftBackendTest.OversizedLabel do
  @moduledoc false

  def new_label(_label, _command), do: :binary.copy("x", 1_048_576)
end

defmodule Ferricstore.Test.WARaftBackendCase do
  @moduledoc false

  # The WARaft backend suite is split across modules so CI can run the halves
  # side by side. Each module uses this template and the sections it runs; the
  # helper sections define the private helpers every section may call.

  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :raft
      @moduletag :waraft_backend_suite

      # Each half calls only some of the shared helper sections' functions.
      @compile :nowarn_unused_function

      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Test.ShardHelpers

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

      setup_all do
        :ok = WARaftBackend.stop()
        on_exit(fn -> ShardHelpers.restore_default_waraft!() end)
        :ok
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
    end
  end
end
