defmodule Ferricstore.Commands.ServerInfoObservabilityTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Server

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_server_info_observability_#{System.unique_integer([:positive])}"
      )

    instance_ctx = %FerricStore.Instance{
      name: :server_info_observability_test,
      data_dir: root,
      data_dir_expanded: Path.expand(root),
      shard_count: 1,
      read_sample_rate: 1
    }

    on_exit(fn -> File.rm_rf!(root) end)

    %{root: root, store: %{__instance_ctx__: instance_ctx}}
  end

  test "INFO bitcask emits telemetry when a shard directory cannot be scanned", %{
    root: root,
    store: store
  } do
    shard_dir = Ferricstore.DataDir.shard_data_path(root, 0)
    File.mkdir_p!(Path.dirname(shard_dir))
    File.write!(shard_dir, "not a directory")

    parent = self()
    handler_id = {__MODULE__, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :commands, :info, :bitcask_scan_failed],
      fn event, measurements, metadata, _config ->
        send(parent, {:bitcask_scan_failed, event, measurements, metadata})
      end,
      nil
    )

    try do
      info = Server.handle("INFO", ["bitcask"], store)

      assert info =~ "# Bitcask"
      assert info =~ "shard_0_data_file_count:0"

      assert_receive {:bitcask_scan_failed,
                      [:ferricstore, :commands, :info, :bitcask_scan_failed], %{count: 1},
                      %{
                        phase: :list_shard_dir,
                        shard_index: 0,
                        path: ^shard_dir,
                        reason: {:not_a_directory, _message}
                      }}
    after
      :telemetry.detach(handler_id)
    end
  end

  test "INFO bitcask counts only canonical regular segment and hint files", %{
    root: root,
    store: store
  } do
    shard_dir = Ferricstore.DataDir.shard_data_path(root, 0)
    outside = Path.join(root, "outside.log")
    File.mkdir_p!(shard_dir)
    File.write!(Path.join(shard_dir, "00000.log"), "data")
    File.write!(Path.join(shard_dir, "00000.hint"), "hint")
    File.write!(Path.join(shard_dir, "0.log"), "alias")
    File.write!(Path.join(shard_dir, "notes.log"), "unrelated")
    File.write!(Path.join(shard_dir, "compact_0.log"), "temporary")
    File.write!(outside, "outside")
    File.ln_s!(outside, Path.join(shard_dir, "00001.log"))
    File.ln_s!(outside, Path.join(shard_dir, "00001.hint"))

    info = Server.handle("INFO", ["bitcask"], store)

    assert info =~ "shard_0_data_file_count:1"
    assert info =~ "shard_0_hint_file_count:1"
    assert info =~ "shard_0_merge_candidates:0"
  end
end
