defmodule Ferricstore.Store.BlobGCSweeperTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.BlobGCSweeper

  setup do
    old_enabled = Application.get_env(:ferricstore, :blob_gc_sweeper_enabled)

    on_exit(fn ->
      restore_env(:blob_gc_sweeper_enabled, old_enabled)
    end)

    Application.put_env(:ferricstore, :blob_gc_sweeper_enabled, true)
    :ok
  end

  test "skips expensive GC when there are no blob files or tmp files" do
    parent = self()
    name = :"blob_gc_sweeper_skip_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {BlobGCSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         stats_fun: fn ->
           send(parent, :blob_gc_stats_called)
           {:ok, %{files: 0, bytes: 0, tmp_files: 0, tmp_bytes: 0}}
         end,
         sweep_fun: fn ->
           send(parent, :blob_gc_sweep_called)
           {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 0}}
         end}
      )

    send(pid, :sweep)

    assert_receive :blob_gc_stats_called, 1_000
    refute_receive :blob_gc_sweep_called, 100

    assert %{last_sweep: %{status: :skipped, files: 0, tmp_files: 0}} = BlobGCSweeper.info(name)
  end

  test "runs conservative GC when append segment files exist" do
    parent = self()
    name = :"blob_gc_sweeper_segment_run_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {BlobGCSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         stats_fun: fn ->
           send(parent, :blob_gc_stats_called)

           {:ok,
            %{
              files: 1,
              bytes: 4096,
              legacy_files: 0,
              legacy_bytes: 0,
              segment_files: 1,
              segment_bytes: 4096,
              tmp_files: 0,
              tmp_bytes: 0
            }}
         end,
         sweep_fun: fn ->
           send(parent, :blob_gc_sweep_called)
           {:ok, %{deleted_files: 1, deleted_bytes: 4096, kept_files: 0}}
         end}
      )

    send(pid, :sweep)

    assert_receive :blob_gc_stats_called, 1_000
    assert_receive :blob_gc_sweep_called, 1_000

    assert %{last_sweep: %{status: :ok, files: 1, segment_files: 1, deleted_files: 1}} =
             BlobGCSweeper.info(name)
  end

  test "runs conservative GC automatically when blob files exist" do
    parent = self()
    name = :"blob_gc_sweeper_run_#{System.unique_integer([:positive])}"
    handler_id = {:blob_gc_sweeper_run, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :blob, :gc_sweeper, :sweep],
      fn _event, measurements, metadata, test_pid ->
        send(test_pid, {:blob_gc_sweeper_sweep, measurements, metadata})
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    pid =
      start_supervised!(
        {BlobGCSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         stats_fun: fn ->
           {:ok, %{files: 2, bytes: 4096, tmp_files: 0, tmp_bytes: 0}}
         end,
         sweep_fun: fn ->
           send(parent, :blob_gc_sweep_called)
           {:ok, %{deleted_files: 1, deleted_bytes: 2048, kept_files: 1}}
         end}
      )

    send(pid, :sweep)

    assert_receive :blob_gc_sweep_called, 1_000

    assert_receive {:blob_gc_sweeper_sweep,
                    %{files: 2, bytes: 4096, deleted_files: 1, deleted_bytes: 2048},
                    %{status: :ok}},
                   1_000

    assert %{last_sweep: %{status: :ok, deleted_files: 1}} = BlobGCSweeper.info(name)
  end

  test "reports router skips as skipped sweeps" do
    parent = self()
    name = :"blob_gc_sweeper_skip_reason_#{System.unique_integer([:positive])}"
    reason = {:raft_replay_gap, 10, 9}

    pid =
      start_supervised!(
        {BlobGCSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         stats_fun: fn ->
           {:ok,
            %{
              files: 1,
              bytes: 4096,
              legacy_files: 1,
              legacy_bytes: 4096,
              segment_files: 0,
              segment_bytes: 0,
              tmp_files: 0,
              tmp_bytes: 0
            }}
         end,
         sweep_fun: fn ->
           send(parent, :blob_gc_sweep_called)

           {:ok,
            %{
              deleted_files: 0,
              deleted_bytes: 0,
              kept_files: 0,
              skipped: true,
              reason: reason
            }}
         end}
      )

    send(pid, :sweep)

    assert_receive :blob_gc_sweep_called, 1_000

    assert %{last_sweep: %{status: :skipped, skipped: true, reason: ^reason}} =
             BlobGCSweeper.info(name)
  end

  test "emits error telemetry when automatic GC fails" do
    parent = self()
    name = :"blob_gc_sweeper_error_#{System.unique_integer([:positive])}"
    handler_id = {:blob_gc_sweeper_error, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :blob, :gc_sweeper, :error],
      fn _event, measurements, metadata, test_pid ->
        send(test_pid, {:blob_gc_sweeper_error, measurements, metadata})
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    pid =
      start_supervised!(
        {BlobGCSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         stats_fun: fn -> {:ok, %{files: 1, bytes: 1024, tmp_files: 0, tmp_bytes: 0}} end,
         sweep_fun: fn -> {:error, :eio} end}
      )

    send(pid, :sweep)

    assert_receive {:blob_gc_sweeper_error, %{count: 1}, %{reason: :eio}}, 1_000
    assert %{last_sweep: %{status: :error, reason: :eio}} = BlobGCSweeper.info(name)
  end

  test "can be disabled by configuration" do
    Application.put_env(:ferricstore, :blob_gc_sweeper_enabled, false)

    assert :ignore =
             BlobGCSweeper.start_link(
               name: :"blob_gc_sweeper_disabled_#{System.unique_integer([:positive])}"
             )
  end

  test "does not start when blob side-channel is disabled for the instance" do
    Application.put_env(:ferricstore, :blob_gc_sweeper_enabled, true)

    assert :ignore =
             BlobGCSweeper.start_link(
               name: :"blob_gc_sweeper_threshold_disabled_#{System.unique_integer([:positive])}",
               instance_ctx: %{blob_side_channel_threshold_bytes: 0}
             )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
