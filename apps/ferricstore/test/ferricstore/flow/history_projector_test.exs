Code.require_file("history_projector_test/sections/sync_projection_writes_dedicated_history_log_updates_index_advances_wate.exs", __DIR__)
Code.require_file("history_projector_test/sections/sync_projection_hydrates_spilled_waraft_apply_projection_values_batch.exs", __DIR__)
Code.require_file("history_projector_test/sections/async_enqueue_rejects_above_configured_pending_cap_so_apply_fall_back_sy.exs", __DIR__)

defmodule Ferricstore.Flow.HistoryProjectorTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.OrderedIndex

  use Ferricstore.Flow.HistoryProjectorTest.Sections.SyncProjectionWritesDedicatedHistoryLogUpdatesIndexAdvancesWate

  use Ferricstore.Flow.HistoryProjectorTest.Sections.SyncProjectionHydratesSpilledWaraftApplyProjectionValuesBatch

  def handle_recover_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:history_projector_recover_error, event, measurements, metadata})
  end

  use Ferricstore.Flow.HistoryProjectorTest.Sections.AsyncEnqueueRejectsAboveConfiguredPendingCapSoApplyFallBackSy

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
