Code.require_file("history_projector_test/sections/part_01.exs", __DIR__)
Code.require_file("history_projector_test/sections/part_02.exs", __DIR__)
Code.require_file("history_projector_test/sections/part_03.exs", __DIR__)

defmodule Ferricstore.Flow.HistoryProjectorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.OrderedIndex

  use Ferricstore.Flow.HistoryProjectorTest.Sections.Part01

  use Ferricstore.Flow.HistoryProjectorTest.Sections.Part02

  def handle_recover_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:history_projector_recover_error, event, measurements, metadata})
  end

  use Ferricstore.Flow.HistoryProjectorTest.Sections.Part03

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
