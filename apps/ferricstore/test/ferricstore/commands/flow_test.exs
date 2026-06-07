Code.require_file("flow_test/sections/part_01.exs", __DIR__)
Code.require_file("flow_test/sections/part_02.exs", __DIR__)
Code.require_file("flow_test/sections/part_03.exs", __DIR__)

defmodule Ferricstore.Commands.FlowTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  use Ferricstore.Commands.FlowTest.Sections.Part01
  use Ferricstore.Commands.FlowTest.Sections.Part02
  use Ferricstore.Commands.FlowTest.Sections.Part03
end
