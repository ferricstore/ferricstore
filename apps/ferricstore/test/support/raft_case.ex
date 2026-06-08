defmodule Ferricstore.Test.RaftCase do
  @moduledoc """
  Shared imports/tags for Raft-focused tests.
  """

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: false

      @moduletag :raft

      import Ferricstore.Test.Eventually
      alias Ferricstore.Test.ShardHelpers
    end
  end
end
