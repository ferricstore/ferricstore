Code.require_file("bitmap_test/sections/part_01.exs", __DIR__)
Code.require_file("bitmap_test/sections/part_02.exs", __DIR__)

defmodule Ferricstore.Commands.BitmapTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bitmap
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.Set
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  # ---------------------------------------------------------------------------
  # SETBIT
  # ---------------------------------------------------------------------------

  use Ferricstore.Commands.BitmapTest.Sections.Part01

  use Ferricstore.Commands.BitmapTest.Sections.Part02
end
