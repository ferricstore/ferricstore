Code.require_file("bitmap_test/sections/setbit.exs", __DIR__)
Code.require_file("bitmap_test/sections/bitop_xor.exs", __DIR__)

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

  use Ferricstore.Commands.BitmapTest.Sections.Setbit

  use Ferricstore.Commands.BitmapTest.Sections.BitopXor
end
