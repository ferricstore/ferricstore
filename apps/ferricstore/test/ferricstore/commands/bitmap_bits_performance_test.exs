defmodule Ferricstore.Commands.Bitmap.BitsPerformanceTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bitmap.Bits

  test "bit-range count traverses full interior bytes instead of every bit" do
    binary = :binary.copy(<<0xFF>>, 128 * 1024)
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert bit_size(binary) - 6 ==
             Bits.bitcount_bit_range(binary, 3, bit_size(binary) - 4)

    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert after_reductions - before_reductions < 3_000_000
  end

  test "bit-range position skips whole bytes when the requested bit is absent" do
    binary = :binary.copy(<<0xFF>>, 128 * 1024)
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert -1 == Bits.bitpos_bit_range(binary, 0, 3, bit_size(binary) - 4)

    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert after_reductions - before_reductions < 3_000_000
  end
end
