defmodule Ferricstore.Flow.LMDBWriter.TimerTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDBWriter.Timer

  test "timer delay saturates at the runtime timer limit" do
    assert Timer.__bounded_delay_for_test__(4_294_967_295, 4_294_967_295) ==
             4_294_967_295

    assert Timer.__bounded_delay_for_test__(500, 250) == 750
  end
end
