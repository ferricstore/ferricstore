defmodule Ferricstore.Store.DiskPressureTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.DiskPressure

  test "operational pressure does not clear io pressure" do
    DiskPressure.init(1)

    DiskPressure.set(0)
    assert DiskPressure.under_pressure?(0)

    DiskPressure.set_operational(0)
    DiskPressure.clear_operational(0)
    assert DiskPressure.under_pressure?(0)

    DiskPressure.clear(0)
    refute DiskPressure.under_pressure?(0)
  end

  test "io pressure clear does not clear operational pressure" do
    DiskPressure.init(1)

    DiskPressure.set_operational(0)
    DiskPressure.set(0)
    DiskPressure.clear(0)
    assert DiskPressure.under_pressure?(0)

    DiskPressure.clear_operational(0)
    refute DiskPressure.under_pressure?(0)
  end

  test "default operational pressure never leaks into a custom instance" do
    DiskPressure.init(1)
    custom_ctx = %{name: :embedded_store, disk_pressure: :atomics.new(1, signed: false)}
    default_ctx = %{name: :default, disk_pressure: :atomics.new(1, signed: false)}

    DiskPressure.set_operational(0)

    assert DiskPressure.under_pressure?(default_ctx, 0)
    refute DiskPressure.under_pressure?(custom_ctx, 0)

    DiskPressure.clear_operational(0)
  end
end
