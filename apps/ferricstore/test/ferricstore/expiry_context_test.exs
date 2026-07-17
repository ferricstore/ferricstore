defmodule Ferricstore.ExpiryContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.CommandTime
  alias Ferricstore.ExpiryContext

  setup do
    ref = :persistent_term.get(:ferricstore_hlc_ref)
    previous = :atomics.get(ref, 1)

    on_exit(fn ->
      :atomics.put(:persistent_term.get(:ferricstore_hlc_ref), 1, previous)
    end)

    {:ok, ref: ref}
  end

  test "captures one read-only request clock snapshot", %{ref: ref} do
    physical_ms = System.os_time(:millisecond) + 60_000
    packed = Bitwise.bor(Bitwise.bsl(physical_ms, 16), 17)
    :atomics.put(ref, 1, packed)

    assert {:request, ^physical_ms, wall_ms} = ExpiryContext.capture()
    assert wall_ms < physical_ms
    assert :atomics.get(ref, 1) == packed
  end

  test "distinguishes unsafe HLC-only expiry from wall-clock expiry" do
    context = {:request, 61_000, 1_000}

    assert ExpiryContext.classify(context, 31_000) ==
             {:unsafe, :hlc_drift_exceeded}

    assert ExpiryContext.classify(context, 999) == :expired
    assert ExpiryContext.classify(context, 61_001) == :live
    assert ExpiryContext.classify(context, 0) == :live
  end

  test "replicated apply classifies only by its immutable stamped time", %{ref: ref} do
    :atomics.put(ref, 1, Bitwise.bsl(System.os_time(:millisecond) + 60_000, 16))

    assert CommandTime.with_now_ms(2_000, fn ->
             context = ExpiryContext.capture()
             {context, ExpiryContext.classify(context, 1_500)}
           end) == {{:replicated_apply, 2_000}, :expired}
  end

  test "replicated apply reproduces the leader's unsafe expiry decision" do
    assert CommandTime.with_expiry_context(61_000, 1_000, fn ->
             context = ExpiryContext.capture()
             {context, ExpiryContext.classify(context, 31_000)}
           end) ==
             {{:replicated_apply, 61_000, 1_000}, {:unsafe, :hlc_drift_exceeded}}
  end
end
