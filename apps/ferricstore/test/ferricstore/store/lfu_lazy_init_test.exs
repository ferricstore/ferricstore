defmodule Ferricstore.Store.LFULazyInitTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.LFU

  @initial_ref_key :ferricstore_lfu_initial_ref

  test "initial/0 lazily initializes its atomics cache when app startup has not run" do
    original = :persistent_term.get(@initial_ref_key, :missing)

    if original != :missing do
      :persistent_term.erase(@initial_ref_key)
    end

    try do
      packed = LFU.initial()

      assert is_integer(packed)
      assert :persistent_term.get(@initial_ref_key, nil) != nil
    after
      if original != :missing do
        :persistent_term.put(@initial_ref_key, original)
      end
    end
  end
end
