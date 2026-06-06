defmodule Ferricstore.Flow.LMDBWriter.ConfigTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDBWriter.Config

  test "instance name prefers explicit opts over context" do
    assert Config.instance_name_from_opts(instance_name: :custom, instance_ctx: %{name: :ctx}) ==
             :custom
  end

  test "instance name falls back to context then default" do
    assert Config.instance_name_from_opts(instance_ctx: %{name: :ctx}) == :ctx
    assert Config.instance_name_from_opts([]) == :default
    assert Config.instance_name_from_ctx(%{name: :ctx}) == :ctx
    assert Config.instance_name_from_ctx(%{}) == :default
  end

  test "default writer config matches lagged defaults" do
    assert Config.default_flush_interval_ms() == 500
    assert Config.default_flush_jitter_ms() == 250
    assert Config.default_max_ops() == 25_000
    assert Config.default_flush_on_max_ops(:lagged) == false
    assert Config.default_flush_chunk_pause_ms() == 1
  end
end
