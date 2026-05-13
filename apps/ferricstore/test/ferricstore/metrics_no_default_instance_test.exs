defmodule Ferricstore.MetricsNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Metrics

  @default_key {FerricStore.Instance, :default}

  setup do
    original = :persistent_term.get(@default_key, :missing)
    :persistent_term.erase(@default_key)

    on_exit(fn ->
      case original do
        :missing -> :persistent_term.erase(@default_key)
        ctx -> :persistent_term.put(@default_key, ctx)
      end
    end)

    :ok
  end

  test "scrape is available before a default instance is registered" do
    text = Metrics.scrape()

    assert text =~ "ferricstore_total_commands_processed 0"
    assert text =~ "ferricstore_hot_reads_total 0"
    assert text =~ "ferricstore_blob_files 0"
    assert text =~ "ferricstore_bitcask_release_cursor_gap"
  end
end
