defmodule Ferricstore.Flow.Query.IndexRegistryJournalTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.IndexRegistryJournal

  test "reading a missing journal initializes the durable empty stream" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_query_journal_#{System.unique_integer([:positive, :monotonic])}"
      )

    ctx = %{data_dir: data_dir}
    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert {:ok, []} = IndexRegistryJournal.read(ctx)
    assert File.read!(IndexRegistryJournal.path(ctx)) == ""
  end
end
