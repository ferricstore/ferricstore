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

  test "reads the snapshot each event follows, and events older releases wrote" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_query_journal_#{System.unique_integer([:positive, :monotonic])}"
      )

    ctx = %{data_dir: data_dir}
    on_exit(fn -> File.rm_rf!(data_dir) end)

    legacy_payload =
      Ferricstore.TermCodec.encode({:flow_query_index_registry_progress, 1, {:legacy_event}})

    legacy_frame =
      <<byte_size(legacy_payload)::unsigned-big-32, :crypto.hash(:sha256, legacy_payload)::binary,
        legacy_payload::binary>>

    File.mkdir_p!(Path.dirname(IndexRegistryJournal.path(ctx)))
    File.write!(IndexRegistryJournal.path(ctx), legacy_frame)

    digest = :crypto.hash(:sha256, "snapshot")
    assert :ok = IndexRegistryJournal.append(ctx, digest, {:current_event})

    assert {:ok, [{:legacy, {:legacy_event}}, {^digest, {:current_event}}]} =
             IndexRegistryJournal.read(ctx)
  end
end
