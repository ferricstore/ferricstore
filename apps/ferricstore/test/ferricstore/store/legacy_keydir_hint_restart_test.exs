defmodule Ferricstore.Store.LegacyKeydirHintRestartTest do
  @moduledoc """
  Releases before 0.12.7 wrote a shard's shutdown hint from its keydir: every
  row in file 0, among them the rows of a promoted collection's dedicated
  file, which is numbered 0 too. The hint's metadata matches the shared log,
  so the next start loaded those rows as shared-log rows and stopped on the
  collection's type marker ("failed to rebuild logical key index on shard N:
  :invalid_type_metadata_values"). A start must not trust a hint those
  releases wrote.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.{CompoundKey, HintFile, HintMetadata, Router}
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Test.ShardHelpers

  @threshold 5

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: @threshold)

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(snapshot)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    %{isolated: isolated}
  end

  test "a start ignores a keydir-built hint that lists a promoted collection's rows",
       %{isolated: isolated} do
    ctx = FerricStore.Instance.get(:default)
    key = "{legacy-hint}:index"
    # Enough members that the dedicated file's marker lies past the shared
    # log's tombstone for it (each dedicated record carries a score), so that
    # tombstone cannot mask the hint's row.
    count = 150
    members = Enum.map(1..count, &("m" <> String.pad_leading(Integer.to_string(&1), 3, "0")))

    assert ^count =
             Dispatcher.dispatch("ZADD", [key | Enum.flat_map(members, &["0", &1])], ctx)

    shard = Router.shard_for(ctx, key)
    assert GenServer.call(Router.shard_name(ctx, shard), {:promoted?, key})

    # What a release before 0.12.7 wrote at shutdown: the keydir's file-0 rows,
    # the dedicated file's type marker among them.
    file_zero_rows =
      :ets.select(elem(ctx.keydir_refs, shard), [{{:_, :_, :_, :_, 0, :_, :_}, [], [:"$_"]}])

    assert Enum.any?(file_zero_rows, &(elem(&1, 0) == CompoundKey.type_key(key)))

    shard_path = Ferricstore.DataDir.shard_data_path(isolated.tmp_dir, shard)
    hint_path = Path.join(shard_path, "00000.hint")

    stop_store!()

    keydir_copy = :ets.new(:legacy_keydir_rows, [:set, :public])
    true = :ets.insert(keydir_copy, file_zero_rows)
    assert :ok = HintFile.write_from_keydir(hint_path, keydir_copy, 0)
    publish_as_legacy!(hint_path)

    # The shared log ends with the marker's tombstone (promotion moved the
    # collection out), so recovering the shared keydir leaves no marker row.
    recovered = :ets.new(:recovered_keydir, [:set, :public])
    :ok = ShardLifecycle.recover_keydir(shard_path, recovered, shard)
    assert [] == :ets.lookup(recovered, CompoundKey.type_key(key))

    ShardHelpers.restart_current_data_dir(isolated)

    assert count == Dispatcher.dispatch("ZCARD", [key], ctx())
    assert Enum.take(members, 3) == Dispatcher.dispatch("ZRANGE", [key, "0", "2"], ctx())
    assert {:simple, "zset"} == Dispatcher.dispatch("TYPE", [key], ctx())
  end

  defp ctx, do: FerricStore.Instance.get(:default)

  defp stop_store! do
    _ = Application.stop(:ferricstore_server)
    :ok = Application.stop(:ferricstore)
    Ferricstore.Raft.WARaftBackend.stop()
  end

  # Releases before 0.12.7 published version 1 hint metadata.
  @legacy_version 1

  defp publish_as_legacy!(hint_path) do
    meta_path = HintMetadata.metadata_path(hint_path)

    <<"FSHM", size::unsigned-big-32, payload::binary-size(size), _crc::unsigned-big-32>> =
      File.read!(meta_path)

    {:ferricstore_hint_metadata, _version, file_id, source, hint} =
      :erlang.binary_to_term(payload)

    term =
      :erlang.term_to_binary(
        {:ferricstore_hint_metadata, @legacy_version, file_id, source, hint},
        [:deterministic]
      )

    File.write!(
      meta_path,
      <<"FSHM", byte_size(term)::unsigned-big-32, term::binary,
        :erlang.crc32(term)::unsigned-big-32>>
    )
  end
end
