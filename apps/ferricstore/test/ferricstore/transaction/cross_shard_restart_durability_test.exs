defmodule Ferricstore.Transaction.RestartDurabilityTest do
  use ExUnit.Case, async: false

  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag timeout: 120_000

  alias Ferricstore.Commands.Server
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Test.PreparedTransactionCoordinator, as: Coordinator

  setup do
    original_max_active_file_size = Application.get_env(:ferricstore, :max_active_file_size)
    Application.put_env(:ferricstore, :max_active_file_size, 1_024)

    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      restore_env(:max_active_file_size, original_max_active_file_size)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    {:ok, isolated: isolated}
  end

  test "single-shard transaction survives SAVE and full app restart", %{isolated: isolated} do
    keys = same_shard_keys(3)

    assert [:ok, :ok, :ok] =
             Coordinator.execute(
               Enum.map(keys, fn key -> {"SET", [key, "tx-value:#{key}"]} end),
               %{},
               nil
             )

    assert :ok = Server.handle("SAVE", [], FerricStore.Instance.get(:default))

    restart_current_dir!(isolated)

    Enum.each(keys, fn key ->
      ShardHelpers.eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == "tx-value:#{key}" end,
        "transaction key #{inspect(key)} should survive restart",
        100,
        100
      )
    end)
  end

  test "single-shard transaction survives active-file rotation and full app restart", %{
    isolated: isolated
  } do
    keys = same_shard_keys(2)
    large = String.duplicate("x", 4_096)

    assert [:ok, :ok] =
             Coordinator.execute(
               Enum.map(keys, fn key -> {"SET", [key, large <> key]} end),
               %{},
               nil
             )

    assert :ok = Server.handle("SAVE", [], FerricStore.Instance.get(:default))

    keys
    |> Enum.map(&Router.shard_for(FerricStore.Instance.get(:default), &1))
    |> Enum.uniq()
    |> Enum.each(fn shard ->
      ShardHelpers.eventually(
        fn -> log_file_count(shard) >= 2 end,
        "shard #{shard} should rotate active Bitcask files before restart",
        50,
        100
      )

      ShardHelpers.eventually(
        fn -> shard_active_file_id(shard) >= 1 end,
        "shard #{shard} process should sync rotated active Bitcask file",
        50,
        100
      )
    end)

    restart_current_dir!(isolated)

    Enum.each(keys, fn key ->
      expected = large <> key

      ShardHelpers.eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == expected end,
        "rotated transaction key #{inspect(key)} should survive restart",
        100,
        100
      )
    end)
  end

  defp restart_current_dir!(isolated) do
    Ferricstore.Application.prep_stop(nil)
    ShardHelpers.restart_current_data_dir(isolated)
    ShardHelpers.wait_default_pipeline_ready(60_000)
    Ferricstore.Health.set_ready(true)
  end

  defp same_shard_keys(count) do
    tag = "tx-restart-#{System.unique_integer([:positive])}"
    Enum.map(1..count, &"{#{tag}}:#{&1}")
  end

  defp log_file_count(shard) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    data_dir
    |> Ferricstore.DataDir.shard_data_path(shard)
    |> File.ls()
    |> case do
      {:ok, files} ->
        Enum.count(files, fn file ->
          String.ends_with?(file, ".log") and not String.starts_with?(file, "compact_")
        end)

      {:error, _reason} ->
        0
    end
  end

  defp shard_active_file_id(shard) do
    FerricStore.Instance.get(:default)
    |> Router.shard_name(shard)
    |> :sys.get_state(5_000)
    |> Map.fetch!(:active_file_id)
  rescue
    _ -> -1
  catch
    :exit, _ -> -1
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
