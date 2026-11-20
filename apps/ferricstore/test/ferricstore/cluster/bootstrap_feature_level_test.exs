defmodule Ferricstore.Cluster.BootstrapFeatureLevelTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Cluster.BootstrapFeatureLevel
  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Test.ShardHelpers

  setup do
    previous = Application.get_env(:ferricstore, :bootstrap_feature_level)

    on_exit(fn ->
      if previous == nil,
        do: Application.delete_env(:ferricstore, :bootstrap_feature_level),
        else: Application.put_env(:ferricstore, :bootstrap_feature_level, previous)
    end)

    :ok
  end

  describe "a brand-new standalone node" do
    test "starts at the level its release supports" do
      Application.put_env(:ferricstore, :bootstrap_feature_level, :supported)
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)

      assert Ferricstore.Raft.WARaftBackend.fresh_standalone_bootstrap?()
      supported = FeatureLevel.supported()
      assert wait_active_level(supported) == supported
    end

    test "stays at level 0 when bootstrap_feature_level is 0" do
      Application.put_env(:ferricstore, :bootstrap_feature_level, 0)
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)

      Process.sleep(500)
      assert {:ok, %{active: 0}} = FeatureLevel.status(FerricStore.Instance.get(:default))
    end
  end

  test "a node restarted on existing data is not raised" do
    Application.put_env(:ferricstore, :bootstrap_feature_level, 0)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 0}} = FeatureLevel.status(FerricStore.Instance.get(:default))

    Application.put_env(:ferricstore, :bootstrap_feature_level, :supported)
    :ok = ShardHelpers.restart_current_data_dir(isolated)

    refute Ferricstore.Raft.WARaftBackend.fresh_standalone_bootstrap?()
    Process.sleep(500)
    assert {:ok, %{active: 0}} = FeatureLevel.status(FerricStore.Instance.get(:default))
  end

  describe "run/1" do
    test "raises a fresh node to the configured level, retrying until the raise succeeds" do
      parent = self()
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      raise_fun = fn :ctx, level ->
        send(parent, {:raise, level})

        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          0 -> {:error, :shards_not_ready}
          _ -> {:ok, %{active: level}}
        end
      end

      assert {:ok, %{active: 7}} =
               BootstrapFeatureLevel.run(
                 instance_ctx: :ctx,
                 fresh?: true,
                 level: 7,
                 raise_fun: raise_fun,
                 retry_ms: 1
               )

      assert_received {:raise, 7}
      assert_received {:raise, 7}
    end

    test "uses the supported level for :supported" do
      raise_fun = fn :ctx, level -> {:ok, %{active: level}} end
      supported = FeatureLevel.supported()

      assert {:ok, %{active: ^supported}} =
               BootstrapFeatureLevel.run(
                 instance_ctx: :ctx,
                 fresh?: true,
                 level: :supported,
                 raise_fun: raise_fun
               )
    end

    test "leaves a node alone when it is not fresh or the level is 0" do
      raise_fun = fn _ctx, _level -> flunk("must not raise") end

      assert :skipped =
               BootstrapFeatureLevel.run(
                 instance_ctx: :ctx,
                 fresh?: false,
                 level: :supported,
                 raise_fun: raise_fun
               )

      assert :skipped =
               BootstrapFeatureLevel.run(
                 instance_ctx: :ctx,
                 fresh?: true,
                 level: 0,
                 raise_fun: raise_fun
               )
    end

    test "gives up after its attempts and leaves the level unchanged" do
      raise_fun = fn _ctx, _level -> {:error, :members_unreachable} end

      assert {:error, :members_unreachable} =
               BootstrapFeatureLevel.run(
                 instance_ctx: :ctx,
                 fresh?: true,
                 level: 3,
                 raise_fun: raise_fun,
                 attempts: 3,
                 retry_ms: 1
               )
    end
  end

  defp wait_active_level(target, attempts \\ 100) do
    case FeatureLevel.status(FerricStore.Instance.get(:default)) do
      {:ok, %{active: ^target}} ->
        target

      {:ok, %{active: active}} when attempts == 0 ->
        active

      _other ->
        Process.sleep(100)
        wait_active_level(target, attempts - 1)
    end
  end
end
