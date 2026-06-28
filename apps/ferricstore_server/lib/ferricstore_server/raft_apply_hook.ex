defmodule FerricstoreServer.RaftApplyHook do
  @moduledoc false

  @hooks_key :raft_apply_hooks
  @unknown_results [
    {:error, :unknown_acl_command},
    {:error, :unknown_enterprise_server_command}
  ]

  @spec register((term() -> term())) :: :ok
  def register(hook) when is_function(hook, 1) do
    hooks =
      :ferricstore
      |> Application.get_env(@hooks_key, [])
      |> List.wrap()
      |> Enum.filter(&is_function(&1, 1))

    Application.put_env(:ferricstore, @hooks_key, Enum.uniq([hook | hooks]))
    :ok
  end

  @spec install_instance(atom()) :: :ok
  def install_instance(name \\ :default) when is_atom(name) do
    FerricStore.Instance.inject_callbacks(name, raft_apply_hook: compose_current(name))
    :ok
  rescue
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  @spec compose_current(atom()) :: (term() -> term())
  def compose_current(name \\ :default) when is_atom(name), do: compose(current_hook(name))

  @spec compose(nil | (term() -> term())) :: (term() -> term())
  def compose(previous_hook \\ nil) do
    hooks = [(&FerricstoreServer.Acl.handle_raft_command/1) | registered_hooks()]
    hooks = if is_function(previous_hook, 1), do: hooks ++ [previous_hook], else: hooks

    fn command -> run_hooks(hooks, command) end
  end

  defp registered_hooks do
    :ferricstore
    |> Application.get_env(@hooks_key, [])
    |> List.wrap()
    |> Enum.filter(&is_function(&1, 1))
  end

  defp current_hook(name) do
    case FerricStore.Instance.get(name) do
      %{raft_apply_hook: hook} when is_function(hook, 1) -> hook
      _other -> nil
    end
  rescue
    ArgumentError -> nil
  catch
    :exit, _reason -> nil
  end

  defp run_hooks([], _command), do: {:error, :unknown_server_command}

  defp run_hooks([hook | rest], command) do
    case hook.(command) do
      result when result in @unknown_results -> run_hooks(rest, command)
      result -> result
    end
  end
end
