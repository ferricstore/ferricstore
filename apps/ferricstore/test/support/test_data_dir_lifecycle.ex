defmodule Ferricstore.Test.DataDirLifecycle do
  @moduledoc false

  @generated_prefix "ferricstore_test_"
  @applications [:ferricstore_server, :ferricstore]

  def register_generated_cleanup do
    data_dir = Application.fetch_env!(:ferricstore, :data_dir)

    if Application.get_env(:ferricstore, :test_data_dir_auto_cleanup, false) do
      register_cleanup(data_dir)
    else
      :ok
    end
  end

  def cleanup_generated(data_dir) do
    cleanup_generated(data_dir, &Application.stop/1, &File.rm_rf/1)
  end

  def cleanup_generated(data_dir, stop, remove)
      when is_binary(data_dir) and is_function(stop, 1) and is_function(remove, 1) do
    if safe_generated_data_dir?(data_dir) do
      with :ok <- stop_applications(stop),
           :ok <- remove_data_dir(data_dir, remove) do
        :ok
      end
    else
      {:error, :unsafe_data_dir}
    end
  end

  defp register_cleanup(data_dir) do
    if safe_generated_data_dir?(data_dir) do
      registration_key = {__MODULE__, :registered, Path.expand(data_dir)}

      case :persistent_term.get(registration_key, false) do
        false ->
          :persistent_term.put(registration_key, true)

          System.at_exit(fn _status ->
            _ = cleanup_generated(data_dir)
            :ok
          end)

        true ->
          :ok
      end
    else
      {:error, :unsafe_data_dir}
    end
  end

  defp safe_generated_data_dir?(data_dir) do
    expanded = Path.expand(data_dir)

    Path.dirname(expanded) == Path.expand(System.tmp_dir!()) and
      String.starts_with?(Path.basename(expanded), @generated_prefix)
  end

  defp stop_applications(stop) do
    Enum.reduce_while(@applications, :ok, fn app, :ok ->
      case stop.(app) do
        :ok -> {:cont, :ok}
        {:error, {:not_started, ^app}} -> {:cont, :ok}
        other -> {:halt, {:error, {:application_stop_failed, app, other}}}
      end
    end)
  end

  defp remove_data_dir(data_dir, remove) do
    case remove.(data_dir) do
      :ok -> :ok
      {:ok, _removed} -> :ok
      other -> {:error, {:data_dir_remove_failed, other}}
    end
  end
end
