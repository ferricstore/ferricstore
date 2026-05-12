defmodule Ferricstore.Raft.PerfToggles do
  @moduledoc false

  @spec pipeline_priority() :: :low | :normal
  def pipeline_priority do
    case Application.get_env(:ferricstore, :raft_pipeline_priority, :low) do
      :low -> :low
      "low" -> :low
      :normal -> :normal
      "normal" -> :normal
      other -> invalid!(:raft_pipeline_priority, other, "low|normal")
    end
  end

  @spec direct_batch_commands?() :: boolean()
  def direct_batch_commands? do
    boolean_env!(:raft_direct_batch_commands, true)
  end

  @spec compact_hot_batches?() :: boolean()
  def compact_hot_batches? do
    boolean_env!(:raft_compact_hot_batches, true)
  end

  @spec put_batch_apply_fast_path?() :: boolean()
  def put_batch_apply_fast_path? do
    boolean_env!(:raft_put_batch_apply_fast_path, true)
  end

  @spec delete_batch_apply_fast_path?() :: boolean()
  def delete_batch_apply_fast_path? do
    boolean_env!(:raft_delete_batch_apply_fast_path, true)
  end

  defp boolean_env!(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      true -> true
      false -> false
      "true" -> true
      "TRUE" -> true
      "1" -> true
      "false" -> false
      "FALSE" -> false
      "0" -> false
      other -> invalid!(key, other, "true|false")
    end
  end

  defp invalid!(key, value, expected) do
    raise ArgumentError,
          "#{key} must be #{expected}, got: #{inspect(value)}"
  end
end
