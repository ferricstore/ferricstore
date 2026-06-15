defmodule Ferricstore.Flow.PipelineWriteCommand do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  def command({:create, id, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.create_attrs.(id, opts) do
      state_command(:flow_create, attrs)
    end
  end

  def command({:flow_create, id, opts}, callbacks) do
    command({:create, id, opts}, callbacks)
  end

  def command({:start_and_claim, id, type, initial_state, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.start_and_claim_attrs.(id, type, initial_state, opts) do
      state_command(:flow_start_and_claim, attrs)
    end
  end

  def command({:flow_start_and_claim, id, type, initial_state, opts}, callbacks) do
    command({:start_and_claim, id, type, initial_state, opts}, callbacks)
  end

  def command({:transition, id, from_state, to_state, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.transition_attrs.(id, from_state, to_state, opts) do
      state_command(:flow_transition, attrs)
    end
  end

  def command({:flow_transition, id, from_state, to_state, opts}, callbacks) do
    command({:transition, id, from_state, to_state, opts}, callbacks)
  end

  def command({:step_continue, id, lease_token, from_state, to_state, opts}, callbacks) do
    with {:ok, attrs} <-
           callbacks.step_continue_attrs.(id, lease_token, from_state, to_state, opts) do
      state_command(:flow_step_continue, attrs)
    end
  end

  def command({:flow_step_continue, id, lease_token, from_state, to_state, opts}, callbacks) do
    command({:step_continue, id, lease_token, from_state, to_state, opts}, callbacks)
  end

  def command({:named_value_put, value, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.named_value_attrs.(value, opts) do
      state_command(:flow_named_value_put, attrs)
    end
  end

  def command({:flow_named_value_put, value, opts}, callbacks) do
    command({:named_value_put, value, opts}, callbacks)
  end

  def command({:signal, id, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.signal_attrs.(id, opts) do
      state_command(:flow_signal, attrs)
    end
  end

  def command({:flow_signal, id, opts}, callbacks) do
    command({:signal, id, opts}, callbacks)
  end

  def command({:complete, id, lease_token, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.complete_attrs.(id, lease_token, opts) do
      {:ok, :terminal, {:complete, attrs}}
    end
  end

  def command({:flow_complete, id, lease_token, opts}, callbacks) do
    command({:complete, id, lease_token, opts}, callbacks)
  end

  def command({:retry, id, lease_token, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.retry_attrs.(id, lease_token, opts) do
      {:ok, :terminal, {:retry, attrs}}
    end
  end

  def command({:flow_retry, id, lease_token, opts}, callbacks) do
    command({:retry, id, lease_token, opts}, callbacks)
  end

  def command({:fail, id, lease_token, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.fail_attrs.(id, lease_token, opts) do
      {:ok, :terminal, {:fail, attrs}}
    end
  end

  def command({:flow_fail, id, lease_token, opts}, callbacks) do
    command({:fail, id, lease_token, opts}, callbacks)
  end

  def command({:cancel, id, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.cancel_attrs.(id, opts) do
      {:ok, :terminal, {:cancel, attrs}}
    end
  end

  def command({:flow_cancel, id, opts}, callbacks) do
    command({:cancel, id, opts}, callbacks)
  end

  def command({:rewind, id, opts}, callbacks) do
    with {:ok, attrs} <- callbacks.rewind_attrs.(id, opts) do
      state_command(:flow_rewind, attrs)
    end
  end

  def command({:flow_rewind, id, opts}, callbacks) do
    command({:rewind, id, opts}, callbacks)
  end

  def command(_op, _callbacks), do: {:error, "ERR unsupported flow pipeline command"}

  defp state_command(command, %{id: id, partition_key: partition_key} = attrs) do
    key = Keys.state_key(id, partition_key)
    {:ok, :state, {key, {command, key, attrs}}}
  end
end
