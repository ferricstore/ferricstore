defmodule Ferricstore.Flow.Schedule.ResourceGovernance do
  @moduledoc false

  alias FerricStore.ResourceLimits

  @command "FLOW.CREATE"
  @default_state "queued"

  @type create_result(result) :: {:created, result} | {:existing, result}

  @spec create(
          FerricStore.Instance.t(),
          binary(),
          map(),
          (-> create_result(result))
        ) :: result | {:error, binary()}
        when result: term()
  def create(ctx, target_id, target, fun)
      when is_binary(target_id) and is_map(target) and is_function(fun, 0) do
    implementation = ResourceLimits.implementation()

    if implementation == FerricStore.ResourceLimits.Default do
      execute(fun, nil, nil)
    else
      key = Map.get(target, :partition_key) || target_id
      args = [target_id, Map.get(target, :type), Map.get(target, :state, @default_state)]

      opts = [
        impl: implementation,
        store: ctx,
        flow_create_count: 1,
        scheduled: true
      ]

      case ResourceLimits.acquire_command(@command, args, [key], opts) do
        {:ok, lease} -> execute(fun, {key, opts}, lease)
        {:error, reason} -> {:error, ResourceLimits.error_message(reason)}
      end
    end
  rescue
    _error -> {:error, ResourceLimits.error_message(:resource_limit_check_failed)}
  catch
    _kind, _reason -> {:error, ResourceLimits.error_message(:resource_limit_check_failed)}
  end

  defp execute(fun, activity, lease) do
    try do
      case fun.() do
        {:created, result} ->
          record_activity(activity)
          result

        {:existing, result} ->
          result
      end
    after
      release(lease, activity)
    end
  end

  defp record_activity(nil), do: :ok

  defp record_activity({key, opts}) do
    ResourceLimits.record_activity([key], Keyword.put(opts, :command_checked, true))
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp release(nil, _activity), do: :ok

  defp release(lease, {_key, opts}) do
    ResourceLimits.release_command(lease, opts)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end
end
