defmodule Ferricstore.Flow.Governance.AtomicRecord do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision
  alias Ferricstore.Store.Router

  @default_max_retries 16

  def mutate(ctx, key, decode, encode, init, mutate, opts \\ [])
      when is_binary(key) and is_function(decode, 1) and is_function(encode, 1) and
             is_function(init, 0) and is_function(mutate, 1) and is_list(opts) do
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    retention_owner = Keyword.get(opts, :flow_retention_owner)
    do_mutate(ctx, key, decode, encode, init, mutate, max_retries, retention_owner)
  end

  defp do_mutate(_ctx, _key, _decode, _encode, _init, _mutate, retries_left, _retention_owner)
       when retries_left <= 0 do
    {:error,
     Decision.conflict(%{
       message: "Governance record changed too often; retry command",
       policy: "governance_atomic_update"
     })}
  end

  defp do_mutate(ctx, key, decode, encode, init, mutate, retries_left, retention_owner) do
    case Router.get(ctx, key) do
      nil ->
        case init.() do
          {:return, reply} ->
            reply

          {:ok, record} ->
            create_record(
              ctx,
              key,
              decode,
              encode,
              init,
              mutate,
              record,
              retries_left,
              retention_owner
            )

          {:error, _reason} = error ->
            error
        end

      value when is_binary(value) ->
        with {:ok, record} <- decode.(value) do
          update_record(
            ctx,
            key,
            decode,
            encode,
            init,
            mutate,
            value,
            record,
            retries_left,
            retention_owner
          )
        end

      _other ->
        {:error, "ERR governance record is corrupt"}
    end
  end

  defp create_record(
         ctx,
         key,
         decode,
         encode,
         init,
         mutate,
         record,
         retries_left,
         retention_owner
       ) do
    case mutation_result(mutate.(record)) do
      {:return, reply} ->
        reply

      {:write, updated, reply} ->
        set_opts = %{
          expire_at_ms: 0,
          nx: true,
          xx: false,
          get: false,
          keepttl: false,
          flow_retention_owner: retention_owner
        }

        case Router.set(ctx, key, encode.(updated), set_opts) do
          :ok ->
            reply

          nil ->
            do_mutate(
              ctx,
              key,
              decode,
              encode,
              init,
              mutate,
              retries_left - 1,
              retention_owner
            )

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp update_record(
         ctx,
         key,
         decode,
         encode,
         init,
         mutate,
         expected,
         record,
         retries_left,
         retention_owner
       ) do
    case mutation_result(mutate.(record)) do
      {:return, reply} ->
        reply

      {:write, updated, reply} ->
        case Router.cas(ctx, key, expected, encode.(updated), nil) do
          1 ->
            reply

          0 ->
            do_mutate(
              ctx,
              key,
              decode,
              encode,
              init,
              mutate,
              retries_left - 1,
              retention_owner
            )

          nil ->
            do_mutate(
              ctx,
              key,
              decode,
              encode,
              init,
              mutate,
              retries_left - 1,
              retention_owner
            )

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp mutation_result({:ok, updated}), do: {:write, updated, {:ok, updated}}
  defp mutation_result({:ok, updated, reply}), do: {:write, updated, {:ok, reply}}
  defp mutation_result({:error, reason}), do: {:return, {:error, reason}}
  defp mutation_result({:error, reason, updated}), do: {:write, updated, {:error, reason}}
end
