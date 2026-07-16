defmodule Ferricstore.Flow.Governance.AtomicRecord do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Store.Router

  @default_max_retries 16
  @max_retries 128
  @max_encoded_bytes 900_000

  def mutate(ctx, key, decode, encode, init, mutate, opts \\ [])

  def mutate(ctx, key, decode, encode, init, mutate, opts)
      when is_binary(key) and is_function(decode, 1) and is_function(encode, 1) and
             is_function(init, 0) and is_function(mutate, 1) and is_list(opts) do
    if Keyword.keyword?(opts) do
      max_retries = Keyword.get(opts, :max_retries, @default_max_retries)

      if is_integer(max_retries) and max_retries > 0 and max_retries <= @max_retries do
        write_opts = %{
          retention_owner: Keyword.get(opts, :flow_retention_owner),
          catalog_kind: Keyword.get(opts, :catalog_kind)
        }

        do_mutate(ctx, key, decode, encode, init, mutate, max_retries, write_opts)
      else
        {:error, "ERR invalid governance atomic record options"}
      end
    else
      {:error, "ERR invalid governance atomic record options"}
    end
  end

  def mutate(_ctx, _key, _decode, _encode, _init, _mutate, _opts),
    do: {:error, "ERR invalid governance atomic record options"}

  defp do_mutate(_ctx, _key, _decode, _encode, _init, _mutate, retries_left, _write_opts)
       when retries_left <= 0 do
    {:error,
     Decision.conflict(%{
       message: "Governance record changed too often; retry command",
       policy: "governance_atomic_update"
     })}
  end

  defp do_mutate(ctx, key, decode, encode, init, mutate, retries_left, write_opts) do
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
              write_opts
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
            write_opts
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
         write_opts
       ) do
    case mutation_result(mutate.(record)) do
      {:return, reply} ->
        reply

      {:write, updated, reply} ->
        with {:ok, encoded} <- encode_record(encode, updated),
             :ok <- Catalog.register(ctx, write_opts.catalog_kind, key) do
          set_opts = %{
            expire_at_ms: 0,
            nx: true,
            xx: false,
            get: false,
            keepttl: false,
            flow_retention_owner: write_opts.retention_owner
          }

          case Router.set(ctx, key, encoded, set_opts) do
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
                write_opts
              )

            {:error, _reason} = error ->
              error
          end
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
         write_opts
       ) do
    case mutation_result(mutate.(record)) do
      {:return, reply} ->
        reply

      {:write, updated, reply} ->
        with {:ok, encoded} <- encode_record(encode, updated) do
          case Router.cas(ctx, key, expected, encoded, nil) do
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
                write_opts
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
                write_opts
              )

            {:error, _reason} = error ->
              error
          end
        end
    end
  end

  defp encode_record(encode, record) do
    case encode.(record) do
      encoded when is_binary(encoded) and byte_size(encoded) <= @max_encoded_bytes ->
        {:ok, encoded}

      encoded when is_binary(encoded) ->
        {:error, "ERR governance record exceeds #{@max_encoded_bytes}-byte durable limit"}

      _invalid ->
        {:error, "ERR governance record encoder returned an invalid value"}
    end
  rescue
    _error -> {:error, "ERR governance record encoder failed"}
  catch
    _kind, _reason -> {:error, "ERR governance record encoder failed"}
  end

  defp mutation_result({:ok, updated}), do: {:write, updated, {:ok, updated}}
  defp mutation_result({:ok, updated, reply}), do: {:write, updated, {:ok, reply}}
  defp mutation_result({:error, reason}), do: {:return, {:error, reason}}
  defp mutation_result({:error, reason, updated}), do: {:write, updated, {:error, reason}}
end
