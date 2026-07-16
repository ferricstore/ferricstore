defmodule Ferricstore.Flow.PayloadReturn do
  @moduledoc false

  import Ferricstore.Flow.Options, only: [optional_boolean: 3, optional_non_neg_integer: 3]

  @default_max_bytes 64 * 1024

  @spec options(keyword(), boolean()) ::
          {:ok, %{enabled?: boolean(), max_bytes: non_neg_integer()}} | {:error, binary()}
  def options(opts, default_enabled?) do
    with {:ok, full?} <- optional_boolean(opts, :full, default_enabled?),
         {:ok, enabled?} <- optional_boolean(opts, :payload, full?),
         {:ok, max_bytes} <- bounded_max_bytes(opts) do
      {:ok, %{enabled?: enabled?, max_bytes: max_bytes}}
    end
  end

  @spec history_options(keyword()) ::
          {:ok, %{enabled?: boolean(), max_bytes: non_neg_integer()}} | {:error, binary()}
  def history_options(opts) do
    with {:ok, enabled?} <- optional_boolean(opts, :values, false),
         {:ok, max_bytes} <- bounded_max_bytes(opts) do
      {:ok, %{enabled?: enabled?, max_bytes: max_bytes}}
    end
  end

  @spec max_bytes() :: non_neg_integer()
  def max_bytes do
    case Application.get_env(:ferricstore, :flow_payload_return_max_bytes, @default_max_bytes) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> @default_max_bytes
    end
  end

  defp bounded_max_bytes(opts) do
    configured_max = max_bytes()

    with {:ok, requested_max} <-
           optional_non_neg_integer(opts, :payload_max_bytes, configured_max) do
      if requested_max <= configured_max do
        {:ok, requested_max}
      else
        {:error, "ERR flow payload_max_bytes exceeds maximum #{configured_max}"}
      end
    end
  end
end
