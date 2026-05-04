defmodule Ferricstore.Raft.SafeRaLogger do
  @moduledoc """
  Logger delegate for Ra internals.

  Ra normally lets applications provide a logger module via `:ra,
  :logger_module`, but some server paths reset the logger back to OTP's
  `:logger`. The primary filter catches those raw OTP logger events too.

  Only known malformed upstream log events are sanitized; normal Ra
  debug/info/warning output is preserved unchanged.
  """

  @snapshot_written_format ~c"~ts: ra_log: ~s with ~b bytes written at index ~b with ~b live indexes in ~bms"
  @snapshot_written_safe_format ~c"~ts: ra_log: ~s with ~p bytes written at index ~b with ~b live indexes in ~bms"
  @primary_filter_id :ferricstore_safe_ra_logger

  @spec install_filter() :: :ok
  def install_filter do
    case :logger.add_primary_filter(@primary_filter_id, {&__MODULE__.filter/2, []}) do
      :ok -> :ok
      {:error, {:already_exist, @primary_filter_id}} -> :ok
    end
  end

  @spec filter(:logger.log_event(), term()) :: :logger.log_event() | :stop | :ignore
  def filter(%{msg: {format, args}, meta: metadata} = event, _arg) do
    {format, args} = normalize(format, args, metadata)
    %{event | msg: {format, args}}
  end

  def filter(event, _arg), do: event

  @spec log(:logger.level(), :io.format(), list(), map()) :: :ok
  def log(level, format, args, metadata) do
    {format, args} = normalize(format, args, metadata)
    :logger.log(level, format, args, metadata)
  end

  defp normalize(@snapshot_written_format, [log_id, kind, :undefined, index, live, duration], %{
         domain: [:ra],
         mfa: {:ra_log, :handle_event, 2}
       }) do
    {@snapshot_written_safe_format, [log_id, kind, :undefined, index, live, duration]}
  end

  defp normalize(format, args, _metadata), do: {format, args}
end
