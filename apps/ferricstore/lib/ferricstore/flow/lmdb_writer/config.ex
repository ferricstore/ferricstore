defmodule Ferricstore.Flow.LMDBWriter.Config do
  @moduledoc false

  @default_lagged_flush_interval_ms 500
  @default_lagged_flush_jitter_ms 250
  @default_lagged_max_ops 25_000
  @default_lagged_flush_chunk_pause_ms 1

  def instance_name_from_opts(opts) do
    case {Keyword.get(opts, :instance_name), Keyword.get(opts, :instance_ctx)} do
      {name, _ctx} when is_atom(name) and not is_nil(name) -> name
      {_name, %{name: name}} when is_atom(name) and not is_nil(name) -> name
      _ -> :default
    end
  end

  def instance_name_from_ctx(%{name: name}) when is_atom(name) and not is_nil(name), do: name
  def instance_name_from_ctx(_ctx), do: :default

  def default_flush_interval_ms, do: @default_lagged_flush_interval_ms
  def default_max_ops, do: @default_lagged_max_ops
  def default_flush_on_max_ops(_mode), do: false
  def default_flush_jitter_ms, do: @default_lagged_flush_jitter_ms
  def default_flush_chunk_pause_ms, do: @default_lagged_flush_chunk_pause_ms
end
