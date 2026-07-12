defmodule Ferricstore.Flow.Retention do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Store.Router

  @doc false
  def cleanup(ctx, opts \\ [])

  def cleanup(ctx, opts) when is_list(opts) do
    started = FlowTelemetry.start_time()

    result =
      with :ok <- validate_opts(opts),
           {:ok, limit} <- optional_pos_integer(opts, :limit, 100),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, continuation} <- optional_continuation(opts),
           :ok <- flush_lmdb_before_cleanup(ctx),
           :ok <- flush_history_before_cleanup(ctx),
           :ok <- flush_lmdb_before_cleanup(ctx) do
        Router.flow_retention_cleanup(ctx, %{
          limit: limit,
          now_ms: now,
          continuation: continuation
        })
      end

    FlowTelemetry.observe(:retention_cleanup, started, result, %{flow_id: nil})
  end

  def cleanup(_ctx, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp flush_lmdb_before_cleanup(%{name: name, shard_count: shard_count})
       when is_atom(name) and is_integer(shard_count) and shard_count >= 0 do
    case Ferricstore.Flow.LMDBWriter.flush_all(name, shard_count) do
      :ok -> :ok
      {:error, :writer_not_started} -> :ok
      {:error, {:noproc, _}} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp flush_lmdb_before_cleanup(_ctx), do: :ok

  defp flush_history_before_cleanup(%{shard_count: shard_count} = ctx)
       when is_integer(shard_count) and shard_count >= 0 do
    Enum.reduce_while(0..max(shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      case Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index, 120_000) do
        :ok -> {:cont, :ok}
        {:error, :not_started} -> {:cont, :ok}
        {:error, {:noproc, _}} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flush_history_before_cleanup(_ctx), do: :ok

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-negative integer"}
      :error when is_integer(default) and default >= 0 -> {:ok, default}
      :error when is_nil(default) -> {:ok, nil}
      :error -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  defp optional_continuation(opts) do
    case Keyword.get(opts, :continuation) do
      nil -> {:ok, nil}
      continuation when is_binary(continuation) -> {:ok, continuation}
      _invalid -> {:error, "ERR flow continuation must be a binary token"}
    end
  end

  defp now_ms, do: CommandTime.now_ms()
end
