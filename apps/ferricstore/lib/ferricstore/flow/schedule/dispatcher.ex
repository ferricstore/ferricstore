defmodule Ferricstore.Flow.Schedule.Dispatcher do
  @moduledoc false

  @type claim_fun(item) ::
          (pos_integer(), boolean() -> {:ok, [item]} | {:error, binary()})
  @type process_fun(item, result) :: (item -> result)

  @spec run(
          pos_integer(),
          pos_integer(),
          pos_integer(),
          claim_fun(item),
          process_fun(item, result)
        ) ::
          {:ok, [{item, result}], binary() | nil} | {:error, binary()}
        when item: term(), result: term()
  def run(total_limit, wave_size, concurrency, claim, process)
      when is_integer(total_limit) and total_limit > 0 and is_integer(wave_size) and
             wave_size > 0 and is_integer(concurrency) and concurrency > 0 and
             is_function(claim, 2) and is_function(process, 1) do
    do_run(total_limit, wave_size, concurrency, claim, process, [])
  end

  def run(_total_limit, _wave_size, _concurrency, _claim, _process),
    do: {:error, "ERR invalid schedule dispatch configuration"}

  defp do_run(remaining, wave_size, concurrency, claim, process, batches) do
    requested = min(remaining, wave_size)

    case claim.(requested, batches == []) do
      {:ok, records} when is_list(records) and length(records) <= requested ->
        results = process_batch(records, concurrency, process)
        batches = [results | batches]

        if records == [] or length(records) < requested or length(records) == remaining do
          {:ok, flatten_batches(batches), nil}
        else
          do_run(
            remaining - length(records),
            wave_size,
            concurrency,
            claim,
            process,
            batches
          )
        end

      {:ok, _invalid_records} ->
        finish_with_claim_error(batches, "ERR invalid schedule claim result")

      {:error, reason} when is_binary(reason) ->
        finish_with_claim_error(batches, reason)

      _invalid ->
        finish_with_claim_error(batches, "ERR invalid schedule claim result")
    end
  end

  defp process_batch([], _concurrency, _process), do: []

  defp process_batch([record], _concurrency, process),
    do: [{record, invoke(process, record)}]

  defp process_batch(records, concurrency, process) do
    task_results =
      Task.async_stream(records, &invoke(process, &1),
        max_concurrency: min(concurrency, length(records)),
        ordered: true,
        timeout: :infinity
      )

    records
    |> Enum.zip(task_results)
    |> Enum.map(fn
      {record, {:ok, result}} -> {record, result}
      {record, {:exit, _reason}} -> {record, {:error, :dispatch_task_failed}}
    end)
  end

  defp invoke(process, record) do
    process.(record)
  rescue
    _exception -> {:error, :dispatch_task_failed}
  catch
    _kind, _reason -> {:error, :dispatch_task_failed}
  end

  defp finish_with_claim_error([], reason), do: {:error, reason}
  defp finish_with_claim_error(batches, reason), do: {:ok, flatten_batches(batches), reason}

  defp flatten_batches(batches), do: batches |> Enum.reverse() |> List.flatten()
end
