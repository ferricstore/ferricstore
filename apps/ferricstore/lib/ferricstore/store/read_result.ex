defmodule Ferricstore.Store.ReadResult do
  @moduledoc false

  @type failure :: {:error, {:storage_read_failed, term()}}

  @spec failure(term()) :: failure()
  def failure(reason), do: {:error, {:storage_read_failed, reason}}

  @spec failure?(term()) :: boolean()
  def failure?({:error, {:storage_read_failed, _reason}}), do: true
  def failure?(_result), do: false

  @spec first_failure(list()) :: failure() | nil
  def first_failure([{:error, {:storage_read_failed, _reason}} = failure | _rest]), do: failure
  def first_failure([_value | rest]), do: first_failure(rest)
  def first_failure([]), do: nil

  @spec command_error(failure()) :: {:error, binary()}
  def command_error({:error, {:storage_read_failed, _reason}}),
    do: {:error, "ERR storage read failed"}

  @spec command_result(term()) :: term()
  def command_result({:error, {:storage_read_failed, _reason}} = failure),
    do: command_error(failure)

  def command_result(result), do: result

  @spec map_success(term(), (term() -> term())) :: term()
  def map_success({:error, {:storage_read_failed, _reason}} = failure, _fun), do: failure
  def map_success(result, fun) when is_function(fun, 1), do: fun.(result)
end
