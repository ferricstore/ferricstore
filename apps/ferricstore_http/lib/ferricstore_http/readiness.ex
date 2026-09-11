defmodule FerricstoreHttp.Readiness do
  @moduledoc false

  alias FerricstoreHttp.{Admission, Config}

  @spec check(Config.t()) :: :ok | {:error, :not_ready}
  def check(%Config{} = config) do
    if Admission.stats() && config.backend.ready?(), do: :ok, else: {:error, :not_ready}
  rescue
    _error -> {:error, :not_ready}
  catch
    :exit, _reason -> {:error, :not_ready}
  end
end
