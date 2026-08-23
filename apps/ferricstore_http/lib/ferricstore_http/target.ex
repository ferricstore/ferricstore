defmodule FerricstoreHttp.Target do
  @moduledoc "Behaviour for asynchronous invocation target adapters."

  @callback invoke(map(), map(), keyword()) ::
              {:ok, term()} | {:retry, term()} | {:error, term()}
end
