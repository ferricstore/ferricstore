defmodule FerricstoreHttp.Admission do
  @moduledoc false

  use GenServer

  @table __MODULE__

  @spec start_link(pos_integer()) :: GenServer.on_start()
  def start_link(limit) when is_integer(limit) and limit > 0 do
    GenServer.start_link(__MODULE__, limit, name: __MODULE__)
  end

  @spec acquire() :: :ok | {:error, :request_limit}
  def acquire do
    current = :ets.update_counter(@table, :in_flight, {2, 1})
    limit = :ets.lookup_element(@table, :limit, 2)

    if current <= limit do
      :ok
    else
      _current = :ets.update_counter(@table, :in_flight, {2, -1, 0, 0})
      {:error, :request_limit}
    end
  rescue
    ArgumentError -> {:error, :request_limit}
  end

  @spec release() :: :ok
  def release do
    _current = :ets.update_counter(@table, :in_flight, {2, -1, 0, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec stats() :: %{in_flight: non_neg_integer(), limit: pos_integer()} | nil
  def stats do
    %{
      in_flight: :ets.lookup_element(@table, :in_flight, 2),
      limit: :ets.lookup_element(@table, :limit, 2)
    }
  rescue
    ArgumentError -> nil
  end

  @impl GenServer
  def init(limit) do
    table = :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    true = :ets.insert(table, [{:in_flight, 0}, {:limit, limit}])
    {:ok, table}
  end
end

defmodule FerricstoreHttp.Admission.StreamHandler do
  @moduledoc false

  @behaviour :cowboy_stream

  alias FerricstoreHttp.Admission

  @overloaded_body Jason.encode!(%{"error" => %{"code" => "server_overloaded"}})
  @request_line_too_large_body Jason.encode!(%{
                                 "error" => %{"code" => "request_line_too_large"}
                               })
  @request_headers_too_large_body Jason.encode!(%{
                                    "error" => %{"code" => "request_headers_too_large"}
                                  })

  @impl :cowboy_stream
  def init(stream_id, req, opts) do
    case request_limit_error(req, opts) do
      {:error, status, body} -> reject(status, body)
      :ok -> admit(stream_id, req, opts)
    end
  end

  defp request_limit_error(req, opts) do
    cond do
      request_line_too_large?(req, opts) ->
        {:error, 414, @request_line_too_large_body}

      request_headers_too_large?(req, opts) ->
        {:error, 431, @request_headers_too_large_body}

      true ->
        :ok
    end
  end

  defp reject(status, body) do
    headers = %{
      "cache-control" => "no-store",
      "content-length" => Integer.to_string(byte_size(body)),
      "content-type" => "application/json; charset=utf-8"
    }

    {[{:response, status, headers, body}, :stop], :rejected}
  end

  defp admit(stream_id, req, opts) do
    case Admission.acquire() do
      :ok ->
        {commands, next} = :cowboy_stream.init(stream_id, req, opts)
        {commands, {:accepted, next}}

      {:error, :request_limit} ->
        headers = %{
          "cache-control" => "no-store",
          "content-length" => Integer.to_string(byte_size(@overloaded_body)),
          "content-type" => "application/json; charset=utf-8",
          "retry-after" => "1"
        }

        {[{:response, 503, headers, @overloaded_body}, :stop], :rejected}
    end
  end

  defp request_line_too_large?(req, opts) do
    limit = Map.fetch!(opts, :ferricstore_max_request_line_bytes)
    method_bytes = req |> Map.fetch!(:method) |> byte_size()
    path_bytes = req |> Map.fetch!(:path) |> byte_size()
    query = Map.fetch!(req, :qs)
    query_bytes = if query == "", do: 0, else: byte_size(query) + 1

    version_bytes =
      req
      |> Map.fetch!(:version)
      |> Atom.to_string()
      |> byte_size()

    method_bytes + path_bytes + query_bytes + version_bytes + 2 > limit
  end

  defp request_headers_too_large?(req, opts) do
    max_name = Map.fetch!(opts, :ferricstore_max_header_name_bytes)
    max_value = Map.fetch!(opts, :ferricstore_max_header_value_bytes)

    req
    |> Map.fetch!(:headers)
    |> Enum.any?(fn {name, value} ->
      byte_size(name) > max_name or byte_size(value) > max_value
    end)
  end

  @impl :cowboy_stream
  def data(stream_id, is_fin, data, {:accepted, next}) do
    {commands, next} = :cowboy_stream.data(stream_id, is_fin, data, next)
    {commands, {:accepted, next}}
  end

  def data(_stream_id, _is_fin, _data, :rejected), do: {[], :rejected}

  @impl :cowboy_stream
  def info(stream_id, info, {:accepted, next}) do
    {commands, next} = :cowboy_stream.info(stream_id, info, next)
    {commands, {:accepted, next}}
  end

  def info(_stream_id, _info, :rejected), do: {[], :rejected}

  @impl :cowboy_stream
  def terminate(stream_id, reason, {:accepted, next}) do
    :cowboy_stream.terminate(stream_id, reason, next)
  after
    Admission.release()
  end

  def terminate(_stream_id, _reason, :rejected), do: :ok

  @impl :cowboy_stream
  def early_error(stream_id, reason, partial_req, response, opts) do
    :cowboy_stream.early_error(stream_id, reason, partial_req, response, opts)
  end
end
