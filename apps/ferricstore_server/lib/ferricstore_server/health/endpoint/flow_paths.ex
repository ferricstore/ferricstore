defmodule FerricstoreServer.Health.Endpoint.FlowPaths do
  @moduledoc false

  @spec decode_form_body(binary()) :: map()
  def decode_form_body(body) when is_binary(body) do
    URI.decode_query(body)
  rescue
    _ -> %{}
  end

  @spec decode_flow_detail_request(binary()) :: {binary(), map()}
  def decode_flow_detail_request(encoded_id_with_query) do
    {encoded_id, query} =
      case String.split(encoded_id_with_query, "?", parts: 2) do
        [encoded_id, query] -> {encoded_id, query}
        [encoded_id] -> {encoded_id, ""}
      end

    opts = FerricstoreServer.Health.Dashboard.flow_detail_opts_from_query(query)

    {URI.decode(encoded_id), opts}
  end

  @spec decode_flow_rewind_action(binary()) :: {:ok, binary()} | :not_found
  def decode_flow_rewind_action(encoded_action) do
    suffix = "/rewind"

    if String.ends_with?(encoded_action, suffix) do
      encoded_id =
        binary_part(encoded_action, 0, byte_size(encoded_action) - byte_size(suffix))

      {:ok, URI.decode(encoded_id)}
    else
      :not_found
    end
  end

  @spec decode_flow_signal_action(binary()) :: {:ok, binary()} | :not_found
  def decode_flow_signal_action(encoded_action) do
    suffix = "/signal"

    if String.ends_with?(encoded_action, suffix) do
      encoded_id =
        binary_part(encoded_action, 0, byte_size(encoded_action) - byte_size(suffix))

      {:ok, URI.decode(encoded_id)}
    else
      :not_found
    end
  end

  @spec flow_detail_location(binary(), binary() | nil) :: binary()
  def flow_detail_location(id, ""),
    do: flow_detail_path(id)

  def flow_detail_location(id, nil),
    do: flow_detail_path(id)

  def flow_detail_location(id, partition_key) do
    flow_detail_path(id) <> "?" <> URI.encode_query(%{"partition_key" => partition_key})
  end

  @spec flow_detail_location(binary(), binary() | nil, map()) :: binary()
  def flow_detail_location(id, partition_key, extra_params) when is_map(extra_params) do
    params =
      extra_params
      |> Enum.reject(fn {_key, value} -> is_nil(value) or value == "" end)
      |> Map.new()

    params =
      case partition_key do
        key when is_binary(key) and key != "" -> Map.put(params, "partition_key", key)
        _ -> params
      end

    path = flow_detail_path(id)

    if map_size(params) == 0, do: path, else: path <> "?" <> URI.encode_query(params)
  end

  defp flow_detail_path(id) do
    "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1)
  end
end
