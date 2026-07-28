defmodule Ferricstore.Flow.Schedule.Metadata do
  @moduledoc false

  @kinds [:one_shot, :delay, :interval, :cron]

  @type t :: %{
          required(:kind) => :one_shot | :delay | :interval | :cron,
          required(:target_type) => binary(),
          optional(:timezone) => binary()
        }

  @spec from_definition(map()) :: t()
  def from_definition(definition) do
    case from_definition_result(definition) do
      {:ok, metadata} -> metadata
      :error -> raise ArgumentError, "invalid flow schedule definition metadata"
    end
  end

  @spec from_definition_result(term()) :: {:ok, t()} | :error
  def from_definition_result(%{kind: kind, target: %{type: target_type}} = definition) do
    %{kind: kind, target_type: target_type}
    |> maybe_put_timezone(Map.get(definition, :timezone))
    |> normalize()
    |> case do
      {:ok, %{} = metadata} -> {:ok, metadata}
      _invalid -> :error
    end
  end

  def from_definition_result(_definition), do: :error

  @spec normalize(term()) :: {:ok, t() | nil} | :error
  def normalize(nil), do: {:ok, nil}

  def normalize(%{kind: kind, target_type: target_type} = metadata)
      when map_size(metadata) == 2,
      do: normalize_fields(kind, target_type, nil, 2)

  def normalize(%{kind: kind, target_type: target_type, timezone: timezone} = metadata)
      when map_size(metadata) == 3,
      do: normalize_fields(kind, target_type, timezone, 3)

  def normalize(_metadata), do: :error

  @spec encode_sidecar(term()) :: map() | nil
  def encode_sidecar(metadata) do
    case normalize(metadata) do
      {:ok, nil} ->
        nil

      {:ok, normalized} ->
        %{
          "kind" => Atom.to_string(normalized.kind),
          "target_type_b64" => Base.url_encode64(normalized.target_type, padding: false)
        }
        |> maybe_put_encoded_timezone(Map.get(normalized, :timezone))

      :error ->
        raise ArgumentError, "invalid flow schedule metadata"
    end
  end

  @spec decode_sidecar(term()) :: {:ok, t() | nil} | :error
  def decode_sidecar(nil), do: {:ok, nil}

  def decode_sidecar(%{"kind" => kind, "target_type_b64" => encoded_target_type} = metadata)
      when map_size(metadata) == 2,
      do: decode_fields(kind, encoded_target_type, nil, 2)

  def decode_sidecar(
        %{
          "kind" => kind,
          "target_type_b64" => encoded_target_type,
          "timezone" => timezone
        } = metadata
      )
      when map_size(metadata) == 3,
      do: decode_fields(kind, encoded_target_type, timezone, 3)

  def decode_sidecar(_metadata), do: :error

  @spec fetch_record(map()) :: {:ok, t()} | :error
  def fetch_record(%{schedule_metadata: metadata}) do
    case normalize(metadata) do
      {:ok, %{} = normalized} -> {:ok, normalized}
      _invalid -> :error
    end
  end

  def fetch_record(_record), do: :error

  defp normalize_fields(kind, target_type, timezone, field_count) do
    with {:ok, kind} <- normalize_kind(kind),
         true <- is_binary(target_type) and target_type != "",
         :ok <- validate_timezone(kind, timezone, field_count) do
      {:ok,
       %{kind: kind, target_type: target_type}
       |> maybe_put_timezone(timezone)}
    else
      _invalid -> :error
    end
  end

  defp decode_fields(kind, encoded_target_type, timezone, field_count)
       when is_binary(encoded_target_type) do
    case Base.url_decode64(encoded_target_type, padding: false) do
      {:ok, target_type} -> normalize_fields(kind, target_type, timezone, field_count)
      :error -> :error
    end
  end

  defp decode_fields(_kind, _encoded_target_type, _timezone, _field_count), do: :error

  defp normalize_kind(kind) when kind in @kinds, do: {:ok, kind}
  defp normalize_kind("one_shot"), do: {:ok, :one_shot}
  defp normalize_kind("delay"), do: {:ok, :delay}
  defp normalize_kind("interval"), do: {:ok, :interval}
  defp normalize_kind("cron"), do: {:ok, :cron}
  defp normalize_kind(_kind), do: :error

  defp validate_timezone(:cron, timezone, 3)
       when is_binary(timezone) and timezone != "",
       do: :ok

  defp validate_timezone(kind, nil, 2) when kind in [:one_shot, :delay, :interval], do: :ok
  defp validate_timezone(_kind, _timezone, _field_count), do: :error

  defp maybe_put_timezone(metadata, timezone) when is_binary(timezone) and timezone != "",
    do: Map.put(metadata, :timezone, timezone)

  defp maybe_put_timezone(metadata, _timezone), do: metadata

  defp maybe_put_encoded_timezone(metadata, timezone)
       when is_binary(timezone) and timezone != "",
       do: Map.put(metadata, "timezone", timezone)

  defp maybe_put_encoded_timezone(metadata, _timezone), do: metadata
end
