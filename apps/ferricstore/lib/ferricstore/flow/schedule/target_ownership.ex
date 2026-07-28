defmodule Ferricstore.Flow.Schedule.TargetOwnership do
  @moduledoc false

  @attribute "ferricstore.schedule.owner.v1"
  @secret_bytes 32

  @spec new_secret() :: binary()
  def new_secret, do: :crypto.strong_rand_bytes(@secret_bytes)

  @spec attributes(map(), binary()) :: %{binary() => binary()}
  def attributes(definition, target_id) when is_map(definition) and is_binary(target_id) do
    %{@attribute => digest(definition, target_id)}
  end

  @spec owned?(map(), map(), binary()) :: boolean()
  def owned?(record, definition, target_id)
      when is_map(record) and is_map(definition) and is_binary(target_id) do
    expected = digest(definition, target_id)

    case get_in(record, [:attributes, @attribute]) do
      actual when is_binary(actual) and byte_size(actual) == byte_size(expected) ->
        :crypto.hash_equals(actual, expected)

      _missing_or_invalid ->
        false
    end
  end

  def owned?(_record, _definition, _target_id), do: false

  defp digest(definition, target_id) do
    secret = Map.fetch!(definition, :ownership_secret)

    data =
      {Map.fetch!(definition, :id), target_id, Map.fetch!(definition, :target)}
      |> :erlang.term_to_binary([:deterministic])

    digest = :crypto.mac(:hmac, :sha256, secret, data)
    Base.url_encode64(digest, padding: false)
  end
end
