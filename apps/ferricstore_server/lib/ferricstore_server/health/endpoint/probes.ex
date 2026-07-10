defmodule FerricstoreServer.Health.Endpoint.Probes do
  @moduledoc false

  @type response :: {pos_integer(), String.t(), binary()}

  @spec live_response() :: response()
  def live_response do
    {200, "OK", ~s({"status":"alive"})}
  end

  @spec ready_response() :: response()
  def ready_response do
    health = Ferricstore.Health.check()

    body =
      Jason.encode!(%{
        status: Atom.to_string(health.status),
        shard_count: health.shard_count,
        shards:
          Enum.map(health.shards, fn shard ->
            %{index: shard.index, status: shard.status, keys: shard.keys}
          end),
        uptime_seconds: health.uptime_seconds
      })

    case health.status do
      :ok -> {200, "OK", body}
      :starting -> {503, "Service Unavailable", body}
    end
  end
end
