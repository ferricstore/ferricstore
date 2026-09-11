defmodule FerricstoreServer.Health.Dashboard.Flow.RetentionReview do
  @moduledoc false
  @lifetime_ms 5 * 60_000

  def prepare(limit, now_ms \\ System.system_time(:millisecond)),
    do: %{limit: limit, reviewed_limit: to_string(limit), reviewed_at_ms: to_string(now_ms)}

  def validate(params, limit, now_ms \\ System.system_time(:millisecond)) do
    with true <- Map.get(params, "reviewed_limit") == to_string(limit),
         raw when is_binary(raw) <- Map.get(params, "reviewed_at_ms"),
         {reviewed_at, ""} <- Integer.parse(raw),
         age = now_ms - reviewed_at,
         true <- age >= 0 and age <= @lifetime_ms do
      :ok
    else
      _ ->
        {:error,
         "Global cleanup review is missing, changed, or expired. Review global cleanup again."}
    end
  end
end
