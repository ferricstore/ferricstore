defmodule FerricstoreServer.Health.Endpoint.Bootstrap do
  @moduledoc false

  alias FerricstoreServer.Health.Endpoint.AccessPage
  alias FerricstoreServer.Health.Endpoint.Login
  alias FerricstoreServer.Health.Endpoint.Session

  @minimum_token_bytes 32

  @spec authorize(term(), map(), term()) :: :ok | {:error, binary()}
  def authorize(peer, headers, provided_token) when is_map(headers) do
    if direct_loopback_request?(peer, headers) do
      :ok
    else
      authorize_remote(provided_token)
    end
  end

  def authorize(_peer, _headers, _provided_token),
    do: {:error, "Remote dashboard setup is not configured."}

  @spec token_required?(term(), map()) :: boolean()
  def token_required?(peer, headers) when is_map(headers),
    do: not direct_loopback_request?(peer, headers)

  def token_required?(_peer, _headers), do: true

  @spec location(binary()) :: binary()
  def location(path) do
    "/dashboard/setup?" <> URI.encode_query(%{"next" => sanitize_next(path)})
  end

  @spec sanitize_next(term()) :: binary()
  def sanitize_next(path) do
    case Login.sanitize_next(path) do
      "/dashboard/setup" <> _rest -> "/dashboard"
      safe -> safe
    end
  end

  @spec success_location(term()) :: binary()
  def success_location(next) do
    destination =
      case sanitize_next(next) do
        "/dashboard" -> "/dashboard/security"
        safe -> safe
      end

    separator = if String.contains?(destination, "?"), do: "&", else: "?"

    destination <>
      separator <>
      URI.encode_query(%{
        "status" => "ok",
        "message" => "Recovery administrator created. You can now add additional accounts."
      })
  end

  @spec render_page(binary(), binary() | nil, boolean()) :: binary()
  def render_page(next, error, token_required?) do
    safe_next = sanitize_next(next)

    token_field =
      if token_required? do
        """
        <label for="bootstrap_token">Bootstrap token</label>
        <input id="bootstrap_token" name="bootstrap_token" type="password" maxlength="4096" autocomplete="one-time-code" required>
        <p class="field-help">Use the token from the configured mounted secret file.</p>
        """
      else
        ""
      end

    form_html =
      """
      <form method="post" action="/dashboard/setup">
        <input type="hidden" name="next" value="#{AccessPage.escape(safe_next)}">
        #{token_field}
        <label for="password">Password</label>
        <input id="password" name="password" type="password" minlength="12" maxlength="4096" autocomplete="new-password" required autofocus>
        <label for="password_confirmation">Confirm password</label>
        <input id="password_confirmation" name="password_confirmation" type="password" minlength="12" maxlength="4096" autocomplete="new-password" required>
        <button type="submit">Create administrator</button>
      </form>
      """

    AccessPage.render(%{
      title: "Set up FerricStore Dashboard",
      kicker: "One-time setup",
      heading: "Create the recovery administrator",
      copy:
        "Set a password for the built-in default account. Setup closes as soon as the replicated ACL catalog changes.",
      error: error,
      form_html: form_html,
      context_heading: "Bootstrap is narrow, durable, and race-safe.",
      context_items: [
        {"Account", "default"},
        {"Commit", "Replicated ACL"},
        {"Availability", "Initial catalog only"}
      ],
      footer_items: ["12-character minimum", "PBKDF2-SHA256 at rest"]
    })
  end

  defp authorize_remote(provided_token) when is_binary(provided_token) do
    case Application.get_env(:ferricstore, :dashboard_bootstrap_token) do
      configured when is_binary(configured) and byte_size(configured) >= @minimum_token_bytes ->
        if secure_equal?(provided_token, configured),
          do: :ok,
          else: {:error, "Bootstrap token is invalid."}

      _missing_or_invalid ->
        {:error, "Remote dashboard setup is not configured."}
    end
  end

  defp authorize_remote(_provided_token), do: {:error, "Bootstrap token is invalid."}

  defp secure_equal?(left, right) when is_binary(left) and is_binary(right) do
    :crypto.hash_equals(:crypto.hash(:sha256, left), :crypto.hash(:sha256, right))
  end

  defp direct_loopback_request?(peer, headers) do
    loopback?(peer) and not Session.trusted_proxy_peer?(peer) and
      not Map.has_key?(headers, "x-forwarded-proto")
  end

  defp loopback?({127, _, _, _}), do: true
  defp loopback?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback?({0, 0, 0, 0, 0, 65_535, 32_512, _}), do: true
  defp loopback?(_peer), do: false
end
