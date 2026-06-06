defmodule FerricstoreServer.Health.Endpoint.Forbidden do
  @moduledoc false

  alias FerricstoreServer.Health.Endpoint.Response

  def send_response(socket, transport, path, requirement, reason) do
    details = requirement_details(requirement)

    if dashboard_api_path?(path) or path == "/metrics" do
      Response.send_response(
        socket,
        transport,
        403,
        "Forbidden",
        "application/json",
        Jason.encode!(Map.merge(%{error: "forbidden", reason: reason}, details))
      )
    else
      Response.send_html_response(
        socket,
        transport,
        403,
        "Forbidden",
        """
        <!DOCTYPE html>
        <html lang="en"><head><meta charset="utf-8"><title>Forbidden</title></head>
        <body><h1>Forbidden</h1>#{details_html(details, reason)}<p><a href="/dashboard/login">Sign in as another user</a></p></body></html>
        """
      )
    end
  end

  def requirement_details({command, opts}) do
    command = String.upcase(to_string(command))

    %{
      required_command: command,
      required_acl_rule: acl_command_rule(command)
    }
    |> maybe_put_required_key(opts)
  end

  def requirement_details(command) do
    requirement_details({command, []})
  end

  defp maybe_put_required_key(details, opts) do
    case Keyword.get(opts, :key) do
      {key, access} ->
        details
        |> Map.put(:required_key, key)
        |> Map.put(:required_key_access, to_string(access))
        |> Map.put(:required_key_rule, acl_key_rule(access, key))

      _ ->
        details
    end
  end

  defp acl_command_rule("*"), do: "+@all"
  defp acl_command_rule(command), do: "+" <> command

  defp acl_key_rule(:read, key), do: "%R~" <> key
  defp acl_key_rule(:write, key), do: "%W~" <> key
  defp acl_key_rule(_access, key), do: "~" <> key

  defp details_html(details, reason) do
    key_html =
      case Map.get(details, :required_key) do
        nil ->
          ""

        key ->
          """
          <dt>Required key access</dt><dd><code>#{html_escape(Map.fetch!(details, :required_key_access))}</code> on <code>#{html_escape(key)}</code></dd>
          <dt>Key ACL rule</dt><dd><code>#{html_escape(Map.fetch!(details, :required_key_rule))}</code></dd>
          """
      end

    """
    <p>#{html_escape(reason)}</p>
    <dl>
      <dt>Required ACL command</dt><dd><code>#{html_escape(Map.fetch!(details, :required_acl_rule))}</code></dd>
      #{key_html}
    </dl>
    """
  end

  defp dashboard_api_path?("/dashboard/api"), do: true
  defp dashboard_api_path?("/dashboard/api/" <> _rest), do: true
  defp dashboard_api_path?(_path), do: false

  defp html_escape(value) when is_binary(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  defp html_escape(value), do: value |> to_string() |> html_escape()
end
