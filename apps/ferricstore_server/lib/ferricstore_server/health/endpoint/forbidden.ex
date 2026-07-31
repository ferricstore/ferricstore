defmodule FerricstoreServer.Health.Endpoint.Forbidden do
  @moduledoc false

  alias FerricstoreServer.Health.Endpoint.{AccessPage, Login, Response}

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
        render_page(details, reason, path)
      )
    end
  end

  @doc false
  @spec render_page(map(), binary(), binary()) :: binary()
  def render_page(details, reason, path)
      when is_map(details) and is_binary(reason) and is_binary(path) do
    next = path |> Login.sanitize_next() |> AccessPage.escape()

    AccessPage.render(%{
      title: "Forbidden",
      kicker: "Access denied",
      heading: "This account cannot access that resource.",
      copy: reason,
      form_html: """
      <form method="post" action="/dashboard/logout">
        <input type="hidden" name="next" value="#{next}">
        <button type="submit">Sign in as another user</button>
      </form>
      """,
      context_heading: "The live ACL policy blocked this operation.",
      context_items: context_items(details),
      footer_items: ["No protected data was returned", "ACL rules remain in effect"]
    })
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

  defp context_items(details) do
    items = [{"Required command", Map.fetch!(details, :required_acl_rule)}]

    case Map.get(details, :required_key) do
      nil ->
        items

      key ->
        items ++
          [
            {"Required key", "#{Map.fetch!(details, :required_key_access)} on #{key}"},
            {"Key ACL rule", Map.fetch!(details, :required_key_rule)}
          ]
    end
  end

  defp dashboard_api_path?("/dashboard/api"), do: true
  defp dashboard_api_path?("/dashboard/api/" <> _rest), do: true
  defp dashboard_api_path?(_path), do: false
end
