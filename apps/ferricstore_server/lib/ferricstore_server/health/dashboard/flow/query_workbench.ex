defmodule FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench do
  @moduledoc false

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Flow.Query.{Error, Limits, Request}
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnectionAuth
  alias FerricstoreServer.Health.Dashboard.Flow.{QueryPagination, QueryResult}
  alias FerricstoreServer.Native.FQLParser

  import FerricstoreServer.Health.Dashboard.Flow.Calls

  @default_query """
  FROM runs
  WHERE partition_key = @partition AND type = @type
  ORDER BY updated_at_ms DESC
  LIMIT 40
  RETURN RECORDS (run_id, type, state, updated_at_ms)
  """
  @default_params_json Jason.encode!(%{"partition" => "default", "type" => "workflow"},
                         pretty: true
                       )
  @actions %{"run" => :execute, "explain" => :explain, "analyze" => :analyze}
  @max_parameters Limits.max_parameters()

  @type form :: %{
          mode: :guided | :advanced,
          action: :run | :explain | :analyze,
          fql: binary(),
          params_json: binary(),
          cursor: binary() | nil,
          guided_query: binary() | nil,
          errors: %{optional(:fql | :params_json) => binary()}
        }

  @spec default_form() :: form()
  def default_form do
    %{
      mode: :guided,
      action: :run,
      fql: String.trim(@default_query),
      params_json: @default_params_json,
      cursor: nil,
      guided_query: nil,
      errors: %{}
    }
  end

  @spec default_form(map()) :: form()
  def default_form(filters) when is_map(filters), do: default_form()

  @doc false
  @spec draft_scope(keyword()) :: binary() | nil
  def draft_scope(opts) when is_list(opts) do
    identity =
      case Keyword.get(opts, :acl_username) do
        username when is_binary(username) and username != "" ->
          case Acl.get_user(username) do
            %{enabled: true, auth_epoch: epoch} -> {:acl, username, epoch}
            _unverified -> nil
          end

        _missing ->
          if Application.get_env(:ferricstore, :protected_mode, true) == false,
            do: :open_access
      end

    if identity do
      :crypto.hash(:sha256, :erlang.term_to_binary({node(), identity}))
      |> Base.url_encode64(padding: false)
    end
  end

  @spec guided_form(binary(), map(), binary()) :: form()
  def guided_form(fql, params, guided_query)
      when is_binary(fql) and is_map(params) and is_binary(guided_query) do
    %{
      default_form()
      | mode: :guided,
        fql: fql,
        params_json: Jason.encode!(params),
        guided_query: guided_query
    }
  end

  @spec prepare(map()) ::
          {:ok, PreparedCommand.t(), form()} | {:error, form(), binary()}
  def prepare(params) when is_map(params) do
    form = form_from_params(params)

    with {:ok, form} <- QueryPagination.prepare(params, form),
         {:ok, mode} <- requested_mode(params),
         {:ok, typed_params} <- field_result(decode_parameters(form.params_json), :params_json),
         {:ok, prepared} <- field_result(prepare_command(form.fql, typed_params), :fql),
         prepared = reset_page_cursor(prepared, form),
         {:ok, prepared} <- set_cursor(prepared, form.cursor, mode),
         {:ok, prepared} <- field_result(set_mode(prepared, mode), :fql) do
      {:ok, prepared, form_for_request(form, prepared, params)}
    else
      {:error, {field, reason}} when field in [:fql, :params_json] ->
        message = format_error(reason)
        form = Map.put(form, :errors, %{field => message})

        form =
          case reason do
            %Error{position: %{byte: byte} = position} when is_integer(byte) and byte > 0 ->
              Map.put(form, :error_positions, %{field => position})

            _unpositioned ->
              form
          end

        {:error, form, message}

      {:error, reason} ->
        {:error, form, format_error(reason)}
    end
  end

  def prepare(_params) do
    form = %{default_form() | mode: :advanced}
    {:error, form, "ERR query form parameters must be an object"}
  end

  @spec requirements(PreparedCommand.t()) :: [{binary(), keyword()}]
  def requirements(%PreparedCommand{} = prepared) do
    commands =
      ConnectionAuth.acl_command_names(prepared.command, prepared.args, prepared.ast)

    keys = if prepared.acl_keys == [], do: ["*"], else: prepared.acl_keys

    for command <- commands, key <- keys do
      {command, key: {key, :read}}
    end
  end

  @doc false
  @spec action_requirements(map()) :: [{binary(), keyword()}]
  def action_requirements(%{"action" => "explain"}), do: [{"FLOW.QUERY.EXPLAIN", []}]

  def action_requirements(%{"action" => "analyze"}),
    do: [{"FLOW.QUERY", []}, {"FLOW.QUERY.EXPLAIN", []}]

  def action_requirements(_params), do: [{"FLOW.QUERY", []}]

  @spec execute(PreparedCommand.t()) :: map()
  def execute(%PreparedCommand{ast: {:flow_query, %Request{} = request}}) do
    command = display_command(request.mode)

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_query_prepared(request) end,
           flow_dashboard_list_fetch_timeout_ms(),
           :query_workbench
         ) do
      {:ok, {:ok, response}} ->
        QueryResult.success(command, response, request: request)

      {:ok, {:error, reason}} ->
        error_result(command, reason)

      {:error, :timeout} ->
        %{status: :timeout, command: command, rows: [], message: "query timed out"}

      {:error, reason} ->
        error_result(command, reason)

      _unexpected ->
        %{status: :error, command: command, rows: [], message: "unexpected query result"}
    end
  end

  def execute(%PreparedCommand{}) do
    %{status: :error, command: "FLOW.QUERY", rows: [], message: "invalid prepared query"}
  end

  @spec attach_continuation(map(), form()) :: map()
  def attach_continuation(result, form) when is_map(form) do
    result
    |> QueryResult.present(form)
    |> QueryPagination.attach(form)
  end

  def attach_continuation(result, _form), do: result

  defp reset_page_cursor(%PreparedCommand{ast: {:flow_query, request}} = prepared, %{
         page_action: :first
       }),
       do: %{prepared | ast: {:flow_query, %{request | cursor: nil}}}

  defp reset_page_cursor(prepared, _form), do: prepared

  defp form_for_request(
         %{cursor: nil} = form,
         %PreparedCommand{ast: {:flow_query, %Request{cursor: {:literal, :keyword, cursor}}}},
         params
       ) do
    form
    |> Map.put(:cursor, cursor)
    |> Map.put(:page_number, if(Map.has_key?(params, "page_number"), do: form.page_number))
  end

  defp form_for_request(form, _prepared, _params), do: form

  defp form_from_params(params) do
    action = normalize_action(Map.get(params, "action"))

    %{
      mode: normalize_surface(Map.get(params, "surface")),
      action: action,
      fql: normalize_text(Map.get(params, "fql")),
      params_json: normalize_params_json(Map.get(params, "params_json")),
      cursor: normalize_cursor(Map.get(params, "cursor")),
      guided_query: normalize_guided_query(Map.get(params, "guided_query")),
      errors: %{}
    }
  end

  defp normalize_surface("guided"), do: :guided
  defp normalize_surface(_surface), do: :advanced

  defp normalize_action("explain"), do: :explain
  defp normalize_action("analyze"), do: :analyze
  defp normalize_action(_action), do: :run

  defp normalize_text(value) when is_binary(value), do: String.trim(value)
  defp normalize_text(_value), do: ""

  defp normalize_params_json(value) when is_binary(value), do: String.trim(value)
  defp normalize_params_json(_value), do: "{}"

  defp normalize_guided_query(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      query -> query
    end
  end

  defp normalize_guided_query(_value), do: nil

  defp normalize_cursor(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      cursor -> cursor
    end
  end

  defp normalize_cursor(_value), do: nil

  defp requested_mode(params) do
    case Map.get(params, "action", "run") do
      action when is_map_key(@actions, action) -> {:ok, Map.fetch!(@actions, action)}
      _invalid -> {:error, "ERR query action must be run, explain, or analyze"}
    end
  end

  defp field_result({:error, %Error{reason: reason} = error}, _field)
       when reason in [
              :invalid_parameters,
              :invalid_parameter_type,
              :missing_parameter,
              :unexpected_parameter
            ],
       do: {:error, {:params_json, error}}

  defp field_result({:error, reason}, field), do: {:error, {field, reason}}
  defp field_result(result, _field), do: result

  defp decode_parameters(""), do: {:ok, %{}}

  defp decode_parameters(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, params} when is_map(params) ->
        validate_parameters(params)

      {:ok, _other} ->
        {:error, "ERR FQL1 parameters must be a JSON object"}

      {:error, %Jason.DecodeError{} = error} ->
        {:error, "ERR FQL1 Parameters JSON is invalid: #{Jason.DecodeError.message(error)}"}
    end
  end

  defp validate_parameters(params) when map_size(params) > @max_parameters,
    do: {:error, "ERR FQL1 accepts at most #{@max_parameters} named parameters"}

  defp validate_parameters(params) do
    if Enum.all?(params, fn {name, value} ->
         is_binary(name) and name != "" and scalar_parameter?(value)
       end) do
      {:ok, params}
    else
      {:error, "ERR FQL1 parameter names must be non-empty and values must be scalar"}
    end
  end

  defp scalar_parameter?(value),
    do: is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value)

  defp prepare_command(query, params) do
    args = ["FQL1", query | flatten_parameters(params)]

    PreparedCommand.prepare("FLOW.QUERY", args,
      flow_query_parser: FQLParser,
      flow_query_error_format: :structured
    )
  end

  defp flatten_parameters(params) do
    params
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.flat_map(fn {name, value} -> [name, value] end)
  end

  defp set_mode(
         %PreparedCommand{ast: {:flow_query, %Request{} = request}} = prepared,
         mode
       )
       when mode in [:execute, :explain, :analyze] do
    request = %{request | mode: mode}

    case Request.validate_bound(request) do
      :ok -> {:ok, %{prepared | ast: {:flow_query, request}}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp set_mode(%PreparedCommand{}, _mode), do: {:error, :unsupported_query_shape}

  defp set_cursor(%PreparedCommand{} = prepared, nil, _mode), do: {:ok, prepared}

  defp set_cursor(%PreparedCommand{}, cursor, mode)
       when is_binary(cursor) and mode in [:explain, :analyze],
       do: {:error, :query_cursor_invalid}

  defp set_cursor(
         %PreparedCommand{ast: {:flow_query, %Request{return: :record} = request}} = prepared,
         cursor,
         :execute
       )
       when is_binary(cursor) do
    request = %{request | cursor: {:literal, :keyword, cursor}}

    case Request.validate_bound(request) do
      :ok -> {:ok, %{prepared | ast: {:flow_query, request}}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp set_cursor(%PreparedCommand{}, _cursor, _mode), do: {:error, :query_cursor_invalid}

  defp display_command(:execute), do: "FLOW.QUERY"
  defp display_command(:explain), do: "FLOW.QUERY EXPLAIN"
  defp display_command(:analyze), do: "FLOW.QUERY EXPLAIN ANALYZE"

  defp error_result(command, reason) do
    %{status: :error, command: command, rows: [], message: format_error(reason)}
  end

  defp format_error(%Error{} = error), do: Error.format(error)
  defp format_error(reason) when is_atom(reason), do: Error.format(reason)
  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error({:exit, _reason}), do: "query execution failed"
  defp format_error(_reason), do: "query execution failed"
end
