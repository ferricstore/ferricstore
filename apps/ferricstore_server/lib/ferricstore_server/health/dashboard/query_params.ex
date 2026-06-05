defmodule FerricstoreServer.Health.Dashboard.QueryParams do
  @moduledoc false

  @flow_dashboard_recent_limit 40

  def dashboard_param(opts, key) when is_map(opts), do: Map.get(opts, key, "")

  def dashboard_param(opts, key) when is_list(opts) do
    atom_key = String.to_atom(key)

    cond do
      Keyword.keyword?(opts) ->
        Keyword.get(opts, atom_key, "")

      match?({^key, _value}, List.keyfind(opts, key, 0)) ->
        {_key, value} = List.keyfind(opts, key, 0)
        value

      true ->
        ""
    end
  end

  def dashboard_param(_opts, _key), do: ""

  def truthy_dashboard_param?(value) when value in [true, "true", "1", "on", "yes"], do: true
  def truthy_dashboard_param?(_value), do: false

  def flow_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{partition_key: nil})
  end

  def flow_states_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: Map.get(data, :type_filter),
      state: Map.get(data, :state_filter),
      q: Map.get(data, :name_filter),
      range: Map.get(data, :range_filter),
      from_ms: Map.get(data, :from_ms),
      to_ms: Map.get(data, :to_ms),
      limit: Map.get(data, :limit, @flow_dashboard_recent_limit)
    })
  end

  def flow_failures_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: nil,
      partition_key: nil,
      q: nil,
      limit: @flow_dashboard_recent_limit,
      scan_exact: false
    })
  end

  def flow_signals_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: nil,
      signal: nil,
      q: nil,
      limit: @flow_dashboard_recent_limit,
      scan_history: false
    })
  end
end
