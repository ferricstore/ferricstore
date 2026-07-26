defmodule Ferricstore.Flow.Query.QueryRowDecodePlan do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, Limits, Plan, Request}

  @maximum_selected_fields Limits.max_return_fields() + Limits.max_predicates() * 3 +
                             Limits.max_order_fields() + 8

  @enforce_keys [:attributes, :state_meta]
  defstruct [:attributes, :state_meta]

  @type t :: %__MODULE__{
          attributes: :none | :all | MapSet.t(binary()),
          state_meta: :none | :all | %{optional(binary()) => MapSet.t(binary())}
        }

  @doc false
  @spec for_query(Request.t(), Plan.t()) :: t()
  def for_query(%Request{} = request, %Plan{} = plan) do
    fields =
      projection_fields(request) ++
        predicate_fields(request.predicate) ++
        predicate_fields(plan.residual_predicates) ++
        predicate_fields(plan.recheck_predicates) ++
        order_fields(request.order_by) ++ index_fields(plan.definition)

    Enum.reduce(fields, %__MODULE__{attributes: :none, state_meta: :none}, fn
      :attributes, decode_plan ->
        %{decode_plan | attributes: :all}

      {:attribute, name}, decode_plan ->
        %{decode_plan | attributes: select(decode_plan.attributes, name)}

      :state_meta, decode_plan ->
        %{decode_plan | state_meta: :all}

      {:state_meta, state, name}, decode_plan ->
        %{decode_plan | state_meta: select_state_meta(decode_plan.state_meta, state, name)}

      _field, decode_plan ->
        decode_plan
    end)
  end

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{attributes: attributes, state_meta: state_meta}) do
    valid_selection?(attributes, &Field.valid?({:attribute, &1})) and
      valid_state_meta_selection?(state_meta)
  end

  def valid?(_decode_plan), do: false

  defp projection_fields(%Request{return: :count}), do: []

  defp projection_fields(%Request{return: :record, projection: :all}),
    do: [:attributes, :state_meta]

  defp projection_fields(%Request{return: :record, projection: fields}) when is_list(fields),
    do: fields

  defp projection_fields(%Request{}), do: []

  defp predicate_fields(predicates) when is_list(predicates),
    do: Enum.flat_map(predicates, &predicate_fields/1)

  defp predicate_fields({:and, predicates}), do: predicate_fields(predicates)
  defp predicate_fields({:eq, field, _value}), do: [field]
  defp predicate_fields({:in, field, _values}), do: [field]
  defp predicate_fields({:range, field, _lower, _upper}), do: [field]
  defp predicate_fields({:time_window, field, _lower, _upper}), do: [field]
  defp predicate_fields({:is, field, _kind}), do: [field]
  defp predicate_fields(_predicate), do: []

  defp order_fields(order_by) when is_list(order_by) do
    Enum.flat_map(order_by, fn
      {field, direction} when direction in [:asc, :desc] -> [field]
      _invalid -> []
    end)
  end

  defp order_fields(_order_by), do: []

  defp index_fields(%{fields: fields}) when is_list(fields) do
    Enum.flat_map(fields, fn
      {field, direction, encoding}
      when direction in [:asc, :desc] and encoding in [:hashed, :ordered] ->
        [field]

      _invalid ->
        []
    end)
  end

  defp index_fields(_definition), do: []

  defp select(:all, _field), do: :all
  defp select(:none, field), do: MapSet.new([field])
  defp select(%MapSet{} = fields, field), do: MapSet.put(fields, field)

  defp select_state_meta(:all, _state, _name), do: :all
  defp select_state_meta(:none, state, name), do: %{state => MapSet.new([name])}

  defp select_state_meta(fields, state, name) when is_map(fields) do
    Map.update(fields, state, MapSet.new([name]), &MapSet.put(&1, name))
  end

  defp valid_selection?(selection, _valid_field?) when selection in [:none, :all], do: true

  defp valid_selection?(%MapSet{} = selection, valid_field?) do
    MapSet.size(selection) > 0 and MapSet.size(selection) <= @maximum_selected_fields and
      Enum.all?(selection, valid_field?)
  end

  defp valid_selection?(_selection, _valid_field?), do: false

  defp valid_state_meta_selection?(selection) when selection in [:none, :all], do: true

  defp valid_state_meta_selection?(selection) when is_map(selection) do
    map_size(selection) > 0 and
      selection
      |> Enum.reduce_while(0, fn
        {state, %MapSet{} = names}, count ->
          name_count = MapSet.size(names)
          next_count = count + name_count

          if name_count > 0 and next_count <= @maximum_selected_fields and
               Enum.all?(names, &Field.valid?({:state_meta, state, &1})) do
            {:cont, next_count}
          else
            {:halt, :error}
          end

        _invalid, _count ->
          {:halt, :error}
      end)
      |> is_integer()
  end

  defp valid_state_meta_selection?(_selection), do: false
end
