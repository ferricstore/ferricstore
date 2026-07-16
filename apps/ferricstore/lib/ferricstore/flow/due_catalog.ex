defmodule Ferricstore.Flow.DueCatalog do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  @version 1
  @max_exact_score 9_007_199_254_740_991

  @type t :: %{
          version: pos_integer(),
          entries: map(),
          state_trees: map(),
          heads: map()
        }

  defmodule Selection do
    @moduledoc false

    @enforce_keys [
      :catalog,
      :heap,
      :sequence,
      :state_selection,
      :seen,
      :inspected_entries,
      :inspected_heads
    ]
    defstruct @enforce_keys
  end

  @spec new() :: t()
  def new do
    %{version: @version, entries: %{}, state_trees: %{}, heads: %{}}
  end

  # This is intentionally O(1). Recovery rebuilds from the native index instead of
  # trusting persisted internals; apply-time mutations only need the trusted shape.
  @spec valid?(term()) :: boolean()
  def valid?(%{
        version: @version,
        entries: entries,
        state_trees: state_trees,
        heads: heads
      })
      when is_map(entries) and is_map(state_trees) and is_map(heads),
      do: true

  def valid?(_catalog), do: false

  @spec deep_valid?(term()) :: boolean()
  def deep_valid?(catalog) do
    with true <- valid?(catalog),
         {:ok, expected} <- rebuild_expected_catalog(catalog.entries),
         true <- catalog.entries == expected.entries,
         true <- gb_set_maps_equal?(catalog.state_trees, expected.state_trees),
         true <- gb_set_maps_equal?(catalog.heads, expected.heads) do
      true
    else
      _invalid -> false
    end
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  @spec put(t(), binary(), number()) :: t()
  def put(catalog, due_key, score) when is_binary(due_key) do
    case put_checked(catalog, due_key, score) do
      {:ok, next_catalog} -> next_catalog
      {:error, :invalid_due_catalog} -> catalog
    end
  end

  def put(catalog, _due_key, _score), do: catalog

  @spec put_checked(t(), binary(), number()) ::
          {:ok, t()} | {:error, :invalid_due_catalog}
  def put_checked(catalog, due_key, score) when is_binary(due_key) do
    with true <- valid?(catalog),
         true <- valid_score?(score),
         {:ok, metadata} <- Keys.decode_due_key(due_key),
         true <- existing_entry_valid?(catalog, due_key) do
      groups = due_catalog_groups(metadata)

      entry =
        metadata
        |> Map.put(:key, due_key)
        |> Map.put(:score, score)
        |> Map.put(:groups, groups)

      {:ok,
       catalog
       |> delete_entry(due_key)
       |> put_entry(entry)}
    else
      _invalid -> {:error, :invalid_due_catalog}
    end
  rescue
    _error -> {:error, :invalid_due_catalog}
  catch
    _kind, _reason -> {:error, :invalid_due_catalog}
  end

  def put_checked(_catalog, _due_key, _score), do: {:error, :invalid_due_catalog}

  @spec delete(t(), binary()) :: t()
  def delete(catalog, due_key) when is_binary(due_key) do
    case delete_checked(catalog, due_key) do
      {:ok, next_catalog} -> next_catalog
      {:error, :invalid_due_catalog} -> catalog
    end
  end

  def delete(catalog, _due_key), do: catalog

  @spec delete_checked(t(), binary()) :: {:ok, t()} | {:error, :invalid_due_catalog}
  def delete_checked(catalog, due_key) when is_binary(due_key) do
    with true <- valid?(catalog),
         true <- existing_entry_valid?(catalog, due_key) do
      {:ok, delete_entry(catalog, due_key)}
    else
      _invalid -> {:error, :invalid_due_catalog}
    end
  rescue
    _error -> {:error, :invalid_due_catalog}
  catch
    _kind, _reason -> {:error, :invalid_due_catalog}
  end

  def delete_checked(_catalog, _due_key), do: {:error, :invalid_due_catalog}

  @spec select(t(), binary(), integer(), term(), term(), pos_integer()) ::
          {:ok,
           %{
             keys: [binary()],
             inspected_entries: non_neg_integer(),
             inspected_heads: non_neg_integer()
           }}
          | {:error, :invalid_due_catalog_query}
  def select(catalog, type, priority, partition_filter, state_filter, limit)
      when is_binary(type) and type != "" and is_integer(priority) and is_integer(limit) and
             limit > 0 do
    with {:ok, selection} <-
           start_selection(catalog, type, priority, partition_filter, state_filter),
         {:ok, page} <- take_page(selection, limit) do
      {:ok,
       Map.take(page, [
         :keys,
         :inspected_entries,
         :inspected_heads
       ])}
    else
      _invalid -> {:error, :invalid_due_catalog_query}
    end
  end

  def select(_catalog, _type, _priority, _partition_filter, _state_filter, _limit),
    do: {:error, :invalid_due_catalog_query}

  @spec start_selection(t(), binary(), integer(), term(), term()) ::
          {:ok, %Selection{}} | {:error, :invalid_due_catalog_query}
  def start_selection(catalog, type, priority, partition_filter, state_filter)
      when is_binary(type) and type != "" and is_integer(priority) do
    with true <- valid?(catalog),
         {:ok, groups} <- selection_groups(partition_filter),
         {:ok, state_selection} <- state_selection(state_filter) do
      {heap, sequence, inspected_heads} =
        Enum.reduce(groups, {:gb_trees.empty(), 0, 0}, fn group,
                                                          {heap, sequence, inspected_heads} ->
          initialize_scope_selection(
            catalog,
            {type, priority, group},
            state_selection,
            heap,
            sequence,
            inspected_heads
          )
        end)

      {:ok,
       %Selection{
         catalog: catalog,
         heap: heap,
         sequence: sequence,
         state_selection: state_selection,
         seen: MapSet.new(),
         inspected_entries: 0,
         inspected_heads: inspected_heads
       }}
    else
      _invalid -> {:error, :invalid_due_catalog_query}
    end
  rescue
    _error -> {:error, :invalid_due_catalog_query}
  catch
    _kind, _reason -> {:error, :invalid_due_catalog_query}
  end

  def start_selection(_catalog, _type, _priority, _partition_filter, _state_filter),
    do: {:error, :invalid_due_catalog_query}

  @spec take_page(%Selection{}, pos_integer()) ::
          {:ok,
           %{
             keys: [binary()],
             continuation: %Selection{},
             done?: boolean(),
             inspected_entries: non_neg_integer(),
             inspected_heads: non_neg_integer()
           }}
          | {:error, :invalid_due_catalog_query}
  def take_page(%Selection{} = selection, limit) when is_integer(limit) and limit > 0 do
    take_page_until(selection, limit, :infinity)
  end

  def take_page(_selection, _limit), do: {:error, :invalid_due_catalog_query}

  @spec take_due_page(%Selection{}, number(), pos_integer()) ::
          {:ok,
           %{
             keys: [binary()],
             continuation: %Selection{},
             done?: boolean(),
             inspected_entries: non_neg_integer(),
             inspected_heads: non_neg_integer()
           }}
          | {:error, :invalid_due_catalog_query}
  def take_due_page(%Selection{} = selection, max_score, limit)
      when is_integer(limit) and limit > 0 do
    if valid_score?(max_score),
      do: take_page_until(selection, limit, max_score),
      else: {:error, :invalid_due_catalog_query}
  end

  def take_due_page(_selection, _max_score, _limit),
    do: {:error, :invalid_due_catalog_query}

  defp delete_entry(catalog, due_key) do
    case Map.get(catalog.entries, due_key) do
      %{groups: groups} = entry ->
        catalog =
          Enum.reduce(groups, catalog, fn group, acc ->
            delete_group_entry(acc, group, entry)
          end)

        %{catalog | entries: Map.delete(catalog.entries, due_key)}

      nil ->
        catalog
    end
  end

  defp take_page_until(%Selection{} = selection, limit, max_score) do
    {keys, inspected_entries, inspected_heads, heap, sequence, seen, done?} =
      take_selected_keys(
        selection.catalog,
        selection.heap,
        selection.sequence,
        selection.state_selection,
        limit,
        max_score,
        selection.seen,
        [],
        selection.inspected_entries,
        selection.inspected_heads
      )

    continuation = %Selection{
      selection
      | heap: heap,
        sequence: sequence,
        seen: seen,
        inspected_entries: inspected_entries,
        inspected_heads: inspected_heads
    }

    {:ok,
     %{
       keys: Enum.reverse(keys),
       continuation: continuation,
       done?: done?,
       inspected_entries: inspected_entries,
       inspected_heads: inspected_heads
     }}
  rescue
    _error -> {:error, :invalid_due_catalog_query}
  catch
    _kind, _reason -> {:error, :invalid_due_catalog_query}
  end

  defp put_entry(catalog, entry) do
    catalog =
      Enum.reduce(entry.groups, catalog, fn group, acc ->
        put_group_entry(acc, group, entry)
      end)

    %{catalog | entries: Map.put(catalog.entries, entry.key, entry)}
  end

  defp put_group_entry(catalog, group, entry) do
    tree_key = {entry.type, entry.priority, group, entry.state}
    tree = Map.get(catalog.state_trees, tree_key, :gb_sets.empty())
    old_head = smallest(tree)
    next_tree = :gb_sets.add({entry.score, entry.key}, tree)

    catalog
    |> put_state_tree(tree_key, next_tree)
    |> replace_head(
      {entry.type, entry.priority, group},
      entry.state,
      old_head,
      smallest(next_tree)
    )
  end

  defp delete_group_entry(catalog, group, entry) do
    tree_key = {entry.type, entry.priority, group, entry.state}
    tree = Map.get(catalog.state_trees, tree_key, :gb_sets.empty())
    old_head = smallest(tree)
    item = {entry.score, entry.key}

    next_tree =
      if :gb_sets.is_element(item, tree),
        do: :gb_sets.del_element(item, tree),
        else: tree

    catalog
    |> put_state_tree(tree_key, next_tree)
    |> replace_head(
      {entry.type, entry.priority, group},
      entry.state,
      old_head,
      smallest(next_tree)
    )
  end

  defp put_state_tree(catalog, tree_key, tree) do
    state_trees =
      if :gb_sets.is_empty(tree),
        do: Map.delete(catalog.state_trees, tree_key),
        else: Map.put(catalog.state_trees, tree_key, tree)

    %{catalog | state_trees: state_trees}
  end

  defp replace_head(catalog, scope, state, old_head, new_head) do
    heads = Map.get(catalog.heads, scope, :gb_sets.empty())
    heads = maybe_delete_head(heads, old_head, state)
    heads = maybe_put_head(heads, new_head, state)

    next_heads =
      if :gb_sets.is_empty(heads),
        do: Map.delete(catalog.heads, scope),
        else: Map.put(catalog.heads, scope, heads)

    %{catalog | heads: next_heads}
  end

  defp maybe_delete_head(heads, nil, _state), do: heads

  defp maybe_delete_head(heads, {score, key}, state) do
    item = {score, key, state}

    if :gb_sets.is_element(item, heads),
      do: :gb_sets.del_element(item, heads),
      else: heads
  end

  defp maybe_put_head(heads, nil, _state), do: heads
  defp maybe_put_head(heads, {score, key}, state), do: :gb_sets.add({score, key, state}, heads)

  defp smallest(tree) do
    if :gb_sets.is_empty(tree), do: nil, else: :gb_sets.smallest(tree)
  end

  defp due_catalog_groups(%{tag_prefix: tag_prefix, auto_partition?: auto?}) do
    groups = [:all, {:partition, tag_prefix}]
    if auto?, do: [:auto | groups], else: groups
  end

  defp selection_groups(:any), do: {:ok, [:all]}
  defp selection_groups(:auto), do: {:ok, [:auto]}

  defp selection_groups(partitions) when is_list(partitions) do
    groups =
      partitions
      |> Enum.reduce_while({:ok, []}, fn
        partition, {:ok, acc} when is_binary(partition) ->
          {:cont, {:ok, [{:partition, partition_tag_prefix(partition)} | acc]}}

        _invalid, _acc ->
          {:halt, :error}
      end)

    case groups do
      {:ok, reversed} -> {:ok, reversed |> Enum.reverse() |> Enum.uniq()}
      :error -> {:error, :invalid_partition_filter}
    end
  end

  defp selection_groups(partition) when is_binary(partition) or is_nil(partition),
    do: {:ok, [{:partition, partition_tag_prefix(partition)}]}

  defp selection_groups(_partition), do: {:error, :invalid_partition_filter}

  defp partition_tag_prefix(partition), do: "f:" <> Keys.tag(partition)

  defp state_selection(:any),
    do: {:ok, %{include: :any, exclude: MapSet.new()}}

  defp state_selection(state) when is_binary(state) and state != "",
    do: {:ok, %{include: MapSet.new([state]), exclude: MapSet.new()}}

  defp state_selection(states) when is_list(states) do
    if states != [] and Enum.all?(states, &(is_binary(&1) and &1 != "")),
      do: {:ok, %{include: MapSet.new(states), exclude: MapSet.new()}},
      else: {:error, :invalid_state_filter}
  end

  defp state_selection({:exclude, base, states}) when is_list(states) do
    if Enum.all?(states, &(is_binary(&1) and &1 != "")),
      do: put_excluded_states(state_selection(base), states),
      else: {:error, :invalid_state_filter}
  end

  defp state_selection(_state_filter), do: {:error, :invalid_state_filter}

  defp put_excluded_states({:ok, selection}, states),
    do: {:ok, %{selection | exclude: MapSet.new(states)}}

  defp put_excluded_states({:error, _reason} = error, _states), do: error

  defp initialize_scope_selection(
         catalog,
         scope,
         %{include: :any} = state_selection,
         heap,
         sequence,
         inspected_heads
       ) do
    heads = Map.get(catalog.heads, scope, :gb_sets.empty())

    push_next_head(
      catalog,
      scope,
      :gb_sets.iterator(heads),
      state_selection,
      heap,
      sequence,
      inspected_heads
    )
  end

  defp initialize_scope_selection(
         catalog,
         scope,
         %{include: %MapSet{} = states} = state_selection,
         heap,
         sequence,
         inspected_heads
       ) do
    Enum.reduce(states, {heap, sequence, inspected_heads}, fn state,
                                                              {heap, sequence, inspected_heads} ->
      if eligible_state?(state_selection, state) do
        tree_key = {elem(scope, 0), elem(scope, 1), elem(scope, 2), state}
        state_tree = Map.get(catalog.state_trees, tree_key, :gb_sets.empty())

        case :gb_sets.next(:gb_sets.iterator(state_tree)) do
          :none ->
            {heap, sequence, inspected_heads + 1}

          {{score, key}, next_state_entries} ->
            payload = {:state, next_state_entries}

            {:gb_trees.insert({score, key, sequence}, payload, heap), sequence + 1,
             inspected_heads + 1}
        end
      else
        {heap, sequence, inspected_heads + 1}
      end
    end)
  end

  defp push_next_head(
         catalog,
         scope,
         iterator,
         state_selection,
         heap,
         sequence,
         inspected_heads
       ) do
    case :gb_sets.next(iterator) do
      :none ->
        {heap, sequence, inspected_heads}

      {{score, key, state}, next_heads} ->
        inspected_heads = inspected_heads + 1

        if not eligible_state?(state_selection, state) do
          push_next_head(
            catalog,
            scope,
            next_heads,
            state_selection,
            heap,
            sequence,
            inspected_heads
          )
        else
          tree_key = {elem(scope, 0), elem(scope, 1), elem(scope, 2), state}
          state_tree = Map.get(catalog.state_trees, tree_key, :gb_sets.empty())

          case :gb_sets.next(:gb_sets.iterator(state_tree)) do
            {{^score, ^key}, next_state_entries} ->
              payload = {:head, scope, next_heads, state, next_state_entries}

              {:gb_trees.insert({score, key, sequence}, payload, heap), sequence + 1,
               inspected_heads}

            _missing_or_inconsistent ->
              push_next_head(
                catalog,
                scope,
                next_heads,
                state_selection,
                heap,
                sequence,
                inspected_heads
              )
          end
        end
    end
  end

  defp push_next_state(iterator, heap, sequence) do
    case :gb_sets.next(iterator) do
      :none ->
        {heap, sequence}

      {{score, key}, next_iterator} ->
        payload = {:state, next_iterator}
        {:gb_trees.insert({score, key, sequence}, payload, heap), sequence + 1}
    end
  end

  defp take_selected_keys(
         _catalog,
         heap,
         sequence,
         _state_selection,
         0,
         max_score,
         seen,
         keys,
         inspected_entries,
         inspected_heads
       ) do
    done? = :gb_trees.is_empty(heap) or selected_score_after?(heap, max_score)
    {keys, inspected_entries, inspected_heads, heap, sequence, seen, done?}
  end

  defp take_selected_keys(
         catalog,
         heap,
         sequence,
         state_selection,
         remaining,
         max_score,
         seen,
         keys,
         inspected_entries,
         inspected_heads
       ) do
    cond do
      :gb_trees.is_empty(heap) ->
        {keys, inspected_entries, inspected_heads, heap, sequence, seen, true}

      selected_score_after?(heap, max_score) ->
        {keys, inspected_entries, inspected_heads, heap, sequence, seen, true}

      true ->
        take_selected_key(
          catalog,
          heap,
          sequence,
          state_selection,
          remaining,
          max_score,
          seen,
          keys,
          inspected_entries,
          inspected_heads
        )
    end
  end

  defp take_selected_key(
         catalog,
         heap,
         sequence,
         state_selection,
         remaining,
         max_score,
         seen,
         keys,
         inspected_entries,
         inspected_heads
       ) do
    {{_score, key, _heap_sequence}, payload, heap} = :gb_trees.take_smallest(heap)

    {heap, sequence, inspected_heads} =
      case payload do
        {:head, scope, next_heads, _state, next_state_entries} ->
          {heap, sequence, inspected_heads} =
            push_next_head(
              catalog,
              scope,
              next_heads,
              state_selection,
              heap,
              sequence,
              inspected_heads
            )

          {heap, sequence} = push_next_state(next_state_entries, heap, sequence)
          {heap, sequence, inspected_heads}

        {:state, next_state_entries} ->
          {heap, sequence} = push_next_state(next_state_entries, heap, sequence)
          {heap, sequence, inspected_heads}
      end

    inspected_entries = inspected_entries + 1

    if MapSet.member?(seen, key) do
      take_selected_keys(
        catalog,
        heap,
        sequence,
        state_selection,
        remaining,
        max_score,
        seen,
        keys,
        inspected_entries,
        inspected_heads
      )
    else
      take_selected_keys(
        catalog,
        heap,
        sequence,
        state_selection,
        remaining - 1,
        max_score,
        MapSet.put(seen, key),
        [key | keys],
        inspected_entries,
        inspected_heads
      )
    end
  end

  defp selected_score_after?(_heap, :infinity), do: false

  defp selected_score_after?(heap, max_score) do
    {{score, _key, _sequence}, _payload} = :gb_trees.smallest(heap)
    score > max_score
  end

  defp valid_score?(score) when is_integer(score),
    do: score >= 0 and score <= @max_exact_score

  defp valid_score?(score) when is_float(score),
    do: score >= 0.0 and score <= @max_exact_score

  defp valid_score?(_score), do: false

  defp existing_entry_valid?(catalog, due_key) do
    case Map.get(catalog.entries, due_key) do
      nil -> true
      entry when is_map(entry) -> canonical_entry(due_key, entry) == {:ok, entry}
      _invalid -> false
    end
  end

  defp rebuild_expected_catalog(entries) when is_map(entries) do
    Enum.reduce_while(entries, {:ok, new()}, fn
      {due_key, entry}, {:ok, catalog} when is_binary(due_key) and is_map(entry) ->
        case canonical_entry(due_key, entry) do
          {:ok, ^entry} -> {:cont, {:ok, put_entry(catalog, entry)}}
          _invalid -> {:halt, {:error, :invalid_entry}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_entry}}
    end)
  end

  defp rebuild_expected_catalog(_entries), do: {:error, :invalid_entries}

  defp canonical_entry(due_key, %{score: score}) do
    with true <- valid_score?(score),
         {:ok, metadata} <- Keys.decode_due_key(due_key) do
      {:ok,
       metadata
       |> Map.put(:key, due_key)
       |> Map.put(:score, score)
       |> Map.put(:groups, due_catalog_groups(metadata))}
    else
      _invalid -> {:error, :invalid_entry}
    end
  end

  defp canonical_entry(_due_key, _entry), do: {:error, :invalid_entry}

  defp gb_set_maps_equal?(left, right)
       when is_map(left) and is_map(right) and map_size(left) == map_size(right) do
    Enum.all?(left, fn {key, left_set} ->
      case Map.fetch(right, key) do
        {:ok, right_set} -> :gb_sets.to_list(left_set) == :gb_sets.to_list(right_set)
        :error -> false
      end
    end)
  end

  defp gb_set_maps_equal?(_left, _right), do: false

  defp eligible_state?(%{include: :any, exclude: excluded}, state),
    do: not MapSet.member?(excluded, state)

  defp eligible_state?(%{include: %MapSet{} = included, exclude: excluded}, state),
    do: MapSet.member?(included, state) and not MapSet.member?(excluded, state)
end
