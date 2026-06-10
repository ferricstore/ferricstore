defmodule Ferricstore.Flow.MutationAttrs do
  @moduledoc false

  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Store.Router

  import Ferricstore.Flow.Options,
    only: [
      required_non_neg_integer: 2,
      validate_ref_size: 2
    ]

  @default_state "queued"
  @default_priority 0
  @max_priority 2
  @default_lease_ms 30_000
  @max_history_hot_max_events 10_000
  @max_history_max_events 1_000_000
  @default_max_batch_items 1_000

  def validate_opts(opts, allowed \\ []) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      Keyword.has_key?(opts, :return) and not Keyword.get(allowed, :return, false) ->
        {:error, "ERR flow return option is not supported"}

      true ->
        :ok
    end
  end

  def reject_public_value_ref_input(opts, ref_key, value_key) do
    if Keyword.has_key?(opts, ref_key) do
      {:error, "ERR flow #{ref_key} input is not supported; use #{value_key}"}
    else
      :ok
    end
  end

  def validate_id(id) when is_binary(id) and id != "", do: :ok
  def validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  def validate_state(_name, state) when is_binary(state) and state != "", do: :ok
  def validate_state(name, _state), do: {:error, "ERR flow #{name} must be a non-empty string"}

  def reject_running_state_transition("running"),
    do: {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"}

  def reject_running_state_transition(_state), do: :ok

  def validate_lease_token(token) when is_binary(token) and token != "", do: :ok

  def validate_lease_token(_token),
    do: {:error, "ERR flow lease_token must be a non-empty string"}

  def optional_lease_token(opts) do
    case Keyword.get(opts, :lease_token, nil) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow lease_token must be a non-empty string"}
    end
  end

  def validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  def optional_now_ms(opts) do
    case Keyword.fetch(opts, :now_ms) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        {:ok, value}

      {:ok, _value} ->
        {:error, "ERR flow now_ms must be a non-negative integer"}

      :error ->
        {:ok, nil}
    end
  end

  def create_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, type} <- required_binary(opts, :type),
         {:ok, state} <- optional_binary(opts, :state, @default_state),
         {:ok, parent_flow_id} <- optional_binary_or_nil(opts, :parent_flow_id, nil),
         :ok <- validate_ref_size(:parent_flow_id, parent_flow_id),
         {:ok, root_flow_id} <- optional_binary_or_nil(opts, :root_flow_id, nil),
         :ok <- validate_ref_size(:root_flow_id, root_flow_id),
         root_flow_id = root_flow_id || id,
         {:ok, correlation_id} <- optional_binary_or_nil(opts, :correlation_id, nil),
         :ok <- validate_ref_size(:correlation_id, correlation_id),
         {:ok, idempotent} <- optional_boolean(opts, :idempotent, false),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, retention_ttl_ms} <- optional_retention_ttl_ms(opts),
         {:ok, history_hot_max_events} <- optional_history_hot_max_events(opts),
         {:ok, history_max_events} <- optional_history_max_events(opts),
         :ok <- validate_history_event_caps(history_hot_max_events, history_max_events),
         {:ok, priority} <- optional_priority(opts, @default_priority),
         {:ok, partition_key} <- optional_partition_key(opts) do
      partition_key = partition_key || Ferricstore.Flow.Keys.auto_partition_key(id)

      attrs =
        %{
          id: id,
          type: type,
          state: state,
          partition_key: partition_key
        }
        |> maybe_put_attr(:parent_flow_id, parent_flow_id)
        |> maybe_put_attr(:root_flow_id, if(root_flow_id == id, do: nil, else: root_flow_id))
        |> maybe_put_attr(:correlation_id, correlation_id)
        |> maybe_put_default_attr(:idempotent, idempotent, false)
        |> maybe_put_attr(:retention_ttl_ms, retention_ttl_ms)
        |> maybe_put_attr(:history_hot_max_events, history_hot_max_events)
        |> maybe_put_attr(:history_max_events, history_max_events)
        |> maybe_put_default_attr(:priority, priority, @default_priority)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value_ref(opts, :payload_ref)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)

      {:ok, attrs}
    end
  end

  def create_many_attrs(
        items,
        opts,
        partition_key,
        mismatch_error \\ "ERR flow partition_key mismatch in batch"
      ) do
    base_opts =
      opts
      |> Keyword.delete(:partition_key)
      |> Keyword.delete(:return)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- create_many_item_opts(item),
           {:ok, item_partition_key} <-
             many_item_partition_key(id, partition_key, item_opts, mismatch_error),
           {:ok, attrs} <-
             create_attrs(
               id,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
        {:cont, {:ok, [attrs | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
      {:error, _reason} = error -> error
    end
  end

  def spawn_children_attrs(parent_id, children, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(parent_id),
         :ok <- validate_children(children),
         {:ok, partition_key} <- required_partition_key(Keyword.get(opts, :partition_key)),
         {:ok, group_id} <- required_binary(opts, :group_id),
         {:ok, wait} <- optional_child_policy(opts, :wait, :all, [:all, :any, :none]),
         {:ok, on_child_failed} <-
           optional_child_policy(opts, :on_child_failed, :fail_parent, [
             :fail_parent,
             :ignore
           ]),
         {:ok, on_parent_closed} <-
           optional_child_policy(opts, :on_parent_closed, :cancel_children, [
             :cancel_children,
             :abandon_children
           ]),
         {:ok, exhaust_to} <- exhaust_to_opts(opts),
         {:ok, from_state} <- optional_binary_or_nil(opts, :from_state, nil),
         {:ok, wait_state} <- optional_binary_or_nil(opts, :wait_state, nil),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, child_attrs} <- create_many_attrs(children, opts, partition_key, :allow_override),
         :ok <- validate_unique_create_ids(child_attrs),
         :ok <- validate_no_parent_child_id(parent_id, child_attrs),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(parent_id, partition_key)) do
      attrs =
        %{
          id: parent_id,
          partition_key: partition_key,
          group_id: group_id,
          wait: wait,
          on_child_failed: on_child_failed,
          on_parent_closed: on_parent_closed,
          exhaust_to: exhaust_to,
          from_state: from_state,
          wait_state: wait_state,
          lease_token: lease_token,
          fencing_token: fencing_token,
          children: child_attrs
        }
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  def validate_children(children), do: Ferricstore.Flow.ChildPolicy.validate_children(children)

  def validate_no_parent_child_id(parent_id, child_attrs) do
    Ferricstore.Flow.ChildPolicy.validate_no_parent_child_id(parent_id, child_attrs)
  end

  def optional_child_policy(opts, key, default, allowed) do
    Ferricstore.Flow.ChildPolicy.optional_policy(opts, key, default, allowed)
  end

  def exhaust_to_opts(opts) do
    Ferricstore.Flow.ChildPolicy.exhaust_to_opts(opts)
  end

  def create_many_item_opts(item), do: Ferricstore.Flow.ManyItemOpts.create(item)

  def transition_attrs(id, from_state, to_state, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_state(:from, from_state),
         :ok <- validate_state(:to, to_state),
         :ok <- reject_running_state_transition(to_state),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, priority} <- optional_priority_or_nil(opts) do
      attrs =
        %{
          id: id,
          from_state: from_state,
          to_state: to_state,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:lease_token, lease_token)
        |> maybe_put_attr(:priority, priority)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value_ref(opts, :payload_ref)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)

      {:ok, attrs}
    end
  end

  def retry_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_public_value_ref_input(opts, :error_ref, :error),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms),
         {:ok, retry_policy} <- optional_retry_policy(opts) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :error)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)
        |> maybe_put_attr(:retry_policy, retry_policy)

      {:ok, attrs}
    end
  end

  def complete_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_public_value_ref_input(opts, :result_ref, :result),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:ttl_ms, ttl_ms)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :result)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  def extend_lease_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          lease_ms: lease_ms,
          partition_key: partition_key
        }
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  def complete_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- complete_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             complete_attrs(
               id,
               lease_token,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
        {:cont, {:ok, [attrs | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
      {:error, _reason} = error -> error
    end
  end

  def fail_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_public_value_ref_input(opts, :error_ref, :error),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:ttl_ms, ttl_ms)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :error)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  def cancel_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_external_ref_input(opts, :reason_ref, :reason),
         :ok <- validate_id(id),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms),
         :ok <- validate_cancel_reason_source(opts) do
      attrs =
        %{
          id: id,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:lease_token, lease_token)
        |> maybe_put_attr(:ttl_ms, ttl_ms)
        |> maybe_put_cancel_reason(opts)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  def validate_cancel_reason_source(opts) do
    if Keyword.has_key?(opts, :reason) and Keyword.has_key?(opts, :reason_ref) do
      {:error, "ERR flow reason and reason_ref are mutually exclusive"}
    else
      :ok
    end
  end

  def reject_external_ref_input(opts, ref_key, replacement_key) do
    if Keyword.has_key?(opts, ref_key) do
      {:error, "ERR flow #{ref_key} input is not supported; use #{replacement_key}"}
    else
      :ok
    end
  end

  def maybe_put_cancel_reason(attrs, opts) do
    if Keyword.has_key?(opts, :reason) do
      Map.put(attrs, :error, Keyword.fetch!(opts, :reason))
    else
      attrs
    end
  end

  def rewind_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_external_ref_input(opts, :reason_ref, :reason),
         :ok <- validate_id(id),
         {:ok, to_event} <- required_binary(opts, :to_event),
         {:ok, expect_state} <- optional_binary_or_nil(opts, :expect_state, nil),
         {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.history_key(id, partition_key)) do
      attrs =
        %{
          id: id,
          to_event: to_event,
          expect_state: expect_state,
          run_at_ms: run_at_ms,
          reason_ref: nil,
          partition_key: partition_key
        }
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  def required_binary(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-empty string"}
      :error -> {:error, "ERR flow #{key} is required"}
    end
  end

  def optional_binary(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a non-empty string"}
    end
  end

  def optional_binary_or_nil(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  def optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  def optional_non_neg_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-negative integer"}
      :error when is_integer(default) and default >= 0 -> {:ok, default}
      :error when is_nil(default) -> {:ok, nil}
      :error -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  def maybe_put_attr(attrs, _key, nil), do: attrs
  def maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  def maybe_put_default_attr(attrs, _key, value, value), do: attrs
  def maybe_put_default_attr(attrs, key, value, _default), do: Map.put(attrs, key, value)

  def fail_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- fail_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             fail_attrs(
               id,
               lease_token,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
        {:cont, {:ok, [attrs | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
      {:error, _reason} = error -> error
    end
  end

  def cancel_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- cancel_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             cancel_attrs(id, merge_many_item_opts(base_opts, item_opts, item_partition_key)) do
        {:cont, {:ok, [attrs | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
      {:error, _reason} = error -> error
    end
  end

  def retry_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- retry_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             retry_attrs(
               id,
               lease_token,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
        {:cont, {:ok, [attrs | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
      {:error, _reason} = error -> error
    end
  end

  def complete_many_item_opts(item), do: Ferricstore.Flow.ManyItemOpts.complete(item)

  def retry_many_item_opts(item), do: Ferricstore.Flow.ManyItemOpts.retry(item)

  def fail_many_item_opts(item), do: Ferricstore.Flow.ManyItemOpts.fail(item)

  def cancel_many_item_opts(item), do: Ferricstore.Flow.ManyItemOpts.cancel(item)

  def transition_many_attrs(items, opts, partition_key, from_state, to_state) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- transition_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             transition_attrs(
               id,
               from_state,
               to_state,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
        {:cont, {:ok, [attrs | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
      {:error, _reason} = error -> error
    end
  end

  def transition_many_item_opts(item), do: Ferricstore.Flow.ManyItemOpts.transition(item)

  def merge_many_item_opts(base_opts, item_opts, partition_key),
    do: Ferricstore.Flow.ManyItemOpts.merge(base_opts, item_opts, partition_key)

  def many_item_partition_key(
        id,
        partition_key,
        item_opts,
        mismatch_error \\ "ERR flow partition_key mismatch in batch"
      )

  def many_item_partition_key(id, nil, item_opts, _mismatch_error) do
    case optional_partition_key(partition_key: Keyword.get(item_opts, :partition_key)) do
      {:ok, nil} when is_binary(id) -> {:ok, Ferricstore.Flow.Keys.auto_partition_key(id)}
      other -> other
    end
  end

  def many_item_partition_key(_id, partition_key, item_opts, :allow_override)
      when is_binary(partition_key) do
    item_opts
    |> Keyword.get(:partition_key, partition_key)
    |> required_partition_key()
  end

  def many_item_partition_key(_id, partition_key, item_opts, mismatch_error)
      when is_binary(partition_key) do
    case Keyword.fetch(item_opts, :partition_key) do
      {:ok, item_partition_key} ->
        case required_partition_key(item_partition_key) do
          {:ok, ^partition_key} -> {:ok, partition_key}
          {:ok, _other} -> {:error, mismatch_error}
          {:error, _reason} = error -> error
        end

      :error ->
        {:ok, partition_key}
    end
  end

  def validate_create_many_items([_ | _] = items), do: validate_many_item_count(items)
  def validate_create_many_items(_items), do: {:error, "ERR flow items must be a non-empty list"}

  def validate_transition_many_items([_ | _] = items), do: validate_many_item_count(items)

  def validate_transition_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  def validate_complete_many_items([_ | _] = items), do: validate_many_item_count(items)

  def validate_complete_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  def validate_retry_many_items([_ | _] = items), do: validate_many_item_count(items)

  def validate_retry_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  def validate_fail_many_items([_ | _] = items), do: validate_many_item_count(items)

  def validate_fail_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  def validate_cancel_many_items([_ | _] = items), do: validate_many_item_count(items)

  def validate_cancel_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  def validate_many_item_count(items) do
    max = flow_max_batch_items()

    if length(items) <= max do
      :ok
    else
      {:error, "ERR flow batch item count exceeds maximum #{max}"}
    end
  end

  def flow_max_batch_items do
    case Application.get_env(:ferricstore, :flow_max_batch_items, @default_max_batch_items) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_batch_items
    end
  end

  def validate_unique_create_ids(_attrs_list, true), do: :ok
  def validate_unique_create_ids(attrs_list, false), do: validate_unique_create_ids(attrs_list)

  def validate_unique_transition_ids(_attrs_list, true), do: :ok

  def validate_unique_transition_ids(attrs_list, false),
    do: validate_unique_transition_ids(attrs_list)

  def validate_unique_create_ids(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  def validate_unique_transition_ids(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  def optional_non_neg_integer_or_nil(opts, key) do
    case Keyword.get(opts, key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  def optional_pos_integer_or_nil(opts, key) do
    case Keyword.get(opts, key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  def optional_retry_policy(opts) do
    if Keyword.has_key?(opts, :retry) do
      opts
      |> Keyword.get(:retry)
      |> RetryPolicy.normalize_override()
    else
      {:ok, nil}
    end
  end

  def optional_retention_ttl_ms(opts) do
    cond do
      Keyword.has_key?(opts, :ttl_ms) ->
        {:error, "ERR flow ttl_ms was renamed to retention_ttl_ms"}

      Keyword.has_key?(opts, :retention_ttl_ms) ->
        case Keyword.get(opts, :retention_ttl_ms) do
          value when is_integer(value) and value > 0 ->
            {:ok, value}

          _ ->
            {:error, "ERR flow retention_ttl_ms must be a positive integer"}
        end

      true ->
        {:ok, nil}
    end
  end

  def optional_history_hot_max_events(opts) do
    if Keyword.has_key?(opts, :history_hot_max_events) do
      case Keyword.get(opts, :history_hot_max_events) do
        value when is_integer(value) and value >= 0 ->
          max = flow_max_history_hot_max_events()

          if value <= max do
            {:ok, value}
          else
            {:error, "ERR flow history_hot_max_events exceeds maximum #{max}"}
          end

        _ ->
          {:error, "ERR flow history_hot_max_events must be a non-negative integer"}
      end
    else
      {:ok, nil}
    end
  end

  def optional_history_max_events(opts) do
    if Keyword.has_key?(opts, :history_max_events) do
      case Keyword.get(opts, :history_max_events) do
        value when is_integer(value) and value > 0 ->
          max = flow_max_history_max_events()

          if value <= max do
            {:ok, value}
          else
            {:error, "ERR flow history_max_events exceeds maximum #{max}"}
          end

        _ ->
          {:error, "ERR flow history_max_events must be a positive integer"}
      end
    else
      {:ok, nil}
    end
  end

  def validate_history_event_caps(nil, _history_max_events), do: :ok
  def validate_history_event_caps(_history_hot_max_events, nil), do: :ok

  def validate_history_event_caps(history_hot_max_events, history_max_events)
      when is_integer(history_hot_max_events) and is_integer(history_max_events) do
    if history_max_events >= history_hot_max_events do
      :ok
    else
      {:error,
       "ERR flow history_max_events must be greater than or equal to history_hot_max_events"}
    end
  end

  def flow_max_history_hot_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_hot_max_events,
           @max_history_hot_max_events
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @max_history_hot_max_events
    end
  end

  def flow_max_history_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_max_events,
           @max_history_max_events
         ) do
      value when is_integer(value) and value > 0 -> min(value, @max_history_max_events)
      _ -> @max_history_max_events
    end
  end

  def optional_priority(opts, default) do
    case Keyword.get(opts, :priority, default) do
      value when is_integer(value) and value >= 0 and value <= @max_priority -> {:ok, value}
      _ -> {:error, "ERR flow priority must be between 0 and #{@max_priority}"}
    end
  end

  def optional_priority_or_nil(opts) do
    case Keyword.get(opts, :priority, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 and value <= @max_priority -> {:ok, value}
      _ -> {:error, "ERR flow priority must be between 0 and #{@max_priority}"}
    end
  end

  def optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  def optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
    end
  end

  def required_partition_key(partition_key) do
    case optional_partition_key(partition_key: partition_key) do
      {:ok, nil} -> {:error, "ERR flow partition_key is required"}
      {:ok, value} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  def maybe_put_flow_value(attrs, opts, key) do
    if Keyword.has_key?(opts, key) do
      Map.put(attrs, key, Keyword.fetch!(opts, key))
    else
      attrs
    end
  end

  def maybe_put_flow_value_ref(attrs, opts, key) do
    if Keyword.has_key?(opts, key) do
      Map.put(attrs, key, Keyword.fetch!(opts, key))
    else
      attrs
    end
  end

  def maybe_put_named_value_opts(attrs, opts) do
    attrs
    |> maybe_put_flow_value_ref(opts, :values)
    |> maybe_put_flow_value_ref(opts, :value_refs)
    |> maybe_put_flow_value_ref(opts, :drop_values)
    |> maybe_put_flow_value_ref(opts, :override_values)
  end
end
