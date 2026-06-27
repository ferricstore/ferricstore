defmodule Ferricstore.Commands.Catalog do
  @moduledoc """
  Central registry of all commands supported by FerricStore.

  Each entry contains the command name, arity (negative = variadic), flags,
  first-key / last-key / step indices, and a short summary string.

  This module is the single source of truth consumed by:

    * `COMMAND COUNT`
    * `COMMAND LIST`
    * `COMMAND INFO`
    * `COMMAND DOCS`
    * `COMMAND GETKEYS`

  ## Arity convention (Redis)

    * Positive N — command takes exactly N arguments (including the command name).
    * Negative N — command takes at least |N| arguments.

  ## Key position convention

    * `first_key: 0` means the command has no key arguments.
    * For single-key commands: `first_key: 1, last_key: 1, step: 1`.
    * For multi-key variadic commands: `last_key: -1, step: 1`.
  """

  @type command_entry :: %{
          name: binary(),
          arity: integer(),
          flags: [binary()],
          first_key: integer(),
          last_key: integer(),
          step: integer(),
          summary: binary()
        }

  alias Ferricstore.Commands.Catalog.Entries
  alias Ferricstore.Commands.Extension

  @doc "Returns the full list of command entries."
  @spec all() :: [command_entry()]
  def all, do: Entries.all() ++ extension_commands()

  @doc "Returns the number of supported commands."
  @spec count() :: non_neg_integer()
  def count, do: Entries.count() + length(extension_commands())

  @doc "Returns all command names as lowercase strings."
  @spec names() :: [binary()]
  def names, do: Entries.names() ++ Enum.map(extension_commands(), & &1.name)

  @doc """
  Looks up a command entry by name (case-insensitive).

  Returns `{:ok, entry}` or `:error`.
  """
  @spec lookup(binary()) :: {:ok, command_entry()} | :error
  def lookup(name) when is_binary(name) do
    case Entries.lookup(name) do
      {:ok, _} = ok -> ok
      :error -> Extension.lookup(name)
    end
  end

  @doc """
  Looks up a command entry by an already-uppercase name.

  Avoids the `String.downcase/1` call in `lookup/1` when the caller
  already has an uppercase command name (e.g. from `normalise_cmd`).

  Returns `{:ok, entry}` or `:error`.
  """
  @spec lookup_upper(binary()) :: {:ok, command_entry()} | :error
  def lookup_upper(name) when is_binary(name) do
    case Entries.lookup_upper(name) do
      {:ok, _} = ok -> ok
      :error -> Extension.lookup_upper(name)
    end
  end

  @doc """
  Returns the Redis-style info tuple for a command entry.

  Format: `[name, arity, [flags...], first_key, last_key, step]`
  """
  @spec info_tuple(command_entry()) :: list()
  def info_tuple(%{} = cmd) do
    [cmd.name, cmd.arity, cmd.flags, cmd.first_key, cmd.last_key, cmd.step]
  end

  @doc """
  Extracts the key arguments from a command invocation.

  Given a command name and its arguments list, returns the arguments that are keys
  based on the catalog's first_key/last_key/step metadata.

  Returns `{:ok, keys}` or `{:error, reason}`.
  """
  @spec get_keys(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys(name, args) when is_binary(name) and is_list(args) do
    upper_name = String.upcase(name)

    with :not_flow <- flow_dynamic_keys(upper_name, args) do
      case lookup(name) do
        {:ok, %{first_key: 0}} ->
          {:ok, []}

        {:ok, %{first_key: first, last_key: last, step: step}} ->
          extract_keys(first, last, step, args)

        :error ->
          {:error, "ERR Invalid command specified"}
      end
    end
  end

  @doc """
  Same as `get_keys/2` but accepts an already-uppercase command name,
  avoiding the `String.downcase/1` call inside `lookup/1`.
  """
  @spec get_keys_upper(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys_upper(name, args) when is_binary(name) and is_list(args) do
    with :not_flow <- flow_dynamic_keys(name, args) do
      case lookup_upper(name) do
        {:ok, %{first_key: 0}} ->
          {:ok, []}

        {:ok, %{first_key: first, last_key: last, step: step}} ->
          extract_keys(first, last, step, args)

        :error ->
          {:error, "ERR Invalid command specified"}
      end
    end
  end

  defp flow_dynamic_keys("FLOW.CREATE_MANY", args),
    do: {:ok, args_at(args, flow_create_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.SPAWN_CHILDREN", args),
    do: {:ok, args_at(args, flow_spawn_children_key_indices(args))}

  defp flow_dynamic_keys("FLOW.COMPLETE_MANY", args),
    do: {:ok, args_at(args, flow_complete_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.RETRY_MANY", args),
    do: {:ok, args_at(args, flow_complete_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.FAIL_MANY", args),
    do: {:ok, args_at(args, flow_complete_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.CANCEL_MANY", args),
    do: {:ok, args_at(args, flow_cancel_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.TRANSITION_MANY", args),
    do: {:ok, args_at(args, flow_transition_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.VALUE.PUT", args),
    do: {:ok, args_at(args, flow_partition_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.CREATE", "FLOW.GET", "FLOW.HISTORY", "FLOW.SIGNAL"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL", "FLOW.EXTEND_LEASE"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 2))}

  defp flow_dynamic_keys("FLOW.TRANSITION", args),
    do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 3))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.CANCEL", "FLOW.REWIND"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.BY_PARENT", "FLOW.BY_ROOT", "FLOW.BY_CORRELATION"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in [
              "FLOW.CLAIM_DUE",
              "FLOW.RECLAIM",
              "FLOW.LIST",
              "FLOW.STATS",
              "FLOW.TERMINALS",
              "FLOW.FAILURES",
              "FLOW.INFO",
              "FLOW.STUCK"
            ],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.POLICY.SET", "FLOW.POLICY.GET"],
       do: {:ok, Enum.take(args, 1)}

  defp flow_dynamic_keys(_name, _args), do: :not_flow

  defp flow_partition_or_first_key_indices([], _option_start), do: []

  defp flow_partition_or_first_key_indices(args, option_start) do
    case flow_partition_key_indices(args, option_start) do
      [] -> [0]
      indices -> dedup_indices(indices)
    end
  end

  defp flow_partition_key_indices(args, option_start) do
    flow_partition_key_indices_until(args, option_start, length(args))
  end

  defp flow_partition_key_indices_until(args, option_start, option_end) do
    option_end = min(option_end, length(args))

    option_start..max(option_start, option_end - 1)
    |> Enum.reduce([], fn idx, acc ->
      cond do
        idx + 1 < option_end and arg_eq?(Enum.at(args, idx), "PARTITION") ->
          [idx + 1 | acc]

        idx + 1 < option_end and arg_eq?(Enum.at(args, idx), "PARTITIONS") ->
          flow_partition_count_indices(args, idx + 1, option_end) ++ acc

        true ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  defp flow_partition_count_indices(args, count_idx, option_end) do
    with count_arg when not is_nil(count_arg) <- Enum.at(args, count_idx),
         {count, ""} when count > 0 <- Integer.parse(to_string(count_arg)),
         true <- count_idx + count < option_end do
      Enum.to_list((count_idx + 1)..(count_idx + count))
    else
      _ -> []
    end
  end

  defp flow_create_many_key_indices([]), do: []

  defp flow_create_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 1, "ITEMS") do
        nil ->
          [0]

        items_idx ->
          item_width =
            if flow_option_present_until?(args, 1, items_idx, "PAYLOAD_REF"), do: 2, else: 3

          repeated_item_partition_indices(args, items_idx + 1, item_width)
      end
    end
  end

  defp flow_option_present_until?(args, start, option_end, option) do
    start..max(start, option_end - 1)
    |> Enum.any?(fn idx ->
      rem(idx - start, 2) == 0 and idx < option_end and arg_eq?(Enum.at(args, idx), option)
    end)
  end

  defp flow_spawn_children_key_indices(args) do
    option_end = option_index(args, 1, "ITEMS") || length(args)
    partition_keys = flow_partition_key_indices_until(args, 1, option_end)

    partition_keys =
      if option_end + 1 < length(args) and arg_eq?(Enum.at(args, option_end + 1), "MIXED") do
        partition_keys ++ repeated_item_partition_indices(args, option_end + 2, 4)
      else
        partition_keys
      end

    case partition_keys do
      [] -> Enum.take(0..(length(args) - 1), min(length(args), 1))
      keys -> dedup_indices(keys)
    end
  end

  defp flow_transition_many_key_indices([]), do: []

  defp flow_transition_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 3, "ITEMS") do
        nil -> [0]
        items_idx -> repeated_item_partition_indices(args, items_idx + 1, 4)
      end
    end
  end

  defp flow_complete_many_key_indices([]), do: []

  defp flow_complete_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 1, "ITEMS") do
        nil -> [0]
        items_idx -> repeated_item_partition_indices(args, items_idx + 1, 4)
      end
    end
  end

  defp flow_cancel_many_key_indices([]), do: []

  defp flow_cancel_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 1, "ITEMS") do
        nil -> [0]
        items_idx -> repeated_item_partition_indices(args, items_idx + 1, 3)
      end
    end
  end

  defp repeated_item_partition_indices(args, start_idx, step) do
    do_repeated_item_partition_indices(args, start_idx, step, [])
  end

  defp do_repeated_item_partition_indices(args, idx, step, acc) do
    if idx + step - 1 < length(args) do
      do_repeated_item_partition_indices(args, idx + step, step, [idx + 1 | acc])
    else
      Enum.reverse(acc)
    end
  end

  defp option_index(args, start, name), do: option_index(args, start, name, length(args))

  defp option_index(_args, idx, _name, len) when idx >= len, do: nil

  defp option_index(args, idx, name, len) do
    if arg_eq?(Enum.at(args, idx), name) do
      idx
    else
      option_index(args, idx + 2, name, len)
    end
  end

  defp dedup_indices(indices), do: Enum.uniq(indices)

  defp args_at(args, indices) do
    indices
    |> Enum.map(&Enum.at(args, &1))
    |> Enum.reject(&is_nil/1)
  end

  defp arg_eq?(arg, expected) when is_binary(arg) and is_binary(expected),
    do: ascii_eq_ignore_case?(arg, expected)

  defp arg_eq?(_arg, _expected), do: false

  defp extension_commands do
    Extension.non_shadowing_commands()
  end

  defp ascii_eq_ignore_case?(left, right) when byte_size(left) != byte_size(right), do: false
  defp ascii_eq_ignore_case?(<<>>, <<>>), do: true

  defp ascii_eq_ignore_case?(<<left, left_rest::binary>>, <<right, right_rest::binary>>) do
    ascii_upper(left) == ascii_upper(right) and ascii_eq_ignore_case?(left_rest, right_rest)
  end

  defp ascii_upper(char) when char >= ?a and char <= ?z, do: char - 32
  defp ascii_upper(char), do: char

  # Shared key extraction logic.
  defp extract_keys(first, last, step, args) do
    # Arguments are 0-indexed in our list, but first_key is 1-indexed
    # (position 1 = first arg after the command name).
    first_idx = first - 1
    last_idx = if last == -1, do: length(args) - 1, else: last - 1
    step_val = max(step, 1)

    keys =
      first_idx..last_idx//step_val
      |> Enum.map(fn i -> Enum.at(args, i) end)
      |> Enum.reject(&is_nil/1)

    {:ok, keys}
  end
end
