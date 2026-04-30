defmodule Ferricstore.Test.MockStore do
  @moduledoc false

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @overflow_error "ERR increment or decrement would overflow"

  @doc """
  Creates an in-process mock store backed by an Agent.

  Each call returns a fresh, isolated store map that satisfies the
  command handler contract: `get`, `get_meta`, `put`, `delete`,
  `exists?`, `keys`, `flush`, `dbsize`.

  `initial` is a map of `%{key => {value, expire_at_ms}}`.
  """
  @spec make(map()) :: map()
  def make(initial \\ %{}) do
    {:ok, pid} = Agent.start_link(fn -> initial end)
    tmp_dir = System.tmp_dir!() <> "/ferricstore_mock_#{:erlang.unique_integer([:positive])}"
    prob_dir = Path.join(tmp_dir, "prob")
    File.mkdir_p!(prob_dir)

    # Per-type resource registries for probabilistic data structures.
    # Each registry is an Agent holding a map of key => {resource, meta}.
    {:ok, cms_pid} = Agent.start_link(fn -> %{} end)
    {:ok, cuckoo_pid} = Agent.start_link(fn -> %{} end)
    {:ok, bloom_pid} = Agent.start_link(fn -> %{} end)

    cms_registry = make_registry(cms_pid, prob_dir, ".cms")
    cuckoo_registry = make_registry(cuckoo_pid, prob_dir, ".cuckoo")
    bloom_registry = make_registry(bloom_pid, prob_dir, ".bloom")

    %{
      cms_registry: cms_registry,
      cuckoo_registry: cuckoo_registry,
      bloom_registry: bloom_registry,
      get: fn key -> Agent.get(pid, &read_value(&1, key)) end,
      get_meta: fn key -> Agent.get(pid, &read_meta(&1, key)) end,
      put: fn key, value, expire_at_ms ->
        Agent.update(pid, &Map.put(&1, key, {value, expire_at_ms}))
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, &Map.delete(&1, key))
        :ok
      end,
      exists?: fn key -> Agent.get(pid, &key_alive?(&1, key)) end,
      keys: fn -> Agent.get(pid, &live_keys/1) end,
      flush: fn -> Agent.update(pid, fn _ -> %{} end) end,
      dbsize: fn -> Agent.get(pid, &live_count/1) end,
      compound_get: fn _redis_key, compound_key ->
        Agent.get(pid, fn state ->
          case Map.get(state, compound_key) do
            nil -> nil
            {value, 0} -> value
            {value, exp} -> if Ferricstore.Test.MockStore.alive?(exp), do: value, else: nil
          end
        end)
      end,
      compound_get_meta: fn _redis_key, compound_key ->
        Agent.get(pid, fn state ->
          case Map.get(state, compound_key) do
            nil -> nil
            {value, 0} -> {value, 0}
            {value, exp} -> if Ferricstore.Test.MockStore.alive?(exp), do: {value, exp}, else: nil
          end
        end)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        Agent.update(pid, &Map.put(&1, compound_key, {value, expire_at_ms}))
        :ok
      end,
      compound_delete: fn _redis_key, compound_key ->
        Agent.update(pid, &Map.delete(&1, compound_key))
        :ok
      end,
      compound_scan: fn _redis_key, prefix ->
        Agent.get(pid, fn state ->
          now = System.os_time(:millisecond)
          state
          |> Enum.filter(fn {k, {_, exp}} ->
            is_binary(k) and String.starts_with?(k, prefix) and (exp == 0 or exp > now)
          end)
          |> Enum.map(fn {k, {v, _}} ->
            # Extract field name after the null byte separator
            case :binary.split(k, <<0>>) do
              [_prefix, field] -> {field, v}
              _ -> {k, v}
            end
          end)
          |> Enum.sort_by(fn {field, _} -> field end)
        end)
      end,
      compound_count: fn _redis_key, prefix ->
        Agent.get(pid, fn state ->
          now = System.os_time(:millisecond)
          Enum.count(state, fn {k, {_, exp}} ->
            is_binary(k) and String.starts_with?(k, prefix) and (exp == 0 or exp > now)
          end)
        end)
      end,
      compound_delete_prefix: fn _redis_key, prefix ->
        Agent.update(pid, fn state ->
          Map.reject(state, fn {k, _} ->
            is_binary(k) and String.starts_with?(k, prefix)
          end)
        end)
        :ok
      end,
      incr: fn key, delta ->
        Agent.get_and_update(pid, fn state ->
          case Map.get(state, key) do
            nil ->
              case Ferricstore.Test.MockStore.checked_integer_add(0, delta) do
                {:ok, new_val} ->
                  new_state = Map.put(state, key, {Integer.to_string(new_val), 0})
                  {{:ok, new_val}, new_state}

                :overflow ->
                  {{:error, @overflow_error}, state}
              end

            {value, exp} ->
              if exp != 0 and not Ferricstore.Test.MockStore.alive?(exp) do
                case Ferricstore.Test.MockStore.checked_integer_add(0, delta) do
                  {:ok, new_val} ->
                    new_state = Map.put(state, key, {Integer.to_string(new_val), 0})
                    {{:ok, new_val}, new_state}

                  :overflow ->
                    {{:error, @overflow_error}, state}
                end
              else
                case Integer.parse(value) do
                  {int_val, ""} ->
                    case Ferricstore.Test.MockStore.checked_integer_add(int_val, delta) do
                      {:ok, new_val} ->
                        new_state = Map.put(state, key, {Integer.to_string(new_val), exp})
                        {{:ok, new_val}, new_state}

                      :overflow ->
                        {{:error, @overflow_error}, state}
                    end

                  _ ->
                    {{:error, "ERR value is not an integer or out of range"}, state}
                end
              end
          end
        end)
      end,
      incr_float: fn key, delta ->
        Agent.get_and_update(pid, fn state ->
          case Map.get(state, key) do
            nil ->
              new_str = Ferricstore.Test.MockStore.fmt_float(delta)
              new_state = Map.put(state, key, {new_str, 0})
              {{:ok, new_str}, new_state}

            {value, exp} ->
              if exp != 0 and not Ferricstore.Test.MockStore.alive?(exp) do
                new_str = Ferricstore.Test.MockStore.fmt_float(delta)
                new_state = Map.put(state, key, {new_str, 0})
                {{:ok, new_str}, new_state}
              else
                case Float.parse(value) do
                  {float_val, ""} ->
                    new_val = float_val + delta
                    new_str = Ferricstore.Test.MockStore.fmt_float(new_val)
                    new_state = Map.put(state, key, {new_str, exp})
                    {{:ok, new_str}, new_state}

                  _ ->
                    case Integer.parse(value) do
                      {int_val, ""} ->
                        new_val = int_val + delta
                        new_str = Ferricstore.Test.MockStore.fmt_float(new_val)
                        new_state = Map.put(state, key, {new_str, exp})
                        {{:ok, new_str}, new_state}

                      _ ->
                        {{:error, "ERR value is not a valid float"}, state}
                    end
                end
              end
          end
        end)
      end,
      append: fn key, suffix ->
        Agent.get_and_update(pid, fn state ->
          case Map.get(state, key) do
            nil ->
              new_state = Map.put(state, key, {suffix, 0})
              {{:ok, byte_size(suffix)}, new_state}

            {value, exp} ->
              if exp != 0 and not Ferricstore.Test.MockStore.alive?(exp) do
                new_state = Map.put(state, key, {suffix, 0})
                {{:ok, byte_size(suffix)}, new_state}
              else
                new_val = value <> suffix
                new_state = Map.put(state, key, {new_val, exp})
                {{:ok, byte_size(new_val)}, new_state}
              end
          end
        end)
      end,
      getset: fn key, new_value ->
        Agent.get_and_update(pid, fn state ->
          old =
            case Map.get(state, key) do
              nil -> nil
              {v, 0} -> v
              {v, exp} -> if Ferricstore.Test.MockStore.alive?(exp), do: v, else: nil
            end

          new_state = Map.put(state, key, {new_value, 0})
          {old, new_state}
        end)
      end,
      getdel: fn key ->
        Agent.get_and_update(pid, fn state ->
          case Map.get(state, key) do
            nil ->
              {nil, state}

            {v, 0} ->
              {v, Map.delete(state, key)}

            {v, exp} ->
              if Ferricstore.Test.MockStore.alive?(exp) do
                {v, Map.delete(state, key)}
              else
                {nil, Map.delete(state, key)}
              end
          end
        end)
      end,
      getex: fn key, new_expire_at_ms ->
        Agent.get_and_update(pid, fn state ->
          case Map.get(state, key) do
            nil ->
              {nil, state}

            {v, exp} ->
              if exp != 0 and not Ferricstore.Test.MockStore.alive?(exp) do
                {nil, Map.delete(state, key)}
              else
                new_state = Map.put(state, key, {v, new_expire_at_ms})
                {v, new_state}
              end
          end
        end)
      end,
      setrange: fn key, offset, value ->
        Agent.get_and_update(pid, fn state ->
          current =
            case Map.get(state, key) do
              nil -> ""
              {v, 0} -> v
              {v, exp} -> if Ferricstore.Test.MockStore.alive?(exp), do: v, else: ""
            end

          exp =
            case Map.get(state, key) do
              nil -> 0
              {_, e} -> e
            end

          padded =
            if byte_size(current) < offset do
              current <> String.duplicate(<<0>>, offset - byte_size(current))
            else
              current
            end

          new_val =
            binary_part(padded, 0, offset) <>
              value <>
              if offset + byte_size(value) < byte_size(padded) do
                binary_part(padded, offset + byte_size(value), byte_size(padded) - offset - byte_size(value))
              else
                ""
              end

          new_state = Map.put(state, key, {new_val, exp})
          {{:ok, byte_size(new_val)}, new_state}
        end)
      end,
      prob_dir: fn -> prob_dir end
    }
  end

  # --- Pure helpers applied inside Agent.get/2 callbacks ---

  defp read_value(state, key) do
    case Map.get(state, key) do
      nil -> nil
      {value, 0} -> value
      {value, exp} -> if Ferricstore.Test.MockStore.alive?(exp), do: value, else: nil
    end
  end

  defp read_meta(state, key) do
    case Map.get(state, key) do
      nil -> nil
      {value, 0} -> {value, 0}
      {value, exp} -> if Ferricstore.Test.MockStore.alive?(exp), do: {value, exp}, else: nil
    end
  end

  defp key_alive?(state, key) do
    case Map.get(state, key) do
      nil -> false
      {_, 0} -> true
      {_, exp} -> Ferricstore.Test.MockStore.alive?(exp)
    end
  end

  defp live_keys(state) do
    now = System.os_time(:millisecond)

    state
    |> Enum.filter(fn {_, {_, exp}} -> exp == 0 or exp > now end)
    |> Enum.map(fn {k, _} -> k end)
  end

  defp live_count(state) do
    now = System.os_time(:millisecond)
    Enum.count(state, fn {_, {_, exp}} -> exp == 0 or exp > now end)
  end

  # Public so anonymous functions passed to Agent can use it.
  @doc false
  def alive?(exp), do: exp > System.os_time(:millisecond)

  @doc false
  def checked_integer_add(value, delta) do
    result = value + delta

    if result > @max_int64 or result < @min_int64 do
      :overflow
    else
      {:ok, result}
    end
  end

  @doc false
  def fmt_float(f) when is_float(f),
    do: :erlang.float_to_binary(f, [:compact, decimals: 17])

  def fmt_float(f) when is_integer(f), do: "#{f}.0"

  defp make_registry(reg_pid, base_dir, extension) do
    %{
      get: fn key ->
        Agent.get(reg_pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(reg_pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(reg_pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(base_dir, "#{safe}#{extension}")
      end
    }
  end
end
