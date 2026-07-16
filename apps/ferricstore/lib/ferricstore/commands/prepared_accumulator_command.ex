defmodule Ferricstore.Commands.PreparedAccumulatorCommand do
  @moduledoc false

  alias Ferricstore.Commands.PreparedCommand

  @spec prepare_all([term()]) :: {:ok, [PreparedCommand.t()]} | {:error, binary()}
  def prepare_all(commands) when is_list(commands) do
    commands
    |> Enum.reduce_while({:ok, []}, fn command, {:ok, prepared} ->
      case prepare(command) do
        {:ok, current} -> {:cont, {:ok, [current | prepared]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  def prepare_all(_commands), do: invalid_command()

  @spec prepare(term()) :: {:ok, PreparedCommand.t()} | {:error, binary()}
  def prepare({:set, key, value, opts})
      when is_binary(key) and is_binary(value) and is_list(opts) do
    if valid_set_options?(opts) do
      args =
        [key, value]
        |> maybe_add_expiry(opts, :ttl, "PX", true)
        |> maybe_add_expiry(opts, :ex, "EX", false)
        |> maybe_add_expiry(opts, :px, "PX", false)
        |> maybe_add_flag(opts, :nx, "NX")
        |> maybe_add_flag(opts, :xx, "XX")

      PreparedCommand.prepare("SET", args)
    else
      invalid_command()
    end
  end

  def prepare({:get, key}) when is_binary(key), do: PreparedCommand.prepare("GET", [key])
  def prepare({:del, key}) when is_binary(key), do: PreparedCommand.prepare("DEL", [key])
  def prepare({:incr, key}) when is_binary(key), do: PreparedCommand.prepare("INCR", [key])

  def prepare({:incr_by, key, amount}) when is_binary(key) and is_integer(amount),
    do: PreparedCommand.prepare("INCRBY", [key, Integer.to_string(amount)])

  def prepare({:hset, key, fields}) when is_binary(key) and is_map(fields) do
    case encode_hash_fields(fields) do
      {:ok, field_args} -> PreparedCommand.prepare("HSET", [key | field_args])
      :error -> invalid_command()
    end
  end

  def prepare({:hget, key, field}) when is_binary(key) and is_binary(field),
    do: PreparedCommand.prepare("HGET", [key, field])

  def prepare({:lpush, key, elements}) when is_binary(key) and is_list(elements) do
    prepare_binary_list("LPUSH", key, elements)
  end

  def prepare({:rpush, key, elements}) when is_binary(key) and is_list(elements) do
    prepare_binary_list("RPUSH", key, elements)
  end

  def prepare({:sadd, key, members}) when is_binary(key) and is_list(members) do
    prepare_binary_list("SADD", key, members)
  end

  def prepare({:zadd, key, pairs}) when is_binary(key) and is_list(pairs) do
    case encode_zset_pairs(pairs) do
      {:ok, pair_args} -> PreparedCommand.prepare("ZADD", [key | pair_args])
      :error -> invalid_command()
    end
  end

  def prepare({:expire, key, ttl_ms}) when is_binary(key) and is_integer(ttl_ms),
    do: PreparedCommand.prepare("PEXPIRE", [key, Integer.to_string(ttl_ms)])

  def prepare(_invalid), do: invalid_command()

  defp maybe_add_expiry(args, opts, option, modifier, ignore_zero?) do
    case Keyword.get(opts, option) do
      nil -> args
      0 when ignore_zero? -> args
      value -> args ++ [modifier, Integer.to_string(value)]
    end
  end

  defp maybe_add_flag(args, opts, option, modifier) do
    if Keyword.get(opts, option, false), do: args ++ [modifier], else: args
  end

  defp valid_set_options?(opts) do
    Keyword.keyword?(opts) and
      Enum.all?(opts, fn
        {option, value} when option in [:ttl, :ex, :px] -> is_integer(value)
        {option, value} when option in [:nx, :xx] -> is_boolean(value)
        _invalid -> false
      end)
  end

  defp encode_hash_fields(fields) do
    Enum.reduce_while(fields, {:ok, []}, fn {field, value}, {:ok, acc} ->
      with {:ok, field} <- safe_to_string(field),
           {:ok, value} <- safe_to_string(value) do
        {:cont, {:ok, [value, field | acc]}}
      else
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      :error -> :error
    end
  end

  defp encode_zset_pairs(pairs) do
    Enum.reduce_while(pairs, {:ok, []}, fn
      {score, member}, {:ok, acc} when is_number(score) and is_binary(member) ->
        {:cont, {:ok, [member, to_string(score) | acc]}}

      _invalid, _acc ->
        {:halt, :error}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      :error -> :error
    end
  end

  defp prepare_binary_list(command, key, values) do
    if Enum.all?(values, &is_binary/1),
      do: PreparedCommand.prepare(command, [key | values]),
      else: invalid_command()
  end

  defp safe_to_string(value) do
    {:ok, to_string(value)}
  rescue
    Protocol.UndefinedError -> :error
  end

  defp invalid_command, do: {:error, "ERR invalid transaction command"}
end
