defmodule Ferricstore.EmbeddedStringValidation do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Strings.SetOptions

  @set_options_default %{
    expire_at_ms: 0,
    nx: false,
    xx: false,
    get: false,
    keepttl: false,
    has_expiry: false
  }

  @spec parse_set_options(keyword()) :: {:ok, map()} | {:error, binary()}
  def parse_set_options(opts) when is_list(opts) do
    with {:ok, ast_opts} <- set_option_ast(opts) do
      SetOptions.from_ast(ast_opts, @set_options_default)
    end
  end

  def parse_set_options(_opts), do: {:error, "ERR syntax error"}

  @spec parse_getex_options(keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, binary()}
  def parse_getex_options(opts) when is_list(opts) do
    opts
    |> Enum.reduce_while({:ok, %{persist: :unset, ttl: :unset}}, fn
      {:ttl, ttl}, {:ok, %{ttl: :unset} = parsed} when is_integer(ttl) and ttl > 0 ->
        {:cont, {:ok, %{parsed | ttl: ttl}}}

      {:ttl, ttl}, {:ok, %{ttl: :unset}} when not is_integer(ttl) or ttl <= 0 ->
        {:halt, {:error, "ERR invalid expire time in 'getex' command"}}

      {:persist, persist}, {:ok, %{persist: :unset} = parsed} when is_boolean(persist) ->
        {:cont, {:ok, %{parsed | persist: persist}}}

      _invalid_or_duplicate, _acc ->
        {:halt, {:error, "ERR syntax error"}}
    end)
    |> normalize_getex_options()
  end

  def parse_getex_options(_opts), do: {:error, "ERR syntax error"}

  @spec relative_expiry(term(), pos_integer(), binary()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  def relative_expiry(value, multiplier, command)
      when is_integer(multiplier) and multiplier > 0 and is_binary(command) do
    with :ok <- validate_positive_expiry(value, command) do
      {:ok, CommandTime.now_ms() + value * multiplier}
    end
  end

  @spec validate_positive_expiry(term(), binary()) :: :ok | {:error, binary()}
  def validate_positive_expiry(value, _command) when is_integer(value) and value > 0, do: :ok

  def validate_positive_expiry(_value, command) when is_binary(command),
    do: {:error, "ERR invalid expire time in '#{command}' command"}

  @spec validate_value_size(map(), term()) :: :ok | {:error, binary()}
  def validate_value_size(ctx, value) when is_binary(value) do
    max_value_size = Map.get(ctx, :max_value_size, 1_048_576)

    if byte_size(value) > max_value_size do
      {:error, "ERR value too large (#{byte_size(value)} bytes, max #{max_value_size} bytes)"}
    else
      :ok
    end
  end

  def validate_value_size(_ctx, _value), do: :ok

  defp set_option_ast(opts) do
    opts
    |> Enum.reduce_while({:ok, []}, fn
      {:ttl, 0}, {:ok, acc} -> {:cont, {:ok, acc}}
      {:ttl, value}, {:ok, acc} -> {:cont, {:ok, [{:px, value} | acc]}}
      {:exat, value}, {:ok, acc} -> {:cont, {:ok, [{:exat, value} | acc]}}
      {:pxat, value}, {:ok, acc} -> {:cont, {:ok, [{:pxat, value} | acc]}}
      {:nx, true}, {:ok, acc} -> {:cont, {:ok, [:nx | acc]}}
      {:nx, false}, {:ok, acc} -> {:cont, {:ok, acc}}
      {:xx, true}, {:ok, acc} -> {:cont, {:ok, [:xx | acc]}}
      {:xx, false}, {:ok, acc} -> {:cont, {:ok, acc}}
      {:get, true}, {:ok, acc} -> {:cont, {:ok, [:get | acc]}}
      {:get, false}, {:ok, acc} -> {:cont, {:ok, acc}}
      {:keepttl, true}, {:ok, acc} -> {:cont, {:ok, [:keepttl | acc]}}
      {:keepttl, false}, {:ok, acc} -> {:cont, {:ok, acc}}
      {:cache, value}, {:ok, acc} when is_atom(value) -> {:cont, {:ok, acc}}
      _invalid, _acc -> {:halt, {:error, "ERR syntax error"}}
    end)
    |> case do
      {:ok, ast_opts} -> {:ok, Enum.reverse(ast_opts)}
      error -> error
    end
  end

  defp normalize_getex_options({:ok, %{persist: true, ttl: ttl}}) when is_integer(ttl),
    do: {:error, "ERR syntax error"}

  defp normalize_getex_options({:ok, %{persist: true}}), do: {:ok, 0}

  defp normalize_getex_options({:ok, %{ttl: ttl}}) when is_integer(ttl),
    do: {:ok, CommandTime.now_ms() + ttl}

  defp normalize_getex_options({:ok, _parsed}), do: {:ok, nil}
  defp normalize_getex_options(error), do: error
end
