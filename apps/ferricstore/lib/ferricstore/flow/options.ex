defmodule Ferricstore.Flow.Options do
  @moduledoc false

  @max_ref_size 4_096
  @max_exact_integer 9_007_199_254_740_991

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

  def validate_ref_size(_key, nil), do: :ok

  def validate_ref_size(key, value) when is_binary(value) do
    if byte_size(value) <= @max_ref_size do
      :ok
    else
      {:error, "ERR flow #{key} too large (max #{@max_ref_size} bytes)"}
    end
  end

  def optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  def required_non_neg_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      {:ok, value} when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_integer}"}

      {:ok, _} ->
        {:error, "ERR flow #{key} must be a non-negative integer"}

      :error ->
        {:error, "ERR flow #{key} is required"}
    end
  end

  def optional_non_neg_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      {:ok, value} when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_integer}"}

      {:ok, _} ->
        {:error, "ERR flow #{key} must be a non-negative integer"}

      :error
      when is_integer(default) and default >= 0 and default <= @max_exact_integer ->
        {:ok, default}

      :error when is_integer(default) and default > @max_exact_integer ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_integer}"}

      :error when is_nil(default) ->
        {:ok, nil}

      :error ->
        {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  def maybe_put_keyword(opts, _key, nil), do: opts
  def maybe_put_keyword(opts, key, value), do: Keyword.put(opts, key, value)

  def maybe_put_attr(attrs, _key, nil), do: attrs
  def maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  def maybe_put_default_attr(attrs, _key, value, value), do: attrs
  def maybe_put_default_attr(attrs, key, value, _default), do: Map.put(attrs, key, value)
end
