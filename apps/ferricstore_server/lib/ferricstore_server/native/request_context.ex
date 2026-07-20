defmodule FerricstoreServer.Native.RequestContext do
  @moduledoc false

  @max_identity_bytes 4_096
  @max_scopes 64
  @max_scope_bytes 1_024
  @max_scopes_bytes 64 * 1_025

  @spec from_payload(map(), map()) :: {:ok, map()} | {:error, binary()}
  def from_payload(payload, state) when is_map(payload) and is_map(state) do
    case Map.get(payload, "request_context") do
      nil -> {:ok, %{}}
      %{} = context -> trusted_context(context, state)
      _other -> invalid_context(state)
    end
  end

  def from_payload(_payload, _state), do: {:ok, %{}}

  @spec configured_trusted_users() :: MapSet.t(binary())
  def configured_trusted_users do
    case Application.get_env(:ferricstore, :native_trusted_request_context_users, []) do
      users when is_list(users) ->
        users
        |> Enum.flat_map(&trusted_user/1)
        |> MapSet.new()

      users when is_binary(users) ->
        users
        |> String.split([",", " "], trim: true)
        |> MapSet.new()

      :all ->
        MapSet.new(["*"])

      _other ->
        MapSet.new()
    end
  end

  defp trusted_context(context, state) do
    if trusted_connection?(state), do: normalize(context), else: {:ok, %{}}
  end

  defp invalid_context(state) do
    if trusted_connection?(state) do
      {:error, "ERR native request_context must be an object"}
    else
      {:ok, %{}}
    end
  end

  defp trusted_connection?(state) do
    users =
      case Map.fetch(state, :trusted_request_context_users) do
        {:ok, users} when is_list(users) or is_struct(users, MapSet) -> users
        {:ok, _invalid} -> []
        :error -> configured_trusted_users()
      end

    username = Map.get(state, :username)

    trusted_user?(users, "*") or
      (is_binary(username) and trusted_user?(users, username))
  end

  defp trusted_user?(%MapSet{} = users, username), do: MapSet.member?(users, username)
  defp trusted_user?(users, username) when is_list(users), do: username in users

  defp trusted_user(user) when is_binary(user), do: [user]
  defp trusted_user(user) when is_atom(user), do: [Atom.to_string(user)]
  defp trusted_user(_user), do: []

  defp normalize(%{} = context) do
    with {:ok, subject} <- normalize_identity(context_value(context, "subject", :subject)),
         {:ok, tenant} <- normalize_identity(context_value(context, "tenant", :tenant)),
         {:ok, scopes} <- normalize_scopes(context_value(context, "scopes", :scopes)) do
      normalized =
        %{}
        |> put_value("subject", subject)
        |> put_value("tenant", tenant)
        |> put_scopes(scopes)

      {:ok, normalized}
    end
  end

  defp normalize_identity(value) when value in [nil, ""], do: {:ok, nil}

  defp normalize_identity(value)
       when is_binary(value) and byte_size(value) <= @max_identity_bytes,
       do: {:ok, value}

  defp normalize_identity(value) when is_binary(value), do: limit_error()
  defp normalize_identity(_value), do: invalid_authority_error()

  defp put_value(payload, _key, value) when value in [nil, ""], do: payload
  defp put_value(payload, key, value) when is_binary(value), do: Map.put(payload, key, value)

  defp put_scopes(payload, []), do: payload
  defp put_scopes(payload, scopes), do: Map.put(payload, "scopes", scopes)

  defp normalize_scopes(scopes) when is_binary(scopes) do
    if byte_size(scopes) <= @max_scopes_bytes do
      scopes
      |> String.split([",", " "], trim: true)
      |> normalize_scope_list()
    else
      limit_error()
    end
  end

  defp normalize_scopes(scopes) when is_list(scopes), do: normalize_scope_list(scopes)
  defp normalize_scopes(nil), do: {:ok, []}
  defp normalize_scopes(_scopes), do: invalid_authority_error()

  defp normalize_scope_list(scopes) do
    bounded = Enum.take(scopes, @max_scopes + 1)

    cond do
      length(bounded) > @max_scopes ->
        limit_error()

      Enum.any?(bounded, fn
        scope when is_binary(scope) -> byte_size(scope) > @max_scope_bytes
        _invalid -> false
      end) ->
        limit_error()

      Enum.any?(bounded, &(not is_binary(&1))) ->
        invalid_authority_error()

      true ->
        normalized =
          bounded
          |> Enum.filter(&(is_binary(&1) and &1 != ""))
          |> Enum.uniq()

        {:ok, normalized}
    end
  end

  defp limit_error, do: {:error, "ERR native request_context exceeds limits"}

  defp invalid_authority_error,
    do: {:error, "ERR native request_context contains invalid authority"}

  defp context_value(context, string_key, atom_key) do
    case {Map.fetch(context, string_key), Map.fetch(context, atom_key)} do
      {:error, :error} -> nil
      {{:ok, value}, :error} -> value
      {:error, {:ok, value}} -> value
      {{:ok, value}, {:ok, value}} -> value
      {{:ok, _string_value}, {:ok, _atom_value}} -> {:ambiguous, string_key}
    end
  end
end
