defmodule FerricstoreServer.AuthRateLimiter do
  @moduledoc false

  use GenServer

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.{Password, Rules}

  @default_max_attempts 10
  @default_window_ms 60_000
  @default_max_entries 10_000
  @minimum_max_entries 2
  @max_username_bytes Rules.max_username_bytes()
  @max_password_bytes Password.max_password_bytes()

  @type limiter_key :: {:ip | :user, binary()}
  @type entry :: %{
          count: pos_integer(),
          generation: pos_integer(),
          started_at_ms: integer(),
          touched: non_neg_integer()
        }
  @opaque reservation :: {:reservation, [{limiter_key(), integer()}]}
  @type rate_limit_error :: {:error, {:rate_limited, pos_integer()}}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec authenticate(term(), binary(), binary()) ::
          {:ok, binary()} | {:error, binary()} | rate_limit_error()
  def authenticate(peer, username, password) do
    authenticate(peer, username, password, &FerricstoreServer.Acl.Password.verify/2)
  end

  @doc false
  @spec authenticate(term(), binary(), binary(), (binary(), binary() -> boolean())) ::
          {:ok, binary()} | {:error, binary()} | rate_limit_error()
  def authenticate(peer, username, password, verifier) when is_function(verifier, 2) do
    case permit(peer, username, password) do
      {:ok, reservation} ->
        result = Acl.authenticate(username, password, verifier)

        if match?({:ok, _username}, result) do
          :ok = release_success(reservation)
        end

        result

      {:error, _reason} = error ->
        error
    end
  end

  @spec permit(term(), binary(), binary()) ::
          {:ok, reservation()} | rate_limit_error() | {:error, binary()}
  def permit(peer, username, password) do
    with :ok <- validate_credentials(username, password) do
      case reserve(peer, username) do
        {:ok, _reservation} = allowed ->
          allowed

        {:error, retry_after_ms} when is_integer(retry_after_ms) ->
          {:error, {:rate_limited, retry_after_ms}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec check(term(), binary()) :: :ok | {:error, pos_integer() | binary()}
  def check(peer, username) when is_binary(username) do
    if byte_size(username) <= @max_username_bytes do
      case reserve(peer, username) do
        {:ok, _reservation} -> :ok
        {:error, _reason} = error -> error
      end
    else
      {:error, "ERR authentication username exceeds #{@max_username_bytes} bytes"}
    end
  end

  @spec release_success(reservation()) :: :ok
  def release_success({:reservation, entries}) when is_list(entries) do
    GenServer.call(__MODULE__, {:release_success, entries})
  end

  @spec reset() :: :ok
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @spec info() :: %{entries: non_neg_integer()}
  def info do
    GenServer.call(__MODULE__, :info)
  end

  @impl true
  def init(_opts), do: {:ok, initial_state()}

  @impl true
  def handle_call({:reserve, peer, username}, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    window_ms = positive_env(:auth_rate_limit_window_ms, @default_window_ms)
    max_attempts = positive_env(:auth_rate_limit_max_attempts, @default_max_attempts)

    max_entries =
      positive_env(
        :auth_rate_limit_max_entries,
        @default_max_entries,
        @minimum_max_entries
      )

    state = maybe_discard_expired(state, now_ms, window_ms)
    keys = [{:ip, peer}, {:user, username}]

    case retry_after(state.entries, keys, now_ms, window_ms, max_attempts) do
      nil ->
        case capacity_retry_after(
               state.entries,
               keys,
               now_ms,
               window_ms,
               max_attempts,
               max_entries
             ) do
          nil ->
            state =
              state
              |> increment(keys, now_ms, window_ms)
              |> evict_oldest(
                max_entries,
                max_attempts,
                now_ms,
                window_ms,
                length(keys)
              )

            reservation = reservation(state.entries, keys)
            {:reply, {:ok, reservation}, state}

          retry_after_ms ->
            {:reply, {:error, retry_after_ms}, state}
        end

      retry_after_ms ->
        {:reply, {:error, retry_after_ms}, state}
    end
  end

  def handle_call({:release_success, reservation_entries}, _from, state) do
    entries =
      Enum.reduce(reservation_entries, state.entries, fn {key, generation}, entries ->
        release_reserved_entry(entries, key, generation)
      end)

    {:reply, :ok, %{state | entries: entries}}
  end

  def handle_call(:reset, _from, _state) do
    {:reply, :ok, initial_state()}
  end

  def handle_call(:info, _from, state) do
    {:reply, %{entries: map_size(state.entries)}, state}
  end

  defp maybe_discard_expired(%{last_cleanup_ms: nil} = state, now_ms, _window_ms) do
    %{state | last_cleanup_ms: now_ms}
  end

  defp maybe_discard_expired(state, now_ms, window_ms) do
    cleanup_interval_ms = min(window_ms, 30_000)

    if now_ms - state.last_cleanup_ms >= cleanup_interval_ms do
      discard_expired(state, now_ms, window_ms)
    else
      state
    end
  end

  defp discard_expired(state, now_ms, window_ms) do
    entries =
      Map.reject(state.entries, fn {_key, entry} ->
        now_ms - entry.started_at_ms >= window_ms
      end)

    %{state | entries: entries, last_cleanup_ms: now_ms}
  end

  defp retry_after(entries, keys, now_ms, window_ms, max_attempts) do
    keys
    |> Enum.flat_map(fn key ->
      case Map.get(entries, key) do
        %{count: count, started_at_ms: started_at_ms}
        when count >= max_attempts and now_ms - started_at_ms < window_ms ->
          [max(window_ms - (now_ms - started_at_ms), 1)]

        _other ->
          []
      end
    end)
    |> Enum.max(fn -> nil end)
  end

  defp capacity_retry_after(
         entries,
         keys,
         now_ms,
         window_ms,
         max_attempts,
         max_entries
       ) do
    new_entries = Enum.count(keys, &(not Map.has_key?(entries, &1)))

    if map_size(entries) + new_entries > max_entries do
      limited_remaining_ms =
        Enum.flat_map(entries, fn {_key, entry} ->
          if limited_entry?(entry, max_attempts, now_ms, window_ms) do
            [max(window_ms - (now_ms - entry.started_at_ms), 1)]
          else
            []
          end
        end)

      required_entries = length(Enum.uniq(keys))
      required_expirations = length(limited_remaining_ms) + required_entries - max_entries

      if required_expirations > 0 do
        limited_remaining_ms
        |> Enum.sort()
        |> Enum.at(required_expirations - 1, window_ms)
      end
    end
  end

  defp increment(state, keys, now_ms, window_ms) do
    Enum.reduce(keys, state, fn key, state ->
      touch = state.touch + 1

      entry =
        case Map.get(state.entries, key) do
          nil ->
            %{count: 1, generation: touch, started_at_ms: now_ms, touched: touch}

          existing when now_ms - existing.started_at_ms < window_ms ->
            %{existing | count: existing.count + 1, touched: touch}

          _expired ->
            %{count: 1, generation: touch, started_at_ms: now_ms, touched: touch}
        end

      %{state | entries: Map.put(state.entries, key, entry), touch: touch}
    end)
  end

  defp reservation(entries, keys) do
    versions =
      Enum.map(keys, fn key ->
        {key, Map.fetch!(entries, key).generation}
      end)

    {:reservation, versions}
  end

  defp release_reserved_entry(entries, key, generation) do
    case Map.get(entries, key) do
      %{generation: ^generation, count: 1} ->
        Map.delete(entries, key)

      %{generation: ^generation, count: count} = entry when count > 1 ->
        Map.put(entries, key, %{entry | count: count - 1})

      _expired_or_replaced ->
        entries
    end
  end

  defp evict_oldest(
         state,
         max_entries,
         _max_attempts,
         _now_ms,
         _window_ms,
         _required_entries
       )
       when map_size(state.entries) <= max_entries,
       do: state

  defp evict_oldest(
         state,
         max_entries,
         max_attempts,
         now_ms,
         window_ms,
         required_entries
       ) do
    low_water_mark = max(div(max_entries * 9, 10), 1)

    limited_count =
      Enum.count(state.entries, fn {_key, entry} ->
        limited_entry?(entry, max_attempts, now_ms, window_ms)
      end)

    keep_count = min(max(low_water_mark, limited_count + required_entries), max_entries)

    entries =
      state.entries
      |> Enum.sort_by(fn {_key, entry} ->
        if limited_entry?(entry, max_attempts, now_ms, window_ms) do
          {0, entry.touched}
        else
          {1, -entry.touched}
        end
      end)
      |> Enum.take(keep_count)
      |> Map.new()

    %{state | entries: entries}
  end

  defp limited_entry?(entry, max_attempts, now_ms, window_ms) do
    entry.count >= max_attempts and now_ms - entry.started_at_ms < window_ms
  end

  defp normalize_peer({ip, _port}) when is_tuple(ip), do: normalize_peer(ip)

  defp normalize_peer(peer) when is_tuple(peer) do
    case :inet.ntoa(peer) do
      address when is_list(address) -> List.to_string(address)
      _other -> inspect(peer)
    end
  rescue
    _error -> inspect(peer)
  end

  defp normalize_peer(peer) when is_binary(peer), do: peer
  defp normalize_peer(peer), do: inspect(peer)

  defp reserve(peer, username) do
    GenServer.call(__MODULE__, {:reserve, normalize_peer(peer), username_digest(username)})
  end

  defp username_digest(username), do: :crypto.hash(:sha256, username)

  defp validate_credentials(username, password)
       when not is_binary(username) or not is_binary(password) do
    {:error, "ERR authentication credentials must be binaries"}
  end

  defp validate_credentials(username, _password)
       when byte_size(username) > @max_username_bytes do
    {:error, "ERR authentication username exceeds #{@max_username_bytes} bytes"}
  end

  defp validate_credentials(_username, password)
       when byte_size(password) > @max_password_bytes do
    {:error, "ERR authentication password exceeds #{@max_password_bytes} bytes"}
  end

  defp validate_credentials(_username, _password), do: :ok

  defp positive_env(key, default, minimum \\ 1) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value >= minimum -> value
      _other -> default
    end
  end

  defp initial_state do
    %{entries: %{}, touch: 0, last_cleanup_ms: nil}
  end
end
