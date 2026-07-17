defmodule FerricstoreServer.AuthRateLimiterTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  @runtime_config Path.expand("../../../../../../config/runtime.exs", __DIR__)

  alias FerricstoreServer.Acl
  alias FerricstoreServer.AuthRateLimiter
  alias FerricstoreServer.Acl.Password

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)

    old_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    old_window_ms = Application.get_env(:ferricstore, :auth_rate_limit_window_ms)
    old_max_entries = Application.get_env(:ferricstore, :auth_rate_limit_max_entries)

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 2)
    Application.put_env(:ferricstore, :auth_rate_limit_window_ms, 60_000)
    Application.put_env(:ferricstore, :auth_rate_limit_max_entries, 4)
    AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:auth_rate_limit_max_attempts, old_max_attempts)
      restore_env(:auth_rate_limit_window_ms, old_window_ms)
      restore_env(:auth_rate_limit_max_entries, old_max_entries)
      AuthRateLimiter.reset()
    end)

    :ok
  end

  test "limits attempts by username across source IPs" do
    assert :ok = AuthRateLimiter.check({10, 0, 0, 1}, "alice")
    assert :ok = AuthRateLimiter.check({10, 0, 0, 2}, "alice")
    assert {:error, retry_after_ms} = AuthRateLimiter.check({10, 0, 0, 3}, "alice")
    assert retry_after_ms > 0
  end

  test "limits attempts by source IP across usernames" do
    assert :ok = AuthRateLimiter.check({10, 0, 0, 1}, "alice")
    assert :ok = AuthRateLimiter.check({10, 0, 0, 1}, "bob")
    assert {:error, retry_after_ms} = AuthRateLimiter.check({10, 0, 0, 1}, "charlie")
    assert retry_after_ms > 0
  end

  test "bounds in-memory limiter entries" do
    for index <- 1..20 do
      AuthRateLimiter.check({10, 0, div(index, 256), rem(index, 256)}, "user-#{index}")
    end

    assert AuthRateLimiter.info().entries <= 4
  end

  test "key flooding cannot evict a rate-limited bucket before its window expires" do
    victim_ip = {10, 9, 0, 1}

    assert :ok = AuthRateLimiter.check(victim_ip, "locked-user")
    assert :ok = AuthRateLimiter.check(victim_ip, "locked-user")
    assert {:error, retry_after_ms} = AuthRateLimiter.check(victim_ip, "locked-user")
    assert retry_after_ms > 0

    for index <- 1..20 do
      assert :ok =
               AuthRateLimiter.check(
                 {10, 10, div(index, 256), rem(index, 256)},
                 "flood-user-#{index}"
               )
    end

    assert AuthRateLimiter.info().entries <= 4

    assert {:error, retry_after_ms} = AuthRateLimiter.check(victim_ip, "locked-user")
    assert retry_after_ms > 0
  end

  test "protected bucket saturation fails closed for unknown identities" do
    for {peer, username} <- [
          {{10, 11, 0, 1}, "protected-user-1"},
          {{10, 11, 0, 2}, "protected-user-2"}
        ] do
      assert :ok = AuthRateLimiter.check(peer, username)
      assert :ok = AuthRateLimiter.check(peer, username)
    end

    assert AuthRateLimiter.info().entries == 4

    for _attempt <- 1..3 do
      assert {:error, retry_after_ms} =
               AuthRateLimiter.check({10, 11, 0, 3}, "unknown-at-capacity")

      assert retry_after_ms > 0
    end

    assert AuthRateLimiter.info().entries == 4
  end

  test "batch eviction drops below the cap instead of sorting on every new key" do
    Application.put_env(:ferricstore, :auth_rate_limit_max_entries, 10)

    for index <- 1..6 do
      assert :ok =
               AuthRateLimiter.check(
                 {10, 1, 0, index},
                 "batch-user-#{index}"
               )
    end

    assert AuthRateLimiter.info().entries <= 9
  end

  test "entry capacity smaller than one auth reservation is not accepted" do
    Application.put_env(:ferricstore, :auth_rate_limit_max_entries, 1)

    assert :ok = AuthRateLimiter.check({10, 1, 1, 1}, "minimum-capacity-user")
    assert AuthRateLimiter.info().entries == 2
  end

  test "production config rejects an auth entry capacity below two" do
    env_name = "FERRICSTORE_AUTH_RATE_LIMIT_MAX_ENTRIES"
    previous = System.get_env(env_name)
    System.put_env(env_name, "1")

    on_exit(fn -> restore_system_env(env_name, previous) end)

    assert_raise RuntimeError,
                 "FERRICSTORE_AUTH_RATE_LIMIT_MAX_ENTRIES must be an integer greater than or equal to 2",
                 fn -> Config.Reader.read!(@runtime_config, env: :prod) end
  end

  test "source ports share one IP bucket and a limited attempt skips password verification" do
    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    test_pid = self()

    verifier = fn password, stored_hash ->
      send(test_pid, {:password_verified, password})
      Password.verify(password, stored_hash)
    end

    assert {:error, _reason} =
             AuthRateLimiter.authenticate(
               {{10, 2, 0, 1}, 41_001},
               "missing-first",
               "wrong",
               verifier
             )

    assert_receive {:password_verified, "wrong"}

    assert {:error, {:rate_limited, retry_after_ms}} =
             AuthRateLimiter.authenticate(
               {{10, 2, 0, 1}, 41_002},
               "missing-second",
               "wrong",
               verifier
             )

    assert retry_after_ms > 0
    refute_receive {:password_verified, _password}
  end

  test "successful authentications do not exhaust the attempt budget" do
    username = "rate-limit-success"
    peer = {10, 2, 1, 1}
    assert :ok = Acl.set_user(username, ["on", "nopass"])
    on_exit(fn -> Acl.del_user(username) end)

    verifier = fn _password, _stored_hash -> true end

    for _attempt <- 1..4 do
      assert {:ok, ^username} =
               AuthRateLimiter.authenticate(peer, username, "valid", verifier)
    end
  end

  test "a success releases only its reservation and preserves earlier failures" do
    username = "rate-limit-preserve"
    peer = {10, 2, 2, 1}
    assert :ok = Acl.set_user(username, ["on", ">stored"])
    on_exit(fn -> Acl.del_user(username) end)

    verifier = fn password, _stored_hash -> password == "valid" end

    assert {:error, first_error} =
             AuthRateLimiter.authenticate(peer, username, "invalid", verifier)

    assert is_binary(first_error)
    assert {:ok, ^username} = AuthRateLimiter.authenticate(peer, username, "valid", verifier)

    assert {:error, second_ip_error} =
             AuthRateLimiter.authenticate(peer, "another-missing-user", "invalid", verifier)

    assert is_binary(second_ip_error)

    assert {:error, {:rate_limited, ip_retry_after_ms}} =
             AuthRateLimiter.authenticate(peer, "third-missing-user", "invalid", verifier)

    assert ip_retry_after_ms > 0

    assert {:error, second_user_error} =
             AuthRateLimiter.authenticate({10, 2, 2, 2}, username, "invalid", verifier)

    assert is_binary(second_user_error)

    assert {:error, {:rate_limited, user_retry_after_ms}} =
             AuthRateLimiter.authenticate({10, 2, 2, 3}, username, "valid", verifier)

    assert user_retry_after_ms > 0
  end

  test "a stale reservation cannot release a recreated bucket in the same millisecond" do
    Application.put_env(:ferricstore, :auth_rate_limit_max_entries, 2)
    peer = {10, 2, 3, 1}
    username = "recreated-user"

    assert {:ok, stale_reservation} =
             AuthRateLimiter.permit(peer, username, "password")

    %{entries: old_entries} = :sys.get_state(AuthRateLimiter)
    old_started_at_ms = old_entries |> Map.values() |> hd() |> Map.fetch!(:started_at_ms)

    assert {:ok, _other_reservation} =
             AuthRateLimiter.permit({10, 2, 3, 2}, "evicting-user", "password")

    assert {:ok, _current_reservation} =
             AuthRateLimiter.permit(peer, username, "password")

    :sys.replace_state(AuthRateLimiter, fn state ->
      entries =
        Map.new(state.entries, fn {key, entry} ->
          {key, %{entry | started_at_ms: old_started_at_ms}}
        end)

      %{state | entries: entries}
    end)

    assert :ok = AuthRateLimiter.release_success(stale_reservation)
    assert :ok = AuthRateLimiter.check(peer, username)
    assert {:error, retry_after_ms} = AuthRateLimiter.check(peer, username)
    assert retry_after_ms > 0
  end

  test "oversized credentials are rejected before password verification or retention" do
    test_pid = self()

    verifier = fn _password, _stored_hash ->
      send(test_pid, :password_verified)
      false
    end

    oversized_username = :binary.copy("u", 1_025)
    oversized_password = :binary.copy("p", 4_097)

    assert {:error, "ERR authentication username exceeds 1024 bytes"} =
             AuthRateLimiter.permit({10, 3, 0, 1}, oversized_username, "password")

    assert {:error, username_error} =
             AuthRateLimiter.authenticate(
               {10, 3, 0, 1},
               oversized_username,
               "password",
               verifier
             )

    assert username_error =~ "username exceeds 1024 bytes"
    refute_receive :password_verified

    assert {:error, "ERR authentication password exceeds 4096 bytes"} =
             AuthRateLimiter.permit({10, 3, 0, 1}, "bounded-user", oversized_password)

    assert {:error, password_error} =
             AuthRateLimiter.authenticate(
               {10, 3, 0, 1},
               "bounded-user",
               oversized_password,
               verifier
             )

    assert password_error =~ "password exceeds 4096 bytes"
    refute_receive :password_verified
    assert AuthRateLimiter.info().entries == 0
  end

  test "limiter stores fixed-size username digests" do
    username = :binary.copy("u", 1_024)
    assert :ok = AuthRateLimiter.check({10, 4, 0, 1}, username)

    %{entries: entries} = :sys.get_state(AuthRateLimiter)

    assert Enum.any?(entries, fn
             {{:user, digest}, _entry} -> byte_size(digest) == 32
             _other -> false
           end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp restore_system_env(name, nil), do: System.delete_env(name)
  defp restore_system_env(name, value), do: System.put_env(name, value)
end
