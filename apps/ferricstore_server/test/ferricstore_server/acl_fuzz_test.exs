defmodule FerricstoreServer.AclFuzzTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth

  setup do
    Acl.reset!()
    :ok
  end

  @malformed_globs [
    "[",
    "[]",
    "[z-a]",
    "[\\",
    "[^",
    "key:[",
    "key:[]",
    "key:[z-a]",
    "key:[\\",
    "key:[^",
    "tenant:[abc",
    "tenant:[z-a]:*"
  ]

  test "generated glob patterns compile without crashes or broad fallback grants" do
    for glob <- @malformed_globs do
      regex = Acl.compile_glob(glob)
      assert %Regex{} = regex

      compiled = [{glob, :rw, regex}]
      assert is_boolean(Acl.key_matches_any?(glob, :read, compiled))
      refute Acl.key_matches_any?("outside:" <> glob, :read, compiled)
      refute Acl.key_matches_any?(glob <> ":outside", :write, compiled)
    end

    for glob <- generated_globs() do
      regex = Acl.compile_glob(glob)
      assert %Regex{} = regex
      assert is_boolean(Acl.key_matches_any?(glob, :read, [{glob, :rw, regex}]))
    end
  end

  test "generated key access rules preserve read, write, and rw boundaries" do
    for i <- 1..120 do
      user = "acl_fuzz_#{i}"
      prefix = "acl:fuzz:#{i}:"

      assert :ok =
               Acl.set_user(user, [
                 "on",
                 "nopass",
                 "-@all",
                 "+@all",
                 "resetkeys",
                 "%R~#{prefix}read:*",
                 "%W~#{prefix}write:*",
                 "~#{prefix}rw:*"
               ])

      suffix = generated_suffix(i)

      assert :ok = Acl.check_key_access(user, prefix <> "read:" <> suffix, :read)
      assert {:error, _} = Acl.check_key_access(user, prefix <> "read:" <> suffix, :write)

      assert :ok = Acl.check_key_access(user, prefix <> "write:" <> suffix, :write)
      assert {:error, _} = Acl.check_key_access(user, prefix <> "write:" <> suffix, :read)

      assert :ok = Acl.check_key_access(user, prefix <> "rw:" <> suffix, :read)
      assert :ok = Acl.check_key_access(user, prefix <> "rw:" <> suffix, :write)

      assert {:error, _} = Acl.check_key_access(user, "outside:" <> suffix, :read)
      assert {:error, _} = Acl.check_key_access(user, "outside:" <> suffix, :write)
    end
  end

  test "cached command ACL agrees with canonical ACL for generated rule sets" do
    commands = ~w(GET SET DEL FLUSHDB INFO JSON.GET JSON.SET XADD XRANGE ZADD ZRANGE)

    rule_sets = [
      ["on", "nopass", "allkeys", "-@all", "+GET"],
      ["on", "nopass", "allkeys", "+@all", "-SET"],
      ["on", "nopass", "allkeys", "-@all", "+@read"],
      ["on", "nopass", "allkeys", "-@all", "+@write"],
      ["on", "nopass", "allkeys", "+@all", "-@dangerous"],
      ["on", "nopass", "allkeys", "+@read", "+@write", "-DEL"],
      ["off", "nopass", "allkeys", "+@all"]
    ]

    for {rules, i} <- Enum.with_index(rule_sets) do
      user = "acl_cache_fuzz_#{i}"
      assert :ok = Acl.set_user(user, rules)
      cache = Auth.build_acl_cache(user)

      for cmd <- commands do
        assert permission_result(Auth.check_command_cached(cache, cmd)) ==
                 permission_result(Acl.check_command(user, cmd))
      end
    end
  end

  defp generated_globs do
    alphabet = ["a", "b", "c", ":", "*", "?", "[", "]", "-", "\\", "."]

    for i <- 1..160 do
      count = rem(i, 8) + 1

      1..count
      |> Enum.map(fn j -> Enum.at(alphabet, rem(i * 17 + j * 31, length(alphabet))) end)
      |> IO.iodata_to_binary()
    end
  end

  defp generated_suffix(i) do
    chars = ["a", "Z", "0", ":", "-", "_", ".", "[", "]", "*", "?"]
    count = rem(i + 3, 9) + 1

    1..count
    |> Enum.map(fn j -> Enum.at(chars, rem(i * 13 + j * 7, length(chars))) end)
    |> IO.iodata_to_binary()
  end

  defp permission_result(:ok), do: :ok
  defp permission_result({:error, _}), do: :error
end
