defmodule FerricstoreServer.PreparedCommandAclTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Dispatcher
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth

  test "prepared COPY without REPLACE requires destination read/write access" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("COPY", ["source:key", "destination:key"])

    assert {:error, _reason} =
             Auth.check_keys_cached(
               cache([
                 pattern("source:*", :read),
                 pattern("destination:*", :write)
               ]),
               prepared
             )

    assert :ok =
             Auth.check_keys_cached(
               cache([
                 pattern("source:*", :read),
                 pattern("destination:*", :rw)
               ]),
               prepared
             )

    assert {:error, reason} =
             Auth.check_keys_cached(
               cache([
                 pattern("source:*", :write),
                 pattern("destination:*", :read)
               ]),
               prepared
             )

    assert reason =~ "keys mentioned"
  end

  test "prepared COPY with REPLACE only reads the source and writes the destination" do
    assert {:ok, prepared} =
             Dispatcher.prepare_raw("COPY", ["source:key", "destination:key", "REPLACE"])

    assert :ok =
             Auth.check_keys_cached(
               cache([
                 pattern("source:*", :read),
                 pattern("destination:*", :write)
               ]),
               prepared
             )
  end

  test "prepared RENAME footprint requires source read/write and destination write" do
    assert {:ok, prepared} =
             Dispatcher.prepare_raw("RENAME", ["source:key", "destination:key"])

    assert {:error, _reason} =
             Auth.check_keys_cached(
               cache([
                 pattern("source:*", :read),
                 pattern("destination:*", :write)
               ]),
               prepared
             )

    assert :ok =
             Auth.check_keys_cached(
               cache([
                 pattern("source:*", :rw),
                 pattern("destination:*", :write)
               ]),
               prepared
             )
  end

  test "prepared MEMORY USAGE accepts read-only key scope" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("MEMORY", ["USAGE", "cache:key"])

    assert :ok =
             Auth.check_keys_cached(
               cache([pattern("cache:*", :read)]),
               prepared
             )

    assert {:error, _reason} =
             Auth.check_keys_cached(
               cache([pattern("cache:*", :write)]),
               prepared
             )
  end

  test "prepared read-modify-write commands require both key permissions" do
    write_only = cache([pattern("secret:*", :write)])
    read_write = cache([pattern("secret:*", :rw)])

    assert {:ok, plain_set} = Dispatcher.prepare_raw("SET", ["secret:key", "replacement"])
    assert :ok = Auth.check_keys_cached(write_only, plain_set)

    assert {:ok, set_get} =
             Dispatcher.prepare_raw("SET", ["secret:key", "replacement", "GET"])

    assert {:error, _reason} = Auth.check_keys_cached(write_only, set_get)
    assert :ok = Auth.check_keys_cached(read_write, set_get)

    assert {:ok, hset} = Dispatcher.prepare_raw("HSET", ["secret:hash", "field", "value"])
    assert {:error, _reason} = Auth.check_keys_cached(write_only, hset)
    assert :ok = Auth.check_keys_cached(read_write, hset)
  end

  test "prepared Pub/Sub access checks channel patterns without requiring key patterns" do
    assert {:ok, prepared} =
             Dispatcher.prepare_raw("PUBLISH", ["tenant:events", "payload"])

    allowed = %{
      enabled: true,
      keys: [],
      channels: [channel_pattern("tenant:*")]
    }

    denied = %{
      enabled: true,
      keys: :all,
      channels: [channel_pattern("other:*")]
    }

    assert :ok = Auth.check_prepared_resources_cached(allowed, prepared)
    assert {:error, reason} = Auth.check_prepared_resources_cached(denied, prepared)
    assert reason =~ "channels mentioned"
  end

  defp cache(patterns), do: %{enabled: true, keys: patterns}

  defp pattern(glob, access), do: {glob, access, Acl.compile_glob(glob)}
  defp channel_pattern(glob), do: {glob, Acl.compile_glob(glob)}
end
