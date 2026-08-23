defmodule FerricstoreHttp.Auth.CacheTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Auth.Cache

  setup do
    clock = start_supervised!({Agent, fn -> 1_000 end})

    start_supervised!({Task.Supervisor, name: FerricstoreHttp.Auth.Cache.TaskSupervisor})

    start_supervised!(
      {Cache,
       max_entries: 2,
       ttl_ms: 10_000,
       touch_interval_ms: 1_000,
       sweep_interval_ms: 60_000,
       clock: fn -> Agent.get(clock, & &1) end}
    )

    %{clock: clock}
  end

  test "reuses a successful opaque session without retaining credential material" do
    calls =
      start_supervised!(%{
        id: :successful_authentication_calls,
        start: {Agent, :start_link, [fn -> 0 end]}
      })

    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, {"worker", "secret"})

    authenticate = fn ->
      Agent.update(calls, &(&1 + 1))
      {:ok, {:opaque_session, 1}}
    end

    assert {:ok, {:opaque_session, 1}, :miss} = Cache.fetch(key, :peer, authenticate, 1_000)
    assert {:ok, {:opaque_session, 1}, :hit} = Cache.fetch(key, :peer, authenticate, 1_000)
    assert Agent.get(calls, & &1) == 1

    retained = inspect(:sys.get_state(Cache)) <> inspect(:ets.tab2list(Cache))
    refute retained =~ "worker"
    refute retained =~ "secret"
  end

  test "does not cache authentication failures" do
    calls =
      start_supervised!(%{
        id: :failed_authentication_calls,
        start: {Agent, :start_link, [fn -> 0 end]}
      })

    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :wrong_password)

    authenticate = fn ->
      Agent.update(calls, &(&1 + 1))
      {:error, :unauthenticated}
    end

    assert {:error, :unauthenticated, :miss} = Cache.fetch(key, :peer, authenticate, 1_000)
    assert {:error, :unauthenticated, :miss} = Cache.fetch(key, :peer, authenticate, 1_000)
    assert Agent.get(calls, & &1) == 2
    assert Cache.stats().entries == 0
  end

  test "collapses concurrent cache misses for one credential" do
    parent = self()
    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :shared_credential)

    authenticate = fn ->
      send(parent, {:authentication_started, self()})

      receive do
        :continue -> {:ok, :shared_session}
      end
    end

    callers =
      for _index <- 1..32 do
        Task.async(fn -> Cache.fetch(key, :same_peer, authenticate, 2_000) end)
      end

    assert_receive {:authentication_started, authentication_task}, 1_000
    send(authentication_task, :continue)

    assert Enum.all?(Task.await_many(callers, 2_000), fn
             {:ok, :shared_session, source} when source in [:miss, :coalesced, :hit] -> true
             _other -> false
           end)

    refute_receive {:authentication_started, _duplicate}, 50
  end

  test "cancels an authentication flight after its last caller times out" do
    parent = self()
    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :timeout)

    assert {:error, :authentication_unavailable, :timeout} =
             Cache.fetch(
               key,
               :peer,
               fn ->
                 send(parent, {:authentication_started, self()})
                 Process.sleep(:infinity)
               end,
               25
             )

    assert_receive {:authentication_started, authentication_task}, 100
    monitor = Process.monitor(authentication_task)
    assert_receive {:DOWN, ^monitor, :process, ^authentication_task, _reason}, 1_000
    assert_eventually(fn -> Cache.stats().pending == 0 end)
  end

  test "bounds distinct pending authentication flights" do
    parent = self()

    tasks =
      for index <- 1..3 do
        Task.async(fn ->
          key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, {:pending, index})

          Cache.fetch(
            key,
            :peer,
            fn ->
              send(parent, {:authentication_started, index, self()})
              Process.sleep(:infinity)
            end,
            200
          )
        end)
      end

    assert_receive {:authentication_started, _, _}, 100
    assert_receive {:authentication_started, _, _}, 100
    refute_receive {:authentication_started, _, _}, 50
    assert Cache.stats().pending == 2

    results = Task.await_many(tasks, 1_000)

    assert Enum.all?(results, fn
             {:error, :authentication_unavailable, source} when source in [:miss, :timeout] ->
               true

             _other ->
               false
           end)

    assert_eventually(fn -> Cache.stats().pending == 0 end)
  end

  test "expires sessions and evicts the least recently used entry", %{clock: clock} do
    first = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :first)
    second = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :second)
    third = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :third)

    assert {:ok, :first_session, :miss} =
             Cache.fetch(first, :peer, fn -> {:ok, :first_session} end, 1_000)

    Agent.update(clock, &(&1 + 1_001))

    assert {:ok, :second_session, :miss} =
             Cache.fetch(second, :peer, fn -> {:ok, :second_session} end, 1_000)

    Agent.update(clock, &(&1 + 1_001))
    assert {:ok, :first_session, :hit} = Cache.fetch(first, :peer, fn -> flunk() end, 1_000)

    Agent.update(clock, &(&1 + 1_001))

    assert {:ok, :third_session, :miss} =
             Cache.fetch(third, :peer, fn -> {:ok, :third_session} end, 1_000)

    assert Cache.stats().entries == 2
    assert {:ok, :first_session, :hit} = Cache.fetch(first, :peer, fn -> flunk() end, 1_000)

    assert {:ok, :second_reauthenticated, :miss} =
             Cache.fetch(second, :peer, fn -> {:ok, :second_reauthenticated} end, 1_000)

    Agent.update(clock, &(&1 + 10_001))

    assert {:ok, :first_reauthenticated, :miss} =
             Cache.fetch(first, :peer, fn -> {:ok, :first_reauthenticated} end, 1_000)
  end

  test "samples recent-use writes instead of mutating ETS on every cache hit", %{clock: clock} do
    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :hot)

    assert {:ok, :hot_session, :miss} =
             Cache.fetch(key, :peer, fn -> {:ok, :hot_session} end, 1_000)

    [{^key, :hot_session, _expires_at_ms, inserted_at_ms}] = :ets.lookup(Cache, key)
    Agent.update(clock, &(&1 + 999))

    for _request <- 1..100 do
      assert {:ok, :hot_session, :hit} = Cache.fetch(key, :peer, fn -> flunk() end, 1_000)
    end

    assert [{^key, :hot_session, _expires_at_ms, ^inserted_at_ms}] = :ets.lookup(Cache, key)

    Agent.update(clock, &(&1 + 1))
    assert {:ok, :hot_session, :hit} = Cache.fetch(key, :peer, fn -> flunk() end, 1_000)
    assert [{^key, :hot_session, _expires_at_ms, touched_at_ms}] = :ets.lookup(Cache, key)
    assert touched_at_ms == inserted_at_ms + 1_000
  end

  test "invalidates only the matching stale session" do
    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :rotated)

    assert {:ok, :old_session, :miss} =
             Cache.fetch(key, :peer, fn -> {:ok, :old_session} end, 1_000)

    assert :ok = Cache.invalidate(key, :different_session)
    assert {:ok, :old_session, :hit} = Cache.fetch(key, :peer, fn -> flunk() end, 1_000)

    assert :ok = Cache.invalidate(key, :old_session)

    assert {:ok, :new_session, :miss} =
             Cache.fetch(key, :peer, fn -> {:ok, :new_session} end, 1_000)
  end

  test "invalidates an identity by its underlying session" do
    key = Cache.reference(__MODULE__, FerricstoreHttp.TestBackend, :identity)
    identity = %Auth.Identity{session: :old_session, subject: "worker"}

    assert {:ok, ^identity, :miss} =
             Cache.fetch(key, :peer, fn -> {:ok, identity} end, 1_000)

    assert :ok = Cache.invalidate(key, :different_session)
    assert {:ok, ^identity, :hit} = Cache.fetch(key, :peer, fn -> flunk() end, 1_000)

    assert :ok = Cache.invalidate(key, :old_session)

    assert {:ok, :new_identity, :miss} =
             Cache.fetch(key, :peer, fn -> {:ok, :new_identity} end, 1_000)
  end

  test "scopes cached sessions to the exact network peer" do
    authorization = "Basic " <> Base.encode64("worker:secret")
    {:ok, config} = Config.new(backend: FerricstoreHttp.TestBackend)
    first_peer = {{127, 0, 0, 1}, 40_001}
    second_peer = {{127, 0, 0, 1}, 40_002}

    assert {:ok, %Auth.Context{session: {:session, ^first_peer}}} =
             Auth.resolve(authorization, first_peer, config)

    assert {:ok, %Auth.Context{session: {:session, ^first_peer}}} =
             Auth.resolve(authorization, first_peer, config)

    assert {:ok, %Auth.Context{session: {:session, ^second_peer}}} =
             Auth.resolve(authorization, second_peer, config)

    assert Cache.stats().entries == 2
  end

  test "applies trusted request context after the session cache lookup" do
    authorization = "Basic " <> Base.encode64("worker:secret")

    {:ok, config} =
      Config.new(backend: FerricstoreHttp.TestBackend, trust_context_headers: true)

    assert {:ok, %Auth.Context{subject: "function-a", session: session}} =
             Auth.resolve(
               authorization,
               :peer,
               %{"x-ferricstore-subject" => "function-a"},
               config
             )

    assert {:ok, %Auth.Context{subject: "function-b", session: ^session}} =
             Auth.resolve(
               authorization,
               :peer,
               %{"x-ferricstore-subject" => "function-b"},
               config
             )

    assert Cache.stats().entries == 1
  end

  defp assert_eventually(predicate, attempts \\ 100)

  defp assert_eventually(predicate, attempts) when attempts > 0 do
    if predicate.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(predicate, attempts - 1)
    end
  end

  defp assert_eventually(_predicate, 0), do: flunk("condition did not become true")
end
