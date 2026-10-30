defmodule FerricstoreServer.Native.ZSetBlockingTest do
  @moduledoc """
  BZPOPMIN, BZPOPMAX and BZMPOP: they pop at once from the first sorted set
  that has members, or wait until a write gives one members.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias FerricstoreServer.Native.{Blocking, Session}

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ctx = FerricStore.Instance.get(:default)

    state = %{
      instance_ctx: ctx,
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    tag = "native:zblock:{#{System.unique_integer([:positive])}}"
    on_exit(fn -> for suffix <- ~w(a b c), do: FerricStore.del(tag <> suffix) end)
    %{ctx: ctx, state: state, k: &(tag <> &1)}
  end

  defp start(state, command, args) do
    assert {:ok, prepared} = Session.prepare_command(%{"command" => command, "args" => args})
    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor} = Blocking.start_prepared(prepared, state, meta)
    on_exit(fn -> Process.exit(worker, :kill) end)
    {worker, monitor, meta}
  end

  defp result({worker, _monitor, meta}) do
    assert_receive {:native_blocking_response, ^meta, ^worker, status, value}, 2_000
    {status, value}
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> :ok
      attempts == 0 -> :timeout
      true -> Process.sleep(10) && wait_until(fun, attempts - 1)
    end
  end

  test "BZPOPMIN and BZPOPMAX pop at once from the first key with members", %{
    ctx: ctx,
    state: state,
    k: k
  } do
    2 = Dispatcher.dispatch("ZADD", [k.("b"), "1", "x", "2", "y"], ctx)

    assert {:ok, [k.("b"), "x", "1"]} == result(start(state, "BZPOPMIN", [k.("a"), k.("b"), "1"]))
    assert {:ok, [k.("b"), "y", "2"]} == result(start(state, "BZPOPMAX", [k.("b"), "1"]))
  end

  test "BZPOPMIN waits for a ZADD", %{ctx: ctx, state: state, k: k} do
    worker = start(state, "BZPOPMIN", [k.("a"), "0"])
    assert :ok == wait_until(fn -> Ferricstore.Waiters.count(k.("a")) == 1 end)

    1 = Dispatcher.dispatch("ZADD", [k.("a"), "5", "late"], ctx)
    assert {:ok, [k.("a"), "late", "5"]} == result(worker)
  end

  test "BZMPOP waits for a STORE into the key and pops up to COUNT", %{
    ctx: ctx,
    state: state,
    k: k
  } do
    worker = start(state, "BZMPOP", ["0", "2", k.("a"), k.("b"), "MAX", "COUNT", "2"])
    assert :ok == wait_until(fn -> Ferricstore.Waiters.count(k.("b")) == 1 end)

    3 = Dispatcher.dispatch("ZADD", [k.("c"), "1", "p", "2", "q", "3", "r"], ctx)
    3 = Dispatcher.dispatch("ZUNIONSTORE", [k.("b"), "1", k.("c")], ctx)
    assert {:ok, [k.("b"), [["r", "3"], ["q", "2"]]]} == result(worker)
  end

  test "several waiters each take one member, in arrival order", %{ctx: ctx, state: state, k: k} do
    workers =
      for position <- 1..3 do
        worker = start(state, "BZPOPMIN", [k.("a"), "0"])
        assert :ok == wait_until(fn -> Ferricstore.Waiters.count(k.("a")) == position end)
        worker
      end

    3 = Dispatcher.dispatch("ZADD", [k.("a"), "1", "one", "2", "two", "3", "three"], ctx)

    assert [{:ok, [_, "one", "1"]}, {:ok, [_, "two", "2"]}, {:ok, [_, "three", "3"]}] =
             Enum.map(workers, &result/1)
  end

  test "a timeout answers nil and a wrong type is refused", %{ctx: ctx, state: state, k: k} do
    assert {:ok, nil} == result(start(state, "BZPOPMIN", [k.("a"), "0.1"]))
    :ok = Dispatcher.dispatch("SET", [k.("c"), "x"], ctx)

    assert {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"} ==
             result(start(state, "BZPOPMAX", [k.("c"), "1"]))
  end

  test "a served pop fires the pop's keyspace event", %{ctx: ctx, state: state, k: k} do
    :ok = Ferricstore.Config.set("notify-keyspace-events", "KEA")

    on_exit(fn ->
      Ferricstore.Config.set("notify-keyspace-events", "")
      Ferricstore.PubSub.cleanup(self())
    end)

    for event <- ["zpopmin", "zpopmax"],
        do: :ok = Ferricstore.PubSub.subscribe("__keyevent@0__:" <> event, self())

    # A key named like the side must not be mistaken for it.
    max_key = k.("a") <> "max"
    on_exit(fn -> FerricStore.del(max_key) end)
    2 = Dispatcher.dispatch("ZADD", [max_key, "1", "x", "2", "y"], ctx)

    assert {:ok, _} = result(start(state, "BZPOPMAX", [max_key, "1"]))
    assert_receive {:pubsub_message, "__keyevent@0__:zpopmax", ^max_key}, 1_000

    assert {:ok, _} = result(start(state, "BZMPOP", ["1", "1", max_key, "MIN"]))
    assert_receive {:pubsub_message, "__keyevent@0__:zpopmin", ^max_key}, 1_000
  end

  test "bad arguments are refused before waiting", %{state: state, k: k} do
    for {command, args} <- [
          {"BZPOPMIN", [k.("a"), "-1"]},
          {"BZPOPMIN", [k.("a"), "soon"]},
          {"BZMPOP", ["0", "0", k.("a"), "MIN"]},
          {"BZMPOP", ["0", "1", k.("a"), "SIDEWAYS"]}
        ] do
      assert {:ok, prepared} = Session.prepare_command(%{"command" => command, "args" => args})

      assert {:error, _status, _reason} =
               Blocking.start_prepared(prepared, state, %{request_id: make_ref()})
               |> then(fn
                 {:ok, worker, _monitor} ->
                   assert_receive {:native_blocking_response, _, ^worker, status, reason}, 1_000
                   {:error, status, reason}

                 error ->
                   error
               end)
    end
  end
end
