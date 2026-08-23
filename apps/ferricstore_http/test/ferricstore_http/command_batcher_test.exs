defmodule FerricstoreHttp.CommandBatcherTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.{Auth, CommandBatcher, CommandService, Config, Deadline}

  setup do
    observer = self()
    task_supervisor = start_supervised!(Task.Supervisor)

    executor = fn session, requests, _config ->
      counts =
        Enum.map(requests, fn {prepared, _deadline} -> CommandService.command_count(prepared) end)

      send(observer, {:executed, session, counts})
      Enum.map(counts, &{:ok, {:commands, &1}})
    end

    batcher =
      start_supervised!(
        {CommandBatcher,
         %{
           executor: executor,
           max_commands: 4,
           name: nil,
           task_supervisor: task_supervisor,
           window_ms: 20
         }}
      )

    {:ok, config} = Config.new(backend: FerricstoreHttp.TestBackend)
    {:ok, one} = CommandService.prepare(%{"commands" => [["PING"]]})
    {:ok, two} = CommandService.prepare(%{"commands" => [["PING"], ["PING"]]})

    %{batcher: batcher, config: config, one: one, two: two}
  end

  test "coalesces concurrent requests only for the same authenticated session", context do
    auth = %Auth.Context{session: :shared, cache_key: <<1>>}

    first = request(context, auth, context.one)
    second = request(context, auth, context.two)

    assert {:ok, {:commands, 1}} = Task.await(first)
    assert {:ok, {:commands, 2}} = Task.await(second)
    assert_receive {:executed, :shared, counts}
    assert Enum.sort(counts) == [1, 2]
    refute_receive {:executed, _session, _counts}
  end

  test "never combines different authenticated sessions", context do
    first_auth = %Auth.Context{session: :first, cache_key: <<1>>}
    second_auth = %Auth.Context{session: :second, cache_key: <<2>>}

    assert {:ok, _result} = context |> request(first_auth, context.one) |> Task.await()
    assert {:ok, _result} = context |> request(second_auth, context.one) |> Task.await()

    assert_receive {:executed, :first, [1]}
    assert_receive {:executed, :second, [1]}
  end

  test "flushes before the combined command bound is exceeded", context do
    auth = %Auth.Context{session: :shared, cache_key: <<1>>}

    first = request(context, auth, context.two)
    second = request(context, auth, context.two)
    third = request(context, auth, context.one)

    assert {:ok, _result} = Task.await(first)
    assert {:ok, _result} = Task.await(second)
    assert {:ok, _result} = Task.await(third)

    assert_receive {:executed, :shared, first_counts}
    assert_receive {:executed, :shared, second_counts}
    assert Enum.sum(first_counts) <= 4
    assert Enum.sum(second_counts) <= 4
    assert Enum.sort(first_counts ++ second_counts) == [1, 2, 2]
  end

  test "does not submit work after its request deadline expires", context do
    auth = %Auth.Context{session: :shared, cache_key: <<1>>}
    deadline = Deadline.new(1)

    task =
      Task.async(fn ->
        CommandBatcher.request(
          context.batcher,
          auth,
          context.one,
          context.config,
          deadline
        )
      end)

    assert {:error, :request_timeout} = Task.await(task)
    refute_receive {:executed, _session, _counts}
  end

  test "executes a valid empty command envelope without crashing metrics", context do
    auth = %Auth.Context{session: :shared, cache_key: <<1>>}
    {:ok, empty} = CommandService.prepare(%{"commands" => []})

    assert {:ok, {:commands, 0}} = context |> request(auth, empty) |> Task.await()
    assert_receive {:executed, :shared, [0]}
  end

  defp request(context, auth, prepared) do
    Task.async(fn ->
      CommandBatcher.request(
        context.batcher,
        auth,
        prepared,
        context.config,
        Deadline.new(1_000)
      )
    end)
  end
end
