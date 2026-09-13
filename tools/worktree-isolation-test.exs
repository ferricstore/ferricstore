# Run after mix compile: elixir tools/worktree-isolation-test.exs
ExUnit.start()

defmodule Ferricstore.WorktreeIsolationSmokeTest do
  use ExUnit.Case, async: false

  @repo Path.expand("..", __DIR__)
  @tag timeout: 900_000
  test "two linked checkouts have separate live listeners and retained data" do
    build_env = System.get_env("MIX_ENV", "dev")

    assert File.regular?(
             Path.join(
               @repo,
               "_build/#{build_env}/lib/ferricstore/ebin/Elixir.Ferricstore.Bitcask.NIF.beam"
             )
           ),
           "Run MIX_ENV=#{build_env} mix compile first"

    base =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-worktrees-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    roots = Enum.map(["one", "two"], &Path.join(base, &1))
    File.mkdir_p!(base)

    on_exit(fn ->
      Enum.each(roots, &System.cmd("git", ["worktree", "remove", "--force", &1], cd: @repo))
      File.rm_rf!(base)
    end)

    Enum.each(roots, fn root ->
      {output, code} =
        System.cmd("git", ["worktree", "add", "--detach", root, "HEAD"],
          cd: @repo,
          stderr_to_stdout: true
        )

      assert code == 0, output
      File.cp_r!(Path.join(@repo, "config"), Path.join(root, "config"))
    end)

    [first_root, second_root] = roots
    {first, first_info} = start_instance(first_root, build_env)
    {second, second_info} = start_instance(second_root, build_env)
    assert first_info.data_dir == Path.join(first_root, "data")
    assert second_info.data_dir == Path.join(second_root, "data")
    ports = first_info.ports ++ second_info.ports
    assert length(Enum.uniq(ports)) == 8
    assert Enum.all?(ports, &(&1 > 0))

    Enum.each(ports, fn port ->
      assert {:ok, socket} =
               :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)

      :gen_tcp.close(socket)
    end)

    key = "worktree-isolation-sentinel"
    assert :ok = request(first, {:set, key, "first"})
    assert :ok = request(second, {:set, key, "second"})
    assert {:ok, "first"} = request(first, {:get, key})
    assert {:ok, "second"} = request(second, {:get, key})
    stop_instance(first)
    {restarted, restarted_info} = start_instance(first_root, build_env)
    assert {:ok, "first"} = request(restarted, {:get, key})
    assert {:ok, "second"} = request(second, {:get, key})
    assert restarted_info.data_dir == first_info.data_dir
    assert request(second, :info).ports == second_info.ports
    stop_instance(restarted)
    stop_instance(second)

    IO.inspect(%{first: first_info, second: second_info, restarted: restarted_info},
      label: "Verified worktree isolation"
    )
  end

  defp start_instance(root, build_env) do
    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 65_536},
        args: [
          "-pa",
          Path.join(@repo, "_build/#{build_env}/lib/*/ebin"),
          Path.join(__DIR__, "worktree-isolation-node.exs"),
          root
        ]
      ])

    on_exit(fn -> if Port.info(port), do: Port.close(port) end)
    assert {:ready, info} = await_reply(port, 240_000)
    IO.inspect(info, label: "Started #{root}")
    {port, info}
  end

  defp request(port, command) do
    Port.command(port, Base.encode64(:erlang.term_to_binary(command)) <> "\n")
    await_reply(port, 30_000)
  end

  defp await_reply(port, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    receive_reply(port, deadline, [])
  end

  defp receive_reply(port, deadline, logs) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, {:eol, "WORKTREE_REPLY " <> encoded}}} ->
        encoded |> Base.decode64!() |> :erlang.binary_to_term([:safe])

      {^port, {:data, {_kind, line}}} ->
        receive_reply(port, deadline, Enum.take([line | logs], 100))

      {^port, {:exit_status, status}} ->
        flunk("Worktree process exited #{status}: #{inspect(Enum.reverse(logs))}")
    after
      remaining -> flunk("Worktree process timed out: #{inspect(Enum.reverse(logs))}")
    end
  end

  defp stop_instance(port) do
    assert :ok = request(port, :stop)

    receive do
      {^port, {:exit_status, 0}} -> :ok
    after
      10_000 -> flunk("Worktree process did not exit cleanly")
    end
  end
end
