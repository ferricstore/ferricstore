defmodule Ferricstore.Test.OwnedTmpDir do
  @moduledoc false

  # A directory under the system temp directory that is removed when the test
  # process owning it exits. The owner is the test that started the calling
  # Task (the last of `$callers`), else the caller, so a store made inside a
  # Task lives as long as its test.

  @spec create(binary()) :: binary()
  def create(name) when is_binary(name) do
    dir = Path.join(System.tmp_dir!(), name)
    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    remove_when_down(owner(), dir)
    dir
  end

  defp owner do
    case Process.get(:"$callers") do
      [_ | _] = callers -> List.last(callers)
      _none -> self()
    end
  end

  defp remove_when_down(owner, dir) do
    spawn(fn ->
      ref = Process.monitor(owner)

      receive do
        {:DOWN, ^ref, :process, _pid, _reason} -> File.rm_rf(dir)
      end
    end)

    :ok
  end
end
