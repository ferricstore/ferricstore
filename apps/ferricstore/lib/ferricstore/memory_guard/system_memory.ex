defmodule Ferricstore.MemoryGuard.SystemMemory do
  @moduledoc false

  def detect_memory_limit do
    cgroup_v2_limit() ||
      cgroup_v1_limit() ||
      host_total_memory() ||
      proc_meminfo_total() ||
      1_073_741_824
  end

  def parse_sysctl_memsize(output) when is_binary(output) do
    case Integer.parse(String.trim(output)) do
      {bytes, ""} when bytes > 0 -> bytes
      _ -> nil
    end
  end

  def process_rss_bytes do
    read_proc_self_rss() ||
      read_ps_rss() ||
      erlang_total_memory()
  end

  def parse_ps_rss_kb(output) when is_binary(output) do
    output
    |> String.split()
    |> List.first()
    |> case do
      nil ->
        nil

      value ->
        case Integer.parse(value) do
          {kb, ""} when kb > 0 -> kb * 1024
          _ -> nil
        end
    end
  end

  defp cgroup_v2_limit do
    case File.read("/sys/fs/cgroup/memory.max") do
      {:ok, "max\n"} ->
        nil

      {:ok, data} ->
        case Integer.parse(String.trim(data)) do
          {bytes, _} when bytes > 0 -> bytes
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp cgroup_v1_limit do
    case File.read("/sys/fs/cgroup/memory/memory.limit_in_bytes") do
      {:ok, data} ->
        case Integer.parse(String.trim(data)) do
          # Very large values (>= 2^62) mean "no limit"
          {bytes, _} when bytes > 0 and bytes < 4_611_686_018_427_387_904 -> bytes
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp host_total_memory do
    memsup_total_memory() || sysctl_total_memory()
  end

  defp memsup_total_memory do
    data = apply(:memsup, :get_system_memory_data, [])

    case data do
      list when is_list(list) -> Keyword.get(list, :total_memory)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp sysctl_total_memory do
    case System.cmd("sysctl", ["-n", "hw.memsize"], stderr_to_stdout: true) do
      {output, 0} -> parse_sysctl_memsize(output)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp proc_meminfo_total do
    case File.read("/proc/meminfo") do
      {:ok, content} ->
        case Regex.run(~r/MemTotal:\s+(\d+)\s+kB/, content) do
          [_, kb_str] ->
            case Integer.parse(kb_str) do
              {kb, _} -> kb * 1024
              _ -> nil
            end

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  # Linux: parse VmRSS from /proc/self/status (in kB)
  defp read_proc_self_rss do
    case File.read("/proc/self/status") do
      {:ok, content} ->
        case Regex.run(~r/VmRSS:\s+(\d+)\s+kB/, content) do
          [_, kb_str] ->
            case Integer.parse(kb_str) do
              {kb, _} -> kb * 1024
              _ -> nil
            end

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  defp read_ps_rss do
    pid = :os.getpid() |> List.to_string()

    case System.cmd("ps", ["-o", "rss=", "-p", pid], stderr_to_stdout: true) do
      {output, 0} -> parse_ps_rss_kb(output)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp erlang_total_memory do
    try do
      :erlang.memory(:total)
    rescue
      _ -> nil
    catch
      _, _ -> nil
    end
  end
end
