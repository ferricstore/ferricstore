Code.require_file("waraft_spike_test/sections/part_01.exs", __DIR__)
Code.require_file("waraft_spike_test/sections/part_02.exs", __DIR__)

defmodule Ferricstore.Raft.WARaftSpikeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @moduletag :waraft_spike

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-spike-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)

    on_exit(fn ->
      :ferricstore_waraft_spike.stop()
      File.rm_rf!(root)
    end)

    %{root: String.to_charlist(root)}
  end

  use Ferricstore.Raft.WARaftSpikeTest.Sections.Part01
  use Ferricstore.Raft.WARaftSpikeTest.Sections.Part02

defp start_waraft_peers(unique, count) do
    code_paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    for i <- 1..count do
      name = :"waraft_spike_#{unique}_#{i}"
      data_dir = Path.join(System.tmp_dir!(), "ferricstore-waraft-peer-#{unique}-#{i}")
      File.rm_rf!(data_dir)
      File.mkdir_p!(data_dir)

      {:ok, peer, node_name} =
        :peer.start(%{
          name: name,
          args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
          wait_boot: 120_000
        })

      %{name: node_name, peer: peer, data_dir: data_dir}
    end
  end

  defp wait_for_waraft_leader(names, attempts \\ 100)
  defp wait_for_waraft_leader(_names, 0), do: flunk("WARaft leader was not elected")

  defp wait_for_waraft_leader(names, attempts) do
    case Enum.find(names, fn node ->
           case :rpc.call(node, :ferricstore_waraft_spike, :status, []) do
             status when is_list(status) -> Keyword.get(status, :state) == :leader
             _ -> false
           end
         end) do
      nil ->
        Process.sleep(50)
        wait_for_waraft_leader(names, attempts - 1)

      leader ->
        leader
    end
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      eventually(fun, attempts - 1)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
  defp restore_app_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_app_env(app, key, value), do: Application.put_env(app, key, value)

  def handle_segment_log_corrupt(event, measurements, metadata, parent) do
    send(parent, {:segment_log_corrupt, event, measurements, metadata})
  end

  defp logical_trim_floor(root) do
    path = root |> segment_log_dir() |> Path.join("trim_floor.term")

    case File.read(path) do
      {:ok, binary} ->
        case :erlang.binary_to_term(binary, [:safe]) do
          %{version: 1, index: index} when is_integer(index) -> index
          _other -> 0
        end

      {:error, :enoent} ->
        0
    end
  rescue
    _ -> 0
  end

  defp segment_log_dir(root) do
    Path.join(List.to_string(root), "ferricstore_waraft_spike.1/segment_log")
  end

  defp corrupt_first_segment_payload!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    <<len::32, crc::32, payload::binary-size(len), tail::binary>> = File.read!(path)
    <<first, rest::binary>> = payload
    corrupted_payload = <<Bitwise.bxor(first, 1), rest::binary>>
    File.write!(path, <<len::32, crc::32, corrupted_payload::binary, tail::binary>>)
    path
  end

  defp corrupt_first_segment_length!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    <<_len::32, crc::32, rest::binary>> = File.read!(path)
    File.write!(path, <<0xFFFF_FFFF::32, crc::32, rest::binary>>)
    path
  end

  defp overwrite_first_segment_with_duplicate_indexes!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    records = [
      {1, {1, :noop}},
      {2, {1, :noop}},
      {2, {2, :noop}}
    ]

    File.write!(path, Enum.map(records, &encode_segment_record/1))
    clear_segment_offset_registry!()
    path
  end

  defp clear_segment_offset_registry! do
    case :ets.info(:ferricstore_waraft_segment_offset_registry) do
      :undefined -> :ok
      _info -> :ets.delete_all_objects(:ferricstore_waraft_segment_offset_registry)
    end
  end

  defp overwrite_first_segment_with_index_gap!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    records = [
      {1, {1, :noop}},
      {3, {1, :noop}}
    ]

    File.write!(path, Enum.map(records, &encode_segment_record/1))
    path
  end

  defp write_segment_files_through_double_digits!(segment_dir) do
    File.rm_rf!(segment_dir)
    File.mkdir_p!(segment_dir)
    write_segment_config_fixture!(segment_dir, 4096)

    for segment <- 0..10 do
      start_index = max(1, segment * 4096)
      end_index = min(40_960, (segment + 1) * 4096 - 1)

      if start_index <= end_index do
        path = Path.join(segment_dir, "#{segment}.seg")

        File.open!(path, [:write, :binary], fn file ->
          for index <- start_index..end_index do
            IO.binwrite(file, encode_segment_record({index, {1, :noop}}))
          end
        end)
      end
    end
  end

  defp write_segment_config_fixture!(segment_dir, records_per_segment) do
    payload =
      :erlang.term_to_binary(%{
        version: 1,
        records_per_segment: records_per_segment
      })

    File.write!(Path.join(segment_dir, "segment_config.term"), payload)
  end

  defp encode_segment_record(record) do
    payload = :erlang.term_to_binary(record)
    <<byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
  end

  defp unknown_atom_record_payload(atom_name, index)
       when is_binary(atom_name) and byte_size(atom_name) < 256 and index in 0..255 do
    <<131, 104, 2, 97, index, 104, 2, 97, 1, 119, byte_size(atom_name), atom_name::binary>>
  end

  defp unknown_atom_payload(atom_name) when is_binary(atom_name) and byte_size(atom_name) < 256 do
    <<131, 119, byte_size(atom_name), atom_name::binary>>
  end

  defp segment_log_view(status) do
    waraft_log_view(status)
  end

  defp waraft_log_view(status) do
    name = :wa_raft_log.registered_name(:ferricstore_waraft_spike, 1)
    log_module = Keyword.fetch!(status, :log_module)

    log = {:raft_log, name, :ferricstore, :ferricstore_waraft_spike, 1, log_module}

    {:log_view, log, Keyword.fetch!(status, :log_first), Keyword.fetch!(status, :log_last),
     :undefined}
  end

  defp append_entries!(view, []), do: view

  defp append_entries!(view, entries) do
    assert {:ok, new_view} = :wa_raft_log.append(view, entries)
    new_view
  end

  defp advance_to_next_segment_boundary!(view, term, records_per_segment) do
    next_index = log_view_last(view) + 1

    if rem(next_index, records_per_segment) == 0 do
      view
    else
      entry = {term, {make_ref(), :noop}}
      advance_to_next_segment_boundary!(append_entries!(view, [entry]), term, records_per_segment)
    end
  end

  defp advance_to_split_pair_boundary!(view, term, records_per_segment) do
    next_index = log_view_last(view) + 1

    if rem(next_index, records_per_segment) == records_per_segment - 1 do
      view
    else
      entry = {term, {make_ref(), :noop}}
      advance_to_split_pair_boundary!(append_entries!(view, [entry]), term, records_per_segment)
    end
  end

  defp log_view_last({:log_view, _log, _first, last, _config}), do: last
  defp log_view_first({:log_view, _log, first, _last, _config}), do: first

  defp existing_atom?(atom_name) do
    _ = String.to_existing_atom(atom_name)
    true
  rescue
    ArgumentError -> false
  end

  defp telemetry_started? do
    Enum.any?(Application.started_applications(), fn {app, _description, _version} ->
      app == :telemetry
    end)
  end

  defp ensure_distribution! do
    Ferricstore.Test.ShardHelpers.ensure_distribution_started!(:waraft_runner)
  end

  defp rewind_spike_storage!(root, keys_to_delete, position) do
    storage_path =
      Path.join([
        List.to_string(root),
        "ferricstore_waraft_spike.1",
        "storage.ets"
      ])

    {:ok, table} = :ets.file2tab(String.to_charlist(storage_path))

    Enum.each(keys_to_delete, fn key ->
      :ets.delete(table, key)
    end)

    true = :ets.insert(table, {:"$position", position})
    tmp_path = storage_path <> ".tmp"
    :ok = :ets.tab2file(table, String.to_charlist(tmp_path))
    true = :ets.delete(table)
    File.rename!(tmp_path, storage_path)
  end
end

