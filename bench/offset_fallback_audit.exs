defmodule FerricstoreBench.OffsetFallbackAudit do
  @moduledoc false
  @magic 0xF00D2026
  @max_frame_bytes 64 * 1024 * 1024

  def run do
    root = System.fetch_env!("BENCH_AUDIT_ROOT")
    directories = Path.wildcard(Path.join(root, "waraft/*/apply_projection_log/segment_log"))
    true = directories != []
    reports = Enum.map(directories, &audit/1)
    report = %{root: root, diagnostic_only: true, writes_to_fixture: false, directories: reports}
    File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
    IO.inspect(reports, label: "OFFSET_AUDIT", limit: :infinity)
  end

  defp audit(dir) do
    segments = Path.wildcard(Path.join(dir, "*.seg"))

    Enum.map(segments, fn path ->
      {:ok, %{type: :regular, size: file_bytes}} = File.lstat(path, [:raw])

      {:ok, fd} =
        :file.open(String.to_charlist(path), [:read, :raw, :binary, {:read_ahead, 262_144}])

      scan =
        try do
          scan(fd, 0, %{}, 0)
        after
          :file.close(fd)
        end

      {frames, records} = scan
      index_path = Path.rootname(path) <> ".idx"
      {:ok, %{type: :regular}} = File.lstat(index_path, [:raw])
      slots = File.read!(index_path)
      {valid, rejected} = slots(slots, 0, frames, [], [])
      expected = MapSet.new(Map.keys(frames))
      indexed = MapSet.new(Enum.map(valid, & &1.index))

      %{
        path: path,
        file_bytes: file_bytes,
        records: records,
        unique_indexes: map_size(frames),
        valid_slots: length(valid),
        rejected_slots: Enum.take(rejected, 20),
        missing_index_count: MapSet.size(MapSet.difference(expected, indexed)),
        frame_samples:
          Enum.take(Enum.sort(frames), 3)
          |> Enum.map(fn {index, info} ->
            Map.put(info, :index, index)
          end)
      }
    end)
  end

  defp scan(fd, offset, frames, count) do
    case :file.read(fd, 8) do
      :eof ->
        {frames, count}

      {:ok, <<size::unsigned-big-32, crc::unsigned-big-32>>} when size <= @max_frame_bytes ->
        {:ok, payload} = :file.read(fd, size)
        true = byte_size(payload) == size
        ^crc = :erlang.crc32(payload)
        {:ok, index} = peek_index(payload)

        frames =
          Map.update(frames, index, %{offset: offset, size: size + 8, count: 1}, fn old ->
            %{offset: offset, size: size + 8, count: old.count + 1}
          end)

        scan(fd, offset + size + 8, frames, count + 1)

      other ->
        raise("invalid or oversized audit frame: #{inspect(other)}")
    end
  end

  defp slots(<<>>, _slot, _frames, valid, rejected), do: {valid, rejected}

  defp slots(<<record::binary-size(28), rest::binary>>, slot, frames, valid, rejected) do
    case record do
      <<@magic::unsigned-big-32, index::unsigned-big-64, offset::unsigned-big-64,
        size::unsigned-big-32, crc::unsigned-big-32>> ->
        <<body::binary-size(24), _::binary>> = record
        info = %{slot: slot, index: index, offset: offset, size: size}

        reason =
          cond do
            :erlang.crc32(body) != crc -> :bad_slot_crc
            not Map.has_key?(frames, index) -> :index_absent
            frames[index].offset != offset or frames[index].size != size -> :not_latest_frame
            true -> :ok
          end

        if reason == :ok,
          do: slots(rest, slot + 1, frames, [info | valid], rejected),
          else: slots(rest, slot + 1, frames, valid, [Map.put(info, :reason, reason) | rejected])

      <<0::size(224)>> ->
        slots(rest, slot + 1, frames, valid, rejected)

      _invalid ->
        slots(rest, slot + 1, frames, valid, [%{slot: slot, reason: :invalid_slot} | rejected])
    end
  end

  defp slots(_truncated, slot, _frames, valid, rejected),
    do: {valid, [%{slot: slot, reason: :truncated_slot} | rejected]}

  defp peek_index(<<131, 104, 2, 97, index, _::binary>>), do: {:ok, index}

  defp peek_index(<<131, 104, 2, 98, index::signed-big-32, _::binary>>) when index >= 0,
    do: {:ok, index}

  defp peek_index(other),
    do:
      raise(
        "unsupported audit record index: #{inspect(binary_part(other, 0, min(40, byte_size(other))))}"
      )
end

FerricstoreBench.OffsetFallbackAudit.run()
