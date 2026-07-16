defmodule Ferricstore.Store.HintMetadataTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.HintMetadata

  test "validity checks reject compressed or trailing metadata terms" do
    dir = Path.join(System.tmp_dir!(), "hint_metadata_#{System.unique_integer([:positive])}")
    log_path = Path.join(dir, "00000.log")
    hint_path = Path.join(dir, "00000.hint")
    File.mkdir_p!(dir)
    File.write!(log_path, "log")
    File.write!(hint_path, "hint")
    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, source} = HintMetadata.source_snapshot(log_path)
    hint = snapshot(hint_path)
    term = {:ferricstore_hint_metadata, 1, 0, source, hint}

    compressed =
      term
      |> put_elem(3, Map.put(source, :padding, String.duplicate("x", 4_096)))
      |> :erlang.term_to_binary(compressed: 9)

    assert <<131, 80, _::binary>> = compressed

    for payload <- [compressed, :erlang.term_to_binary(term) <> <<0>>] do
      metadata =
        <<"FSHM", byte_size(payload)::unsigned-big-32, payload::binary,
          :erlang.crc32(payload)::unsigned-big-32>>

      File.write!(HintMetadata.metadata_path(hint_path), metadata)
      refute HintMetadata.valid_for_log?(log_path, hint_path, 0)
    end
  end

  test "validity checks do not follow metadata symlinks" do
    dir = Path.join(System.tmp_dir!(), "hint_metadata_link_#{System.unique_integer([:positive])}")
    log_path = Path.join(dir, "00000.log")
    hint_path = Path.join(dir, "00000.hint")
    metadata_path = HintMetadata.metadata_path(hint_path)
    linked_target = Path.join(dir, "linked.meta")
    File.mkdir_p!(dir)
    File.write!(log_path, "log")
    File.write!(hint_path, "hint")
    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, source} = HintMetadata.source_snapshot(log_path)
    assert :ok = HintMetadata.prepare_publish(hint_path, dir)
    assert :ok = HintMetadata.publish(log_path, hint_path, 0, source, dir)
    assert HintMetadata.valid_for_log?(log_path, hint_path, 0)

    File.rename!(metadata_path, linked_target)
    File.ln_s!(linked_target, metadata_path)

    refute HintMetadata.valid_for_log?(log_path, hint_path, 0)
  end

  test "source snapshots reject log symlinks" do
    dir = Path.join(System.tmp_dir!(), "hint_source_link_#{System.unique_integer([:positive])}")
    target = Path.join(dir, "target.log")
    log_path = Path.join(dir, "00000.log")
    File.mkdir_p!(dir)
    File.write!(target, "log")
    File.ln_s!(target, log_path)
    on_exit(fn -> File.rm_rf(dir) end)

    assert {:error, :source_generation_unavailable} = HintMetadata.source_snapshot(log_path)
  end

  test "metadata publication cannot overwrite a symlink inserted after prepare" do
    dir = Path.join(System.tmp_dir!(), "hint_publish_link_#{System.unique_integer([:positive])}")
    log_path = Path.join(dir, "00000.log")
    hint_path = Path.join(dir, "00000.hint")
    victim = Path.join(dir, "victim")
    temp_path = HintMetadata.metadata_path(hint_path) <> ".tmp"
    File.mkdir_p!(dir)
    File.write!(log_path, "log")
    File.write!(hint_path, "hint")
    File.write!(victim, "protected")
    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, source} = HintMetadata.source_snapshot(log_path)
    assert :ok = HintMetadata.prepare_publish(hint_path, dir)
    File.ln_s!(victim, temp_path)

    assert :ok = HintMetadata.publish(log_path, hint_path, 0, source, dir)
    assert File.read!(victim) == "protected"
    assert HintMetadata.valid_for_log?(log_path, hint_path, 0)
    refute File.lstat!(HintMetadata.metadata_path(hint_path)).type == :symlink
  end

  test "covered source size remains the exact published snapshot after later appends" do
    dir = Path.join(System.tmp_dir!(), "hint_covered_size_#{System.unique_integer([:positive])}")
    log_path = Path.join(dir, "00000.log")
    hint_path = Path.join(dir, "00000.hint")
    File.mkdir_p!(dir)
    File.write!(log_path, "source-at-publication")
    File.write!(hint_path, "hint")
    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, source} = HintMetadata.source_snapshot(log_path)
    assert :ok = HintMetadata.prepare_publish(hint_path, dir)
    assert :ok = HintMetadata.publish(log_path, hint_path, 0, source, dir)

    File.write!(log_path, "late-append", [:append])

    assert {:ok, source.size} == HintMetadata.covered_source_size(log_path, hint_path, 0)
    assert File.stat!(log_path).size > source.size
  end

  defp snapshot(path) do
    stat = File.stat!(path, time: :posix)

    %{
      generation: {stat.major_device, stat.minor_device, stat.inode},
      size: stat.size,
      mtime: stat.mtime,
      ctime: stat.ctime
    }
  end
end
