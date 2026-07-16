defmodule Ferricstore.ProbFileTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ProbFile
  alias Ferricstore.Store.Shard.Lifecycle.ProbFiles

  test "filenames are deterministic, fixed-size, and type-specific" do
    long_key = String.duplicate("key", 21_845)

    assert ProbFile.filename("abc", "bloom") ==
             "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad.bloom"

    for extension <- ~w(bloom cms cuckoo topk) do
      filename = ProbFile.filename(long_key, extension)
      assert byte_size(filename) == 65 + byte_size(extension)
      assert ProbFile.valid_filename?(filename)
    end

    refute ProbFile.filename("first", "bloom") == ProbFile.filename("second", "bloom")
    refute ProbFile.filename("key", "bloom") == ProbFile.filename("key", "cms")
  end

  test "only exact digest sidecars and their staging files are accepted" do
    filename = ProbFile.filename(<<0, 255>>, "cuckoo")

    assert ProbFile.valid_filename?(filename)
    assert ProbFile.staged_filename?("." <> filename <> ".ferric-sidecar-12-34")

    refute ProbFile.valid_filename?(Base.url_encode64("old-key", padding: false) <> ".cuckoo")
    refute ProbFile.valid_filename?(String.upcase(filename))
    refute ProbFile.valid_filename?(<<255, 0, 1>>)
    refute ProbFile.staged_filename?("." <> filename <> ".tmp")
    refute ProbFile.staged_filename?(<<46, 255, 0, 1>>)
  end

  test "startup validation removes incomplete staged files and rejects old names" do
    root =
      Path.join(System.tmp_dir!(), "prob_file_validation_#{System.unique_integer([:positive])}")

    prob_dir = Path.join(root, "prob")
    File.mkdir_p!(prob_dir)
    on_exit(fn -> File.rm_rf!(root) end)

    filename = ProbFile.filename("key", "bloom")
    staged = "." <> filename <> ".ferric-sidecar-12-34"
    File.write!(Path.join(prob_dir, filename), "complete")
    File.write!(Path.join(prob_dir, staged), "incomplete")

    assert :ok = ProbFiles.validate(root, 0)
    assert File.exists?(Path.join(prob_dir, filename))
    refute File.exists?(Path.join(prob_dir, staged))

    old_filename = Base.url_encode64("old-key", padding: false) <> ".bloom"
    File.write!(Path.join(prob_dir, old_filename), "unsupported")

    assert {:error, {:invalid_prob_filename, ^old_filename}} = ProbFiles.validate(root, 0)
  end

  test "startup validation rejects non-regular sidecars" do
    root =
      Path.join(
        System.tmp_dir!(),
        "prob_file_type_validation_#{System.unique_integer([:positive])}"
      )

    prob_dir = Path.join(root, "prob")
    File.mkdir_p!(prob_dir)
    on_exit(fn -> File.rm_rf!(root) end)

    filename = ProbFile.filename("key", "bloom")
    path = Path.join(prob_dir, filename)

    File.mkdir!(path)

    assert {:error, {:invalid_prob_file_type, ^filename, :directory}} =
             ProbFiles.validate(root, 0)

    File.rmdir!(path)
    File.write!(Path.join(root, "target"), "not-a-sidecar")
    File.ln_s!(Path.join(root, "target"), path)

    assert {:error, {:invalid_prob_file_type, ^filename, :symlink}} =
             ProbFiles.validate(root, 0)
  end
end
