defmodule Ferricstore.ProbFileTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ProbFile
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.CompoundKey
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
    assert ProbFile.pending_create_filename?(filename <> ".pending-create")
    assert ProbFile.mutation_filename?(filename <> ".mutation")
    assert ProbFile.pending_mutation_filename?(filename <> ".pending-create.mutation")

    assert ProbFile.staged_filename?("." <> filename <> ".mutation.ferric-sidecar-12-34")

    assert ProbFile.staged_filename?(
             "." <> filename <> ".pending-create.mutation.ferric-sidecar-12-34"
           )

    refute ProbFile.valid_filename?(Base.url_encode64("old-key", padding: false) <> ".cuckoo")
    refute ProbFile.valid_filename?(String.upcase(filename))
    refute ProbFile.valid_filename?(<<255, 0, 1>>)
    refute ProbFile.staged_filename?("." <> filename <> ".tmp")
    refute ProbFile.staged_filename?(<<46, 255, 0, 1>>)
    refute ProbFile.pending_create_filename?(filename <> ".pending-create.tmp")
    refute ProbFile.mutation_filename?(filename <> ".mutation.tmp")
    refute ProbFile.pending_mutation_filename?(filename <> ".pending-create.mutation.tmp")
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

  test "startup reconciliation removes files absent from the exact type catalog" do
    root =
      Path.join(System.tmp_dir!(), "prob_file_reconcile_#{System.unique_integer([:positive])}")

    prob_dir = Path.join(root, "prob")
    File.mkdir_p!(prob_dir)
    on_exit(fn -> File.rm_rf!(root) end)

    keydir = :ets.new(:prob_file_reconcile_keydir, [:set, :public])
    live_key = "live-cms"
    live_filename = ProbFile.filename(live_key, "cms")
    orphan_filename = ProbFile.filename("deleted-cms", "cms")
    pending_filename = ProbFile.filename("uncommitted-topk", "topk") <> ".pending-create"

    :ets.insert(
      keydir,
      {CompoundKey.type_key(live_key), CompoundKey.encode_prob_type(:cms, 7), 0, 0, 0, 0, 10}
    )

    File.write!(Path.join(prob_dir, live_filename), "live")
    File.write!(Path.join(prob_dir, orphan_filename), "orphan")
    File.write!(Path.join(prob_dir, pending_filename), "pending")

    assert :ok = ProbFiles.validate(root, 0, keydir)
    assert File.exists?(Path.join(prob_dir, live_filename))
    refute File.exists?(Path.join(prob_dir, orphan_filename))
    refute File.exists?(Path.join(prob_dir, pending_filename))
  end

  test "startup validation recovers an interrupted probabilistic mutation" do
    root =
      Path.join(System.tmp_dir!(), "prob_file_recovery_#{System.unique_integer([:positive])}")

    prob_dir = Path.join(root, "prob")
    File.mkdir_p!(prob_dir)
    on_exit(fn -> File.rm_rf!(root) end)

    keydir = :ets.new(:prob_file_recovery_keydir, [:set, :public])
    key = "recover-cms"
    path = ProbFile.path(prob_dir, key, "cms")

    :ets.insert(
      keydir,
      {CompoundKey.type_key(key), CompoundKey.encode_prob_type(:cms, 7), 0, 0, 0, 0, 10}
    )

    assert {:ok, :ok} = NIF.cms_file_create(path, 8, 1)
    assert {:ok, [5]} = NIF.cms_file_incrby_at(path, path, [{"item", 5}], 7, 1)
    pending_receipt = path <> ".pending-create.mutation"
    File.rename!(path <> ".mutation", pending_receipt)
    assert File.regular?(pending_receipt)

    {:ok, file} = :file.open(path, [:raw, :binary, :read, :write])

    try do
      :ok = :file.pwrite(file, 24, <<0::64>>)
      :ok = :file.pwrite(file, 32, :binary.copy(<<0>>, 64))
      :ok = :file.pwrite(file, 96, :binary.copy(<<0>>, 16))
      :ok = :file.sync(file)
    after
      :ok = :file.close(file)
    end

    assert {:ok, [0]} = NIF.cms_file_query(path, ["item"])
    assert :ok = ProbFiles.validate(root, 0, keydir)
    assert {:ok, [5]} = NIF.cms_file_query(path, ["item"])
    assert File.regular?(path <> ".mutation")
    refute File.exists?(pending_receipt)
  end

  test "startup validation removes a mutation receipt without its sidecar" do
    root =
      Path.join(
        System.tmp_dir!(),
        "prob_file_orphan_receipt_#{System.unique_integer([:positive])}"
      )

    prob_dir = Path.join(root, "prob")
    File.mkdir_p!(prob_dir)
    on_exit(fn -> File.rm_rf!(root) end)

    receipt = Path.join(prob_dir, ProbFile.filename("gone", "cms") <> ".mutation")
    File.write!(receipt, "orphan")

    assert :ok = ProbFiles.validate(root, 0)
    refute File.exists?(receipt)
  end
end
