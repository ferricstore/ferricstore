defmodule Ferricstore.FS do
  @moduledoc """
  Thin Elixir wrapper over `Ferricstore.Bitcask.NIF.fs_*` that:

    * Preserves bang-style raise semantics (`touch!`, `mkdir_p!`, `rm!`,
      `rm_rf!`) matching `File.*` call sites we're replacing.
    * Keeps the non-bang variants returning `:ok | {:error, {kind, msg}}`
      so callers using `with` / `case` can pattern-match on the stable
      atom kinds.

  All ops run on BEAM's Normal scheduler via a Rust NIF — NOT on the
  `:prim_file` async-thread pool. This keeps dirty-scheduler accounting
  out of our crash dumps and makes scheduler-utilization observable.

  For potentially long operations (`rm_rf` of a large tree), the caller
  owns the waiting: `fs_rm_rf_async` returns immediately and delivers a
  `:tokio_complete` message. The `rm_rf!/1` helper here blocks the
  calling process until the message arrives — keeping the call-site
  shape compatible with `File.rm_rf!/1`.
  """

  alias Ferricstore.Bitcask.{Async, NIF}

  @type err_kind ::
          :not_found
          | :already_exists
          | :permission_denied
          | :not_a_directory
          | :is_a_directory
          | :directory_not_empty
          | :invalid_path
          | :other

  @type fs_error :: {:error, {err_kind(), binary()}}

  # -------------------------------------------------------------------
  # Non-bang: propagate {:error, {kind, msg}}
  # -------------------------------------------------------------------

  @spec touch(binary()) :: :ok | fs_error()
  def touch(path), do: NIF.fs_touch(path)

  @spec mkdir_p(binary()) :: :ok | fs_error()
  def mkdir_p(path), do: NIF.fs_mkdir_p(path)

  @spec rename(binary(), binary()) :: :ok | fs_error()
  def rename(old_path, new_path), do: NIF.fs_rename(old_path, new_path)

  @spec rm(binary()) :: :ok | fs_error()
  def rm(path), do: NIF.fs_rm(path)

  @spec exists?(binary()) :: boolean()
  def exists?(path), do: NIF.fs_exists(path)

  @spec dir?(binary()) :: boolean()
  def dir?(path), do: NIF.fs_is_dir(path)

  @spec ls(binary()) :: {:ok, [binary()]} | fs_error()
  def ls(path), do: NIF.fs_ls(path)

  # -------------------------------------------------------------------
  # Bang variants: raise on error, matching File.*!/n
  # -------------------------------------------------------------------

  @spec touch!(binary()) :: :ok
  def touch!(path) do
    case NIF.fs_touch(path) do
      :ok ->
        :ok

      {:error, reason} ->
        raise File.Error, reason: fmt_reason(reason), action: "touch", path: path
    end
  end

  @spec mkdir_p!(binary()) :: :ok
  def mkdir_p!(path) do
    case NIF.fs_mkdir_p(path) do
      :ok ->
        :ok

      {:error, reason} ->
        raise File.Error, reason: fmt_reason(reason), action: "mkdir_p", path: path
    end
  end

  @spec rename!(binary(), binary()) :: :ok
  def rename!(old_path, new_path) do
    case NIF.fs_rename(old_path, new_path) do
      :ok ->
        :ok

      {:error, reason} ->
        raise File.Error, reason: fmt_reason(reason), action: "rename", path: old_path
    end
  end

  @spec rm!(binary()) :: :ok
  def rm!(path) do
    case NIF.fs_rm(path) do
      :ok -> :ok
      {:error, reason} -> raise File.Error, reason: fmt_reason(reason), action: "rm", path: path
    end
  end

  @spec ls!(binary()) :: [binary()]
  def ls!(path) do
    case NIF.fs_ls(path) do
      {:ok, names} -> names
      {:error, reason} -> raise File.Error, reason: fmt_reason(reason), action: "ls", path: path
    end
  end

  # -------------------------------------------------------------------
  # rm_rf — async NIF + blocking wait
  # -------------------------------------------------------------------

  @rm_rf_timeout_ms 30_000

  @doc """
  Recursive remove, blocking the caller until the async NIF completes.

  Idempotent: removing a non-existent path is `:ok`. Never raises for
  missing paths — matches `File.rm_rf/1` (returns `{:ok, []}` there).
  """
  @spec rm_rf(binary()) :: :ok | fs_error()
  def rm_rf(path) do
    case Async.await(
           fn proxy, corr -> NIF.fs_rm_rf_async(proxy, corr, path) end,
           @rm_rf_timeout_ms
         ) do
      {:ok, :ok} -> :ok
      {:error, :timeout} -> {:error, {:other, "rm_rf timed out"}}
      {:error, _reason} = err -> err
    end
  end

  @spec rm_rf!(binary()) :: :ok
  def rm_rf!(path) do
    case rm_rf(path) do
      :ok ->
        :ok

      {:error, reason} ->
        raise File.Error, reason: fmt_reason(reason), action: "rm_rf", path: path
    end
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  # File.Error expects a POSIX atom or a string for :reason. Map our
  # {kind, msg} tuple to the closest POSIX atom where possible, falling
  # back to the raw message so the raised error is still actionable.
  defp fmt_reason({:not_found, _}), do: :enoent
  defp fmt_reason({:already_exists, _}), do: :eexist
  defp fmt_reason({:permission_denied, _}), do: :eacces
  defp fmt_reason({:not_a_directory, _}), do: :enotdir
  defp fmt_reason({:is_a_directory, _}), do: :eisdir
  defp fmt_reason({:directory_not_empty, _}), do: :enotempty
  defp fmt_reason({:invalid_path, msg}), do: msg
  defp fmt_reason({:other, msg}), do: msg
  defp fmt_reason(other), do: inspect(other)
end
