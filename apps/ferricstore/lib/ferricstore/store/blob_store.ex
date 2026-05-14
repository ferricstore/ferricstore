defmodule Ferricstore.Store.BlobStore do
  @moduledoc """
  Side-channel blob storage for large values.

  New writes append payload records into a shard-local segment log under
  `data_dir/blob/shard_N/segments/`, while Bitcask stores the fixed-size
  `BlobRef`. Older content-addressed v1 refs remain readable so existing data
  can be served during the transition.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobStore.TableOwner

  @hash_chunk_bytes 1_048_576
  @tmp_stale_after_seconds 300
  @segment_id 0
  @default_segment_max_bytes 256 * 1024 * 1024
  @segment_header_magic <<0, ?F, ?S, ?B, ?L, ?O, ?G, 1>>
  @segment_header_bytes 48
  @segment_next_id_filename "next_segment_id"
  @recovery_table :ferricstore_blob_store_recovery
  @segment_table :ferricstore_blob_store_segments
  @lock_table :ferricstore_blob_store_locks
  @dir_table :ferricstore_blob_store_dirs
  @held_locks_key :ferricstore_blob_store_held_locks
  @lock_retry_ms 1

  @type reason :: term()

  @doc false
  @spec init_tables() :: :ok
  def init_tables do
    TableOwner.ensure_tables()
  end

  @doc """
  Stores `payload` in the shard append segment and returns the small ref.
  """
  @spec put(binary(), non_neg_integer(), binary()) :: {:ok, BlobRef.t()} | {:error, reason()}
  def put(data_dir, shard_index, payload)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(payload) do
    batch = prepare_single_payload_batch(payload)

    with_blob_lock(data_dir, shard_index, fn ->
      case do_put_many(data_dir, shard_index, batch) do
        {:ok, [ref]} -> {:ok, ref}
        {:ok, refs} when is_list(refs) -> {:error, {:unexpected_blob_ref_count, length(refs)}}
        {:error, _reason} = error -> error
      end
    end)
  end

  defp prepare_single_payload_batch(payload) do
    size = byte_size(payload)
    checksum = :crypto.hash(:sha256, payload)
    entry = %{payload: payload, checksum: checksum, size: size}

    %{
      unique_entries: [entry],
      value_indexes: [0],
      batch_bytes: @segment_header_bytes + size,
      error_ref: %BlobRef{checksum: checksum, size: size}
    }
  end

  @doc """
  Stores payloads in one append batch and fsyncs the segment once.
  """
  @spec put_many(binary(), non_neg_integer(), [binary()]) ::
          {:ok, [BlobRef.t()]} | {:error, reason()}
  def put_many(data_dir, shard_index, payloads)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(payloads) do
    case prepare_payload_batch(payloads) do
      {:ok, %{batch_bytes: 0}} ->
        {:ok, []}

      {:ok, batch} ->
        with_blob_lock(data_dir, shard_index, fn ->
          do_put_many(data_dir, shard_index, batch)
        end)

      {:error, :invalid_blob_payload} = error ->
        error
    end
  end

  @doc "Reads and validates a blob by ref."
  @spec get(binary(), non_neg_integer(), BlobRef.t()) :: {:ok, binary()} | {:error, reason()}
  def get(data_dir, shard_index, %BlobRef{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    if BlobRef.valid?(ref), do: do_get(data_dir, shard_index, ref), else: invalid_blob_ref()
  end

  defp do_get(data_dir, shard_index, %BlobRef{version: 1} = ref) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with {:ok, payload} <- File.read(path),
           :ok <- verify_size(ref, payload),
           :ok <- verify_checksum(ref, payload) do
        {:ok, payload}
      end

    case result do
      {:ok, _payload} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:get, shard_index, path, ref, reason)
        error
    end
  end

  defp do_get(data_dir, shard_index, %BlobRef{version: 2, size: size, offset: offset} = ref) do
    path = BlobRef.path(data_dir, shard_index, ref)
    result = read_segment_payload(path, offset, size, ref)

    case result do
      {:ok, _payload} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:get, shard_index, path, ref, reason)
        error
    end
  end

  @doc """
  Reads and validates refs in order.

  Segment refs are grouped by append segment so batch reads open each segment
  once while still returning per-ref errors. Duplicate refs are loaded once and
  fanned back out to their original positions.
  """
  @spec get_many(binary(), non_neg_integer(), [BlobRef.t()]) ::
          [{:ok, binary()} | {:error, reason()}]
  def get_many(data_dir, shard_index, refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(refs) do
    {prepared, unique_refs} = prepare_get_many_refs(refs)
    loaded_refs = load_blob_refs(data_dir, shard_index, unique_refs)

    Enum.map(prepared, fn
      {:ref, ref} -> Map.fetch!(loaded_refs, ref)
      :invalid -> {:error, :invalid_blob_ref}
    end)
  end

  @doc """
  Verifies that an existing blob exactly matches its ref.

  This is intended for write/apply correctness boundaries where a ref-only
  command would otherwise acknowledge a pointer without proving the pointed
  bytes are intact. It hashes the file in chunks and does not materialize the
  full payload as a BEAM binary.
  """
  @spec verify(binary(), non_neg_integer(), BlobRef.t()) :: :ok | {:error, reason()}
  def verify(data_dir, shard_index, %BlobRef{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    if BlobRef.valid?(ref) do
      case Process.get(:ferricstore_blob_store_verify_hook) do
        fun when is_function(fun, 3) -> normalize_verify(fun.(data_dir, shard_index, ref))
        _other -> do_verify(data_dir, shard_index, ref)
      end
    else
      invalid_blob_ref()
    end
  end

  defp do_verify(data_dir, shard_index, %BlobRef{version: 1, size: size} = ref) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- stat_regular_size(path, size),
           :ok <- file_matches_ref?(path, ref) do
        :ok
      end

    case result do
      :ok ->
        :ok

      :mismatch ->
        error = {:error, :checksum_mismatch}
        emit_error(:verify, shard_index, path, ref, :checksum_mismatch)
        error

      {:error, reason} = error ->
        emit_error(:verify, shard_index, path, ref, reason)
        error
    end
  end

  defp do_verify(data_dir, shard_index, %BlobRef{version: 2, size: size, offset: offset} = ref) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- stat_regular_min_size(path, offset + size),
           :ok <- verify_segment_record(path, offset, size, ref) do
        :ok
      end

    case result do
      :ok ->
        :ok

      :mismatch ->
        error = {:error, :checksum_mismatch}
        emit_error(:verify, shard_index, path, ref, :checksum_mismatch)
        error

      {:error, reason} = error ->
        emit_error(:verify, shard_index, path, ref, reason)
        error
    end
  end

  defp normalize_verify(:ok), do: :ok
  defp normalize_verify({:error, _reason} = error), do: error
  defp normalize_verify(other), do: {:error, other}

  defp invalid_blob_ref, do: {:error, :invalid_blob_ref}

  @doc """
  Verifies a batch of refs, validating duplicate refs once.

  This keeps apply-time blob ref checks fully checksummed while avoiding
  repeated disk reads when a batch intentionally fans out one payload to many
  keys.
  """
  @spec verify_many(binary(), non_neg_integer(), [BlobRef.t()]) :: :ok | {:error, reason()}
  @spec verify_many(
          binary(),
          non_neg_integer(),
          [BlobRef.t()],
          (binary(), non_neg_integer(), BlobRef.t() -> :ok | {:error, reason()})
        ) :: :ok | {:error, reason()}
  def verify_many(data_dir, shard_index, refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(refs) do
    case Process.get(:ferricstore_blob_store_verify_hook) do
      fun when is_function(fun, 3) ->
        verify_many(data_dir, shard_index, refs, &verify/3)

      _other ->
        do_verify_many(data_dir, shard_index, refs)
    end
  end

  def verify_many(data_dir, shard_index, refs, verifier)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(refs) and is_function(verifier, 3) do
    refs
    |> Enum.reduce_while({:ok, MapSet.new()}, fn
      %BlobRef{} = ref, {:ok, seen} ->
        cond do
          not BlobRef.valid?(ref) ->
            {:halt, {:error, :invalid_blob_ref}}

          MapSet.member?(seen, ref) ->
            {:cont, {:ok, seen}}

          true ->
            case verifier.(data_dir, shard_index, ref) do
              :ok -> {:cont, {:ok, MapSet.put(seen, ref)}}
              {:error, reason} -> {:halt, {:error, reason}}
              other -> {:halt, {:error, other}}
            end
        end

      _invalid, {:ok, _seen} ->
        {:halt, {:error, :invalid_blob_ref}}
    end)
    |> case do
      {:ok, _seen} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp do_verify_many(data_dir, shard_index, refs) do
    with {:ok, unique_refs} <- unique_blob_refs(refs),
         :ok <- verify_legacy_refs(data_dir, shard_index, unique_refs) do
      verify_segment_refs(data_dir, shard_index, unique_refs)
    end
  end

  defp unique_blob_refs(refs) do
    refs
    |> Enum.reduce_while({:ok, [], MapSet.new()}, fn
      %BlobRef{} = ref, {:ok, acc, seen} ->
        cond do
          not BlobRef.valid?(ref) ->
            {:halt, {:error, :invalid_blob_ref}}

          MapSet.member?(seen, ref) ->
            {:cont, {:ok, acc, seen}}

          true ->
            {:cont, {:ok, [ref | acc], MapSet.put(seen, ref)}}
        end

      _invalid, {:ok, _acc, _seen} ->
        {:halt, {:error, :invalid_blob_ref}}
    end)
    |> case do
      {:ok, acc, _seen} -> {:ok, Enum.reverse(acc)}
      {:error, _reason} = error -> error
    end
  end

  defp verify_legacy_refs(data_dir, shard_index, refs) do
    Enum.reduce_while(refs, :ok, fn
      %BlobRef{version: 1} = ref, :ok ->
        case do_verify(data_dir, shard_index, ref) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      %BlobRef{version: 2}, :ok ->
        {:cont, :ok}

      %BlobRef{}, :ok ->
        {:halt, {:error, :invalid_blob_ref}}
    end)
  end

  defp verify_segment_refs(data_dir, shard_index, refs) do
    refs
    |> Enum.filter(&match?(%BlobRef{version: 2}, &1))
    |> Enum.group_by(&BlobRef.path(data_dir, shard_index, &1))
    |> Enum.reduce_while(:ok, fn {path, path_refs}, :ok ->
      case verify_segment_refs_at_path(path, shard_index, path_refs) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp verify_segment_refs_at_path(path, shard_index, [first_ref | _] = refs) do
    max_extent =
      Enum.reduce(refs, 0, fn %BlobRef{offset: offset, size: size}, acc ->
        max(acc, offset + size)
      end)

    with :ok <- stat_regular_min_size(path, max_extent),
         {:ok, io} <- open_read_file(path) do
      try do
        verify_open_segment_refs(io, path, shard_index, refs)
      after
        :file.close(io)
      end
    else
      {:error, reason} = error ->
        emit_error(:verify, shard_index, path, first_ref, reason)
        error
    end
  end

  defp verify_open_segment_refs(io, path, shard_index, refs) do
    Enum.reduce_while(refs, :ok, fn %BlobRef{offset: offset, size: size} = ref, :ok ->
      result =
        with :ok <- validate_open_segment_record(io, offset, size, ref),
             :ok <- open_file_range_matches_ref?(io, offset, size, ref) do
          :ok
        end

      case result do
        :ok ->
          {:cont, :ok}

        :mismatch ->
          error = {:error, :checksum_mismatch}
          emit_error(:verify, shard_index, path, ref, :checksum_mismatch)
          {:halt, error}

        {:error, reason} = error ->
          emit_error(:verify, shard_index, path, ref, reason)
          {:halt, error}
      end
    end)
  end

  defp prepare_get_many_refs(refs) do
    {prepared, unique_refs, _seen} =
      Enum.reduce(refs, {[], [], MapSet.new()}, fn
        %BlobRef{} = ref, {prepared, unique_refs, seen} ->
          cond do
            not BlobRef.valid?(ref) ->
              {[:invalid | prepared], unique_refs, seen}

            MapSet.member?(seen, ref) ->
              {[{:ref, ref} | prepared], unique_refs, seen}

            true ->
              {[{:ref, ref} | prepared], [ref | unique_refs], MapSet.put(seen, ref)}
          end

        _invalid, {prepared, unique_refs, seen} ->
          {[:invalid | prepared], unique_refs, seen}
      end)

    {Enum.reverse(prepared), Enum.reverse(unique_refs)}
  end

  defp load_blob_refs(data_dir, shard_index, refs) do
    {legacy_refs, segment_refs, invalid_refs} =
      Enum.reduce(refs, {[], [], []}, fn
        %BlobRef{version: 1} = ref, {legacy_refs, segment_refs, invalid_refs} ->
          {[ref | legacy_refs], segment_refs, invalid_refs}

        %BlobRef{version: 2} = ref, {legacy_refs, segment_refs, invalid_refs} ->
          {legacy_refs, [ref | segment_refs], invalid_refs}

        %BlobRef{} = ref, {legacy_refs, segment_refs, invalid_refs} ->
          {legacy_refs, segment_refs, [ref | invalid_refs]}
      end)

    %{}
    |> put_invalid_ref_results(invalid_refs)
    |> put_legacy_ref_results(data_dir, shard_index, Enum.reverse(legacy_refs))
    |> put_segment_ref_results(data_dir, shard_index, Enum.reverse(segment_refs))
  end

  defp put_invalid_ref_results(results, refs) do
    Enum.reduce(refs, results, fn ref, acc ->
      Map.put(acc, ref, {:error, :invalid_blob_ref})
    end)
  end

  defp put_legacy_ref_results(results, data_dir, shard_index, refs) do
    Enum.reduce(refs, results, fn ref, acc ->
      Map.put(acc, ref, get(data_dir, shard_index, ref))
    end)
  end

  defp put_segment_ref_results(results, _data_dir, _shard_index, []), do: results

  defp put_segment_ref_results(results, data_dir, shard_index, refs) do
    refs
    |> Enum.group_by(&BlobRef.path(data_dir, shard_index, &1))
    |> Enum.reduce(results, fn {path, path_refs}, acc ->
      Map.merge(acc, get_segment_refs_at_path(path, shard_index, path_refs))
    end)
  end

  defp get_segment_refs_at_path(path, shard_index, [_first_ref | _] = refs) do
    max_extent =
      Enum.reduce(refs, 0, fn %BlobRef{offset: offset, size: size}, acc ->
        max(acc, offset + size)
      end)

    with :ok <- stat_regular_min_size(path, max_extent),
         {:ok, io} <- open_read_file(path) do
      try do
        get_open_segment_refs(io, path, shard_index, refs)
      after
        :file.close(io)
      end
    else
      {:error, reason} ->
        Enum.reduce(refs, %{}, fn ref, acc ->
          emit_error(:get, shard_index, path, ref, reason)
          Map.put(acc, ref, {:error, reason})
        end)
    end
  end

  defp get_open_segment_refs(io, path, shard_index, refs) do
    Enum.reduce(refs, %{}, fn ref, acc ->
      Map.put(acc, ref, get_open_segment_ref(io, path, shard_index, ref))
    end)
  end

  defp get_open_segment_ref(io, path, shard_index, %BlobRef{offset: offset, size: size} = ref) do
    result =
      with :ok <- validate_open_segment_record(io, offset, size, ref),
           {:ok, payload} <- pread_exact_open(io, offset, size),
           :ok <- verify_checksum(ref, payload) do
        {:ok, payload}
      end

    case result do
      {:ok, _payload} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:get, shard_index, path, ref, reason)
        error
    end
  end

  defp load_blob_file_refs(data_dir, shard_index, refs) do
    {legacy_refs, segment_refs, invalid_refs} =
      Enum.reduce(refs, {[], [], []}, fn
        %BlobRef{version: 1} = ref, {legacy_refs, segment_refs, invalid_refs} ->
          {[ref | legacy_refs], segment_refs, invalid_refs}

        %BlobRef{version: 2} = ref, {legacy_refs, segment_refs, invalid_refs} ->
          {legacy_refs, [ref | segment_refs], invalid_refs}

        %BlobRef{} = ref, {legacy_refs, segment_refs, invalid_refs} ->
          {legacy_refs, segment_refs, [ref | invalid_refs]}
      end)

    %{}
    |> put_invalid_file_ref_results(invalid_refs)
    |> put_legacy_file_ref_results(data_dir, shard_index, Enum.reverse(legacy_refs))
    |> put_segment_file_ref_results(data_dir, shard_index, Enum.reverse(segment_refs))
  end

  defp put_invalid_file_ref_results(results, refs) do
    Enum.reduce(refs, results, fn ref, acc ->
      Map.put(acc, ref, {:error, :invalid_blob_ref})
    end)
  end

  defp put_legacy_file_ref_results(results, data_dir, shard_index, refs) do
    Enum.reduce(refs, results, fn ref, acc ->
      Map.put(acc, ref, file_ref(data_dir, shard_index, ref))
    end)
  end

  defp put_segment_file_ref_results(results, _data_dir, _shard_index, []), do: results

  defp put_segment_file_ref_results(results, data_dir, shard_index, refs) do
    refs
    |> Enum.group_by(&BlobRef.path(data_dir, shard_index, &1))
    |> Enum.reduce(results, fn {path, path_refs}, acc ->
      Map.merge(acc, get_segment_file_refs_at_path(path, shard_index, path_refs))
    end)
  end

  defp get_segment_file_refs_at_path(path, shard_index, refs) do
    max_extent =
      Enum.reduce(refs, 0, fn %BlobRef{offset: offset, size: size}, acc ->
        max(acc, offset + size)
      end)

    with :ok <- stat_regular_min_size(path, max_extent),
         {:ok, io} <- open_read_file(path) do
      try do
        get_open_segment_file_refs(io, path, shard_index, refs)
      after
        :file.close(io)
      end
    else
      {:error, reason} ->
        Enum.reduce(refs, %{}, fn ref, acc ->
          emit_error(:file_ref, shard_index, path, ref, reason)
          Map.put(acc, ref, {:error, reason})
        end)
    end
  end

  defp get_open_segment_file_refs(io, path, shard_index, refs) do
    Enum.reduce(refs, %{}, fn %BlobRef{offset: offset, size: size} = ref, acc ->
      result =
        case validate_open_segment_record(io, offset, size, ref) do
          :ok -> {:ok, {path, offset, size}}
          {:error, _reason} = error -> error
        end

      case result do
        {:ok, _file_ref} = ok ->
          Map.put(acc, ref, ok)

        {:error, reason} = error ->
          emit_error(:file_ref, shard_index, path, ref, reason)
          Map.put(acc, ref, error)
      end
    end)
  end

  @doc """
  Returns a file ref for a blob after validating the file is regular and has
  the expected size.

  This is the hot streaming path. It intentionally does not hash the blob on
  every read; `get/3` and `verify/3` still verify materialized reads. Full
  checksum validation belongs in write-time validation and background scrub,
  not in every sendfile/file-stream GET.
  """
  @spec file_ref(binary(), non_neg_integer(), BlobRef.t()) ::
          {:ok, {binary(), non_neg_integer(), non_neg_integer()}} | {:error, reason()}
  def file_ref(data_dir, shard_index, %BlobRef{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    if BlobRef.valid?(ref),
      do: do_file_ref(data_dir, shard_index, ref),
      else: invalid_blob_ref()
  end

  defp do_file_ref(data_dir, shard_index, %BlobRef{version: 1, size: size} = ref) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- stat_regular_size(path, size) do
        {:ok, {path, 0, size}}
      end

    case result do
      {:ok, _file_ref} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:file_ref, shard_index, path, ref, reason)
        error
    end
  end

  defp do_file_ref(data_dir, shard_index, %BlobRef{version: 2, size: size, offset: offset} = ref) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- stat_regular_min_size(path, offset + size),
           :ok <- validate_segment_record_header(path, offset, size, ref) do
        {:ok, {path, offset, size}}
      end

    case result do
      {:ok, _file_ref} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:file_ref, shard_index, path, ref, reason)
        error
    end
  end

  @doc """
  Returns file refs in input order while validating append-segment headers in
  batches.

  This is the streaming read hot path for MGET/pipelined GET. Segment refs are
  grouped by path so a batch that points at one blob segment opens it once, but
  corruption and missing-file results stay isolated per requested ref.
  """
  @spec file_refs_many(binary(), non_neg_integer(), [BlobRef.t()]) ::
          [{:ok, {binary(), non_neg_integer(), non_neg_integer()}} | {:error, reason()}]
  def file_refs_many(data_dir, shard_index, refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(refs) do
    {prepared, unique_refs} = prepare_get_many_refs(refs)
    loaded_refs = load_blob_file_refs(data_dir, shard_index, unique_refs)

    Enum.map(prepared, fn
      {:ref, ref} -> Map.fetch!(loaded_refs, ref)
      :invalid -> {:error, :invalid_blob_ref}
    end)
  end

  @doc """
  Reads a byte range from a blob ref without materializing the full payload.

  Segment refs still validate their record header and full declared extent
  before the range pread. This keeps range reads aligned with the sendfile path:
  cheap pointer validation, no full-payload hash on every read.
  """
  @spec get_range(binary(), non_neg_integer(), BlobRef.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, reason()}
  def get_range(
        data_dir,
        shard_index,
        %BlobRef{} = ref,
        relative_offset,
        count
      )
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(relative_offset) and relative_offset >= 0 and is_integer(count) and
             count >= 0 do
    if BlobRef.valid?(ref) do
      do_get_range(data_dir, shard_index, ref, relative_offset, count)
    else
      invalid_blob_ref()
    end
  end

  defp do_get_range(_data_dir, _shard_index, %BlobRef{}, _relative_offset, 0), do: {:ok, ""}

  defp do_get_range(
         data_dir,
         shard_index,
         %BlobRef{version: 1, size: size} = ref,
         relative_offset,
         count
       ) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- validate_blob_range(size, relative_offset, count),
           :ok <- stat_regular_size(path, size),
           {:ok, payload} <- read_file_range(path, relative_offset, count) do
        {:ok, payload}
      end

    case result do
      {:ok, _payload} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:get_range, shard_index, path, ref, reason)
        error
    end
  end

  defp do_get_range(
         data_dir,
         shard_index,
         %BlobRef{version: 2, size: size, offset: offset} = ref,
         relative_offset,
         count
       ) do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- validate_blob_range(size, relative_offset, count),
           :ok <- stat_regular_min_size(path, offset + size),
           {:ok, payload} <-
             read_segment_payload_range(path, offset, size, ref, relative_offset, count) do
        {:ok, payload}
      end

    case result do
      {:ok, _payload} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:get_range, shard_index, path, ref, reason)
        error
    end
  end

  @doc """
  Recovers append-segment files by truncating the first partial or corrupt tail.

  This is called lazily before the first append in a VM and is also public for
  startup/lifecycle tests. Older valid records before the bad tail remain
  readable.
  """
  @spec recover_shard(binary(), non_neg_integer()) ::
          {:ok,
           %{
             segments: non_neg_integer(),
             truncated_segments: non_neg_integer(),
             truncated_bytes: non_neg_integer()
           }}
          | {:error, term()}
  def recover_shard(data_dir, shard_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    clear_active_segment_cache(data_dir, shard_index)
    clear_segment_dir_cache(data_dir, shard_index)
    shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)

    with {:ok, paths} <- segment_files(shard_path) do
      latest_path = List.last(paths)

      result =
        Enum.reduce_while(
          paths,
          {:ok, %{segments: 0, truncated_segments: 0, truncated_bytes: 0}},
          fn path, {:ok, acc} ->
            case recover_segment(path, path == latest_path) do
              {:ok, bytes} ->
                acc = %{
                  acc
                  | segments: acc.segments + 1,
                    truncated_segments: acc.truncated_segments + if(bytes > 0, do: 1, else: 0),
                    truncated_bytes: acc.truncated_bytes + bytes
                }

                {:cont, {:ok, acc}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end
        )

      case result do
        {:ok, stats} ->
          mark_recovered(data_dir, shard_index)
          {:ok, stats}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc """
  Deletes blob files that are not present in `live_refs`.

  The caller owns producing a complete live set. This function is deliberately
  conservative for append segments: a segment is kept while any live v2 ref
  points into it, and reclaimed only when the whole segment is dead. The shard
  must guard Ra replay safety before calling this, because unreleased Ra log
  entries can still contain older blob refs.
  """
  @spec sweep_unreferenced(binary(), non_neg_integer(), Enumerable.t()) ::
          {:ok,
           %{
             deleted_files: non_neg_integer(),
             deleted_bytes: non_neg_integer(),
             kept_files: non_neg_integer()
           }}
          | {:error, term()}
  def sweep_unreferenced(data_dir, shard_index, live_refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    with_blob_lock(data_dir, shard_index, fn ->
      do_sweep_unreferenced(data_dir, shard_index, live_refs)
    end)
  end

  @doc """
  Returns current blob side-channel storage usage for metrics and diagnostics.

  This scans the blob directory, so callers should use it for observability
  paths only, not per-command hot paths.
  """
  @spec storage_stats(binary()) ::
          {:ok,
           %{
             files: non_neg_integer(),
             bytes: non_neg_integer(),
             legacy_files: non_neg_integer(),
             legacy_bytes: non_neg_integer(),
             segment_files: non_neg_integer(),
             segment_bytes: non_neg_integer(),
             tmp_files: non_neg_integer(),
             tmp_bytes: non_neg_integer()
           }}
          | {:error, term()}
  def storage_stats(data_dir) when is_binary(data_dir) do
    blob_glob = Path.join([data_dir, "blob", "shard_*", "**", "*.blob"])
    segment_glob = Path.join([data_dir, "blob", "shard_*", "segments", "*.bloblog"])
    tmp_glob = Path.join([data_dir, "blob", "shard_*", "**", "*.tmp"])

    with {:ok, blob_stats} <-
           storage_stats_for_paths({Path.wildcard(blob_glob), Path.wildcard(segment_glob)}),
         {:ok, tmp_stats} <- storage_stats_for_paths(Path.wildcard(tmp_glob, match_dot: true)) do
      {:ok,
       %{
         files: blob_stats.files,
         bytes: blob_stats.bytes,
         legacy_files: blob_stats.legacy_files,
         legacy_bytes: blob_stats.legacy_bytes,
         segment_files: blob_stats.segment_files,
         segment_bytes: blob_stats.segment_bytes,
         tmp_files: tmp_stats.files,
         tmp_bytes: tmp_stats.bytes
       }}
    end
  rescue
    error -> {:error, {:blob_storage_stats_failed, error}}
  end

  defp storage_stats_for_paths({legacy_paths, segment_paths}) do
    with {:ok, legacy_stats} <- storage_stats_for_paths(legacy_paths),
         {:ok, segment_stats} <- storage_stats_for_paths(segment_paths) do
      {:ok,
       %{
         files: legacy_stats.files + segment_stats.files,
         bytes: legacy_stats.bytes + segment_stats.bytes,
         legacy_files: legacy_stats.files,
         legacy_bytes: legacy_stats.bytes,
         segment_files: segment_stats.files,
         segment_bytes: segment_stats.bytes
       }}
    end
  end

  defp storage_stats_for_paths(paths) when is_list(paths) do
    Enum.reduce_while(paths, {:ok, %{files: 0, bytes: 0}}, fn path, {:ok, acc} ->
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} ->
          {:cont, {:ok, %{files: acc.files + 1, bytes: acc.bytes + size}}}

        {:ok, %{type: type}} ->
          {:halt, {:error, {:blob_storage_stats_invalid_file, path, type}}}

        {:error, reason} ->
          {:halt, {:error, {:blob_storage_stats_stat_failed, path, reason}}}
      end
    end)
  end

  defp do_put_many(data_dir, shard_index, batch) do
    fallback_path = segment_path(data_dir, shard_index, @segment_id)

    result =
      case do_put_many_once(data_dir, shard_index, batch) do
        {:error, :blob_segment_dir_missing} ->
          clear_active_segment_cache(data_dir, shard_index)
          clear_segment_dir_cache(data_dir, shard_index)
          do_put_many_once(data_dir, shard_index, batch)

        other ->
          other
      end

    case result do
      {:ok, refs} ->
        {:ok, refs}

      {:error, reason} = error ->
        emit_error(:put, shard_index, fallback_path, batch.error_ref, reason)
        recover_shard(data_dir, shard_index)
        error
    end
  end

  defp do_put_many_once(data_dir, shard_index, batch) do
    with {:ok, _stats} <- ensure_recovered(data_dir, shard_index),
         :ok <- ensure_segment_dir(data_dir, shard_index),
         {:ok, segment} <- writable_segment(data_dir, shard_index, batch.batch_bytes) do
      case File.open(segment.path, [:append, :raw, :binary]) do
        {:ok, io} ->
          try do
            case build_segment_records(batch, segment.id, segment.start_offset) do
              {:ok, refs, iodata, next_offset} ->
                case append_and_sync_segment(io, segment, shard_index, iodata) do
                  :ok ->
                    cache_active_segment(
                      data_dir,
                      shard_index,
                      segment.id,
                      segment.path,
                      next_offset
                    )

                    {:ok, refs}

                  {:error, _reason} = error ->
                    error
                end

              {:error, _reason} = error ->
                error
            end
          after
            :file.close(io)
          end

        {:error, :enoent} ->
          {:error, :blob_segment_dir_missing}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp append_and_sync_segment(io, segment, shard_index, iodata) do
    with :ok <- write_file(io, iodata),
         :ok <- fsync_file(segment.path),
         :ok <- maybe_fsync_new_segment_dir(Path.dirname(segment.path), segment.file_existed?) do
      :ok
    else
      {:error, _reason} = error ->
        rollback_segment_append(io, segment.path, shard_index, segment.start_offset)
        error
    end
  end

  defp rollback_segment_append(io, path, shard_index, start_offset) do
    with {:ok, _} <- :file.position(io, start_offset),
         :ok <- :file.truncate(io) do
      :ok
    else
      {:error, reason} ->
        emit_error(:rollback_append, shard_index, path, empty_blob_error_ref(), reason)
        {:error, reason}
    end
  end

  defp build_segment_records(batch, segment_id, start_offset) do
    Enum.reduce_while(batch.unique_entries, {:ok, [], [], start_offset}, fn entry,
                                                                            {:ok, refs, records,
                                                                             record_offset} ->
      payload_offset = record_offset + @segment_header_bytes
      ref = blob_ref_from_prehashed_segment(entry, segment_id, payload_offset)
      next_offset = payload_offset + entry.size
      record = [segment_header(ref), entry.payload]

      {:cont, {:ok, [ref | refs], [record | records], next_offset}}
    end)
    |> case do
      {:ok, unique_refs, records, next_offset} ->
        unique_refs = Enum.reverse(unique_refs)
        unique_refs_tuple = List.to_tuple(unique_refs)
        refs = Enum.map(batch.value_indexes, &elem(unique_refs_tuple, &1))
        {:ok, refs, Enum.reverse(records), next_offset}

      {:error, _reason} = error ->
        error
    end
  end

  defp blob_ref_from_prehashed_segment(entry, segment_id, offset) do
    %BlobRef{
      version: 2,
      checksum: entry.checksum,
      size: entry.size,
      segment_id: segment_id,
      offset: offset
    }
  end

  defp write_file(io, iodata) do
    case Process.get(:ferricstore_blob_store_write_hook) do
      fun when is_function(fun, 2) -> fun.(io, iodata)
      _ -> :file.write(io, iodata)
    end
  end

  defp prepare_payload_batch(payloads) do
    Enum.reduce_while(payloads, {:ok, [], %{}, [], 0, nil, 0}, fn
      payload, {:ok, entries, seen, indexes, bytes, error_ref, unique_count}
      when is_binary(payload) ->
        size = byte_size(payload)
        checksum = :crypto.hash(:sha256, payload)
        key = {size, checksum}

        case find_seen_payload(Map.get(seen, key, []), payload) do
          {:ok, index} ->
            {:cont, {:ok, entries, seen, [index | indexes], bytes, error_ref, unique_count}}

          :error ->
            entry = %{payload: payload, checksum: checksum, size: size}

            seen =
              Map.update(seen, key, [{payload, unique_count}], &[{payload, unique_count} | &1])

            error_ref = error_ref || %BlobRef{checksum: checksum, size: size}

            {:cont,
             {:ok, [entry | entries], seen, [unique_count | indexes],
              bytes + @segment_header_bytes + size, error_ref, unique_count + 1}}
        end

      _payload, {:ok, _entries, _seen, _indexes, _bytes, _error_ref, _unique_count} ->
        {:halt, {:error, :invalid_blob_payload}}
    end)
    |> case do
      {:ok, entries, _seen, indexes, bytes, error_ref, _unique_count} ->
        {:ok,
         %{
           unique_entries: Enum.reverse(entries),
           value_indexes: Enum.reverse(indexes),
           batch_bytes: bytes,
           error_ref: error_ref || empty_blob_error_ref()
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp find_seen_payload([], _payload), do: :error

  defp find_seen_payload([{seen_payload, index} | _rest], payload)
       when seen_payload == payload,
       do: {:ok, index}

  defp find_seen_payload([_other | rest], payload), do: find_seen_payload(rest, payload)

  defp segment_header(%BlobRef{version: 2, size: size, checksum: checksum})
       when is_binary(checksum) and byte_size(checksum) == 32 do
    <<@segment_header_magic::binary, size::unsigned-big-64, checksum::binary>>
  end

  defp maybe_fsync_new_segment_dir(_dir, true), do: :ok
  defp maybe_fsync_new_segment_dir(dir, false), do: fsync_dir(dir)

  defp writable_segment(data_dir, shard_index, batch_bytes) do
    case cached_active_segment(data_dir, shard_index, batch_bytes) do
      {:ok, segment} -> {:ok, segment}
      :miss -> scan_writable_segment(data_dir, shard_index, batch_bytes)
      {:error, _reason} = error -> error
    end
  end

  defp cached_active_segment(data_dir, shard_index, batch_bytes) do
    ensure_segment_table()
    key = {data_dir, shard_index}

    case :ets.lookup(@segment_table, key) do
      [{^key, id, path, cached_size}]
      when is_integer(id) and id >= 0 and is_binary(path) and is_integer(cached_size) and
             cached_size >= 0 ->
        if rotate_segment?(cached_size, batch_bytes) do
          rotate_after_segment(data_dir, shard_index, id)
        else
          {:ok, %{id: id, path: path, start_offset: cached_size, file_existed?: true}}
        end

      [] ->
        :miss

      _other ->
        :miss
    end
  end

  defp scan_writable_segment(data_dir, shard_index, batch_bytes) do
    shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)

    with {:ok, latest} <- latest_segment(shard_path),
         {:ok, next_id} <- read_next_segment_id(Path.join(shard_path, "segments")) do
      case latest do
        nil ->
          id = next_id || @segment_id
          path = segment_path(data_dir, shard_index, id)

          {:ok,
           %{
             id: id,
             path: path,
             start_offset: 0,
             file_existed?: File.exists?(path)
           }}

        %{id: id, path: path, size: size} ->
          if rotate_segment?(size, batch_bytes) do
            new_id = max(next_id || 0, id + 1)
            new_path = segment_path(data_dir, shard_index, new_id)

            {:ok,
             %{
               id: new_id,
               path: new_path,
               start_offset: 0,
               file_existed?: File.exists?(new_path)
             }}
          else
            cache_active_segment(data_dir, shard_index, id, path, size)
            {:ok, %{id: id, path: path, start_offset: size, file_existed?: true}}
          end
      end
    end
  end

  defp rotate_after_segment(data_dir, shard_index, id) do
    shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)

    with {:ok, next_id} <- read_next_segment_id(Path.join(shard_path, "segments")) do
      new_id = max(next_id || 0, id + 1)
      path = segment_path(data_dir, shard_index, new_id)

      {:ok,
       %{
         id: new_id,
         path: path,
         start_offset: 0,
         file_existed?: File.exists?(path)
       }}
    end
  end

  defp rotate_segment?(current_size, batch_bytes) do
    max_bytes = segment_max_bytes()
    current_size > 0 and current_size + batch_bytes > max_bytes
  end

  defp segment_max_bytes do
    case Process.get(
           :ferricstore_blob_store_segment_max_bytes,
           Application.get_env(:ferricstore, :blob_segment_max_bytes, @default_segment_max_bytes)
         ) do
      value when is_integer(value) and value > 0 -> value
      _other -> @default_segment_max_bytes
    end
  end

  defp latest_segment(shard_path) do
    with {:ok, paths} <- segment_files(shard_path) do
      Enum.reduce_while(paths, {:ok, nil}, fn path, {:ok, latest} ->
        with {:ok, id} <- segment_id_from_path(path),
             {:ok, %{type: :regular, size: size}} <- File.stat(path) do
          latest =
            case latest do
              nil -> %{id: id, path: path, size: size}
              %{id: current_id} when id > current_id -> %{id: id, path: path, size: size}
              _other -> latest
            end

          {:cont, {:ok, latest}}
        else
          {:ok, %{type: type}} ->
            {:halt, {:error, {:invalid_blob_segment_file, path, type}}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp do_sweep_unreferenced(data_dir, shard_index, live_refs) do
    shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)
    live_paths = live_relative_paths(live_refs)

    case blob_files(shard_path) do
      {:ok, paths} ->
        with :ok <- ensure_next_segment_id_for_dead_segments(shard_path, paths, live_paths),
             {:ok, stats} <- sweep_blob_paths(shard_path, paths, live_paths) do
          if stats.deleted_files > 0 do
            clear_active_segment_cache(data_dir, shard_index)
          end

          {:ok, stats}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_recovered(data_dir, shard_index) do
    ensure_recovery_table()
    key = {data_dir, shard_index}

    case :ets.lookup(@recovery_table, key) do
      [{^key, :recovered}] ->
        {:ok, %{segments: 0, truncated_segments: 0, truncated_bytes: 0}}

      [] ->
        recover_shard(data_dir, shard_index)
    end
  end

  defp mark_recovered(data_dir, shard_index) do
    ensure_recovery_table()
    :ets.insert(@recovery_table, {{data_dir, shard_index}, :recovered})
    :ok
  end

  defp ensure_recovery_table do
    case :ets.whereis(@recovery_table) do
      :undefined ->
        TableOwner.ensure_tables()

      tid ->
        tid
    end
  end

  defp ensure_segment_dir(data_dir, shard_index) do
    ensure_dir_table()
    key = {data_dir, shard_index}
    dir = Path.dirname(segment_path(data_dir, shard_index, @segment_id))

    case :ets.lookup(@dir_table, key) do
      [{^key, ^dir}] ->
        :ok

      _other ->
        create_segment_dir(key, dir)
    end
  end

  defp create_segment_dir(key, dir) do
    dir_existed? = Ferricstore.FS.dir?(dir)

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- fsync_parent_after_mkdir(dir, dir_existed?) do
      :ets.insert(@dir_table, {key, dir})
      :ok
    end
  end

  defp clear_segment_dir_cache(data_dir, shard_index) do
    ensure_dir_table()
    :ets.delete(@dir_table, {data_dir, shard_index})
    :ok
  end

  defp ensure_dir_table do
    case :ets.whereis(@dir_table) do
      :undefined ->
        TableOwner.ensure_tables()

      tid ->
        tid
    end
  end

  defp cache_active_segment(data_dir, shard_index, id, path, size) do
    ensure_segment_table()
    :ets.insert(@segment_table, {{data_dir, shard_index}, id, path, size})
    :ok
  end

  defp clear_active_segment_cache(data_dir, shard_index) do
    ensure_segment_table()
    :ets.delete(@segment_table, {data_dir, shard_index})
    :ok
  end

  defp ensure_segment_table do
    case :ets.whereis(@segment_table) do
      :undefined ->
        TableOwner.ensure_tables()

      tid ->
        tid
    end
  end

  defp empty_blob_error_ref, do: %BlobRef{checksum: :binary.copy(<<0>>, 32), size: 0}

  # Blob segments are shard-local files. A local ETS latch is enough to
  # serialize append offsets and GC deletes without paying for :global locks.
  defp with_blob_lock(data_dir, shard_index, fun) do
    key = {data_dir, shard_index}
    held = Process.get(@held_locks_key, %{})

    case Map.get(held, key) do
      nil ->
        :ok = acquire_blob_lock(key)
        Process.put(@held_locks_key, Map.put(held, key, 1))

        try do
          fun.()
        after
          release_blob_lock(key)
        end

      count when is_integer(count) and count > 0 ->
        Process.put(@held_locks_key, Map.put(held, key, count + 1))

        try do
          fun.()
        after
          release_blob_lock(key)
        end
    end
  end

  defp acquire_blob_lock(key) do
    ensure_lock_table()

    case :ets.insert_new(@lock_table, {key, self()}) do
      true ->
        :ok

      false ->
        wait_for_blob_lock(key)
    end
  end

  defp wait_for_blob_lock(key) do
    case :ets.lookup(@lock_table, key) do
      [{^key, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          blob_lock_backoff()
        else
          :ets.select_delete(@lock_table, [{{key, holder}, [], [true]}])
        end

      _other ->
        :ok
    end

    acquire_blob_lock(key)
  end

  defp release_blob_lock(key) do
    held = Process.get(@held_locks_key, %{})

    case Map.get(held, key) do
      count when is_integer(count) and count > 1 ->
        Process.put(@held_locks_key, Map.put(held, key, count - 1))

      1 ->
        next = Map.delete(held, key)

        if map_size(next) == 0 do
          Process.delete(@held_locks_key)
        else
          Process.put(@held_locks_key, next)
        end

        ensure_lock_table()
        :ets.select_delete(@lock_table, [{{key, self()}, [], [true]}])

      _other ->
        :ok
    end
  end

  defp blob_lock_backoff do
    receive do
    after
      @lock_retry_ms -> :ok
    end
  end

  defp ensure_lock_table do
    case :ets.whereis(@lock_table) do
      :undefined ->
        TableOwner.ensure_tables()

      tid ->
        tid
    end
  end

  defp segment_path(data_dir, shard_index, segment_id) do
    Path.join([
      Ferricstore.DataDir.blob_shard_path(data_dir, shard_index),
      "segments",
      BlobRef.segment_filename(segment_id)
    ])
  end

  defp stat_regular_size(path, expected_size) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: ^expected_size}} -> :ok
      {:ok, %{type: :regular}} -> {:error, :size_mismatch}
      {:ok, _other} -> {:error, :invalid_blob_file}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stat_regular_min_size(path, min_size) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} when size >= min_size -> :ok
      {:ok, %{type: :regular}} -> {:error, :size_mismatch}
      {:ok, _other} -> {:error, :invalid_blob_file}
      {:error, reason} -> {:error, reason}
    end
  end

  defp pread_exact_open(io, offset, size) do
    case :file.pread(io, offset, size) do
      {:ok, payload} when byte_size(payload) == size -> {:ok, payload}
      {:ok, _short} -> {:error, :size_mismatch}
      :eof -> {:error, :enoent}
      {:error, reason} -> {:error, reason}
    end
  end

  defp open_read_file(path) do
    modes = [:read, :raw, :binary]

    case Process.get(:ferricstore_blob_store_open_read_hook) do
      fun when is_function(fun, 2) -> fun.(path, modes)
      _other -> File.open(path, modes)
    end
  end

  defp fsync_parent_after_mkdir(_dir, true), do: :ok

  defp fsync_parent_after_mkdir(dir, false) do
    # The first append segment in a shard must make the segments directory entry
    # durable before the segment file itself is fsynced.
    fsync_dir(Path.dirname(dir))
  end

  defp fsync_file(path) do
    case Process.get(:ferricstore_blob_store_fsync_file_hook) do
      fun when is_function(fun, 1) -> normalize_fsync(fun.(path))
      _ -> normalize_fsync(NIF.v2_fsync(path))
    end
  end

  defp fsync_dir(path) do
    case Process.get(:ferricstore_blob_store_fsync_dir_hook) do
      fun when is_function(fun, 1) -> normalize_fsync(fun.(path))
      _ -> normalize_fsync(NIF.v2_fsync_dir(path))
    end
  end

  defp normalize_fsync(:ok), do: :ok
  defp normalize_fsync({:error, reason}), do: {:error, reason}

  defp emit_error(operation, shard_index, path, %BlobRef{size: size}, reason) do
    :telemetry.execute(
      [:ferricstore, :blob, :error],
      %{count: 1, bytes: size},
      %{operation: operation, shard_index: shard_index, reason: reason, path: path}
    )
  end

  defp file_matches_ref?(path, %BlobRef{checksum: expected_checksum}) do
    case open_read_file(path) do
      {:ok, io} ->
        try do
          case hash_file(io, :crypto.hash_init(:sha256)) do
            {:ok, ^expected_checksum} -> :ok
            {:ok, _other_checksum} -> :mismatch
            {:error, reason} -> {:error, reason}
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_segment_payload(path, offset, size, %BlobRef{} = ref) do
    case open_read_file(path) do
      {:ok, io} ->
        try do
          with :ok <- validate_open_segment_record(io, offset, size, ref),
               {:ok, payload} <- pread_exact_open(io, offset, size),
               :ok <- verify_checksum(ref, payload) do
            {:ok, payload}
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_segment_payload_range(path, offset, size, %BlobRef{} = ref, relative_offset, count) do
    case open_read_file(path) do
      {:ok, io} ->
        try do
          with :ok <- validate_open_segment_record(io, offset, size, ref),
               {:ok, payload} <- pread_exact_open(io, offset + relative_offset, count) do
            {:ok, payload}
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_file_range(path, offset, count) do
    case open_read_file(path) do
      {:ok, io} ->
        try do
          pread_exact_open(io, offset, count)
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp verify_segment_record(path, offset, size, %BlobRef{} = ref) do
    case open_read_file(path) do
      {:ok, io} ->
        try do
          with :ok <- validate_open_segment_record(io, offset, size, ref),
               :ok <- open_file_range_matches_ref?(io, offset, size, ref) do
            :ok
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_segment_record_header(path, offset, size, %BlobRef{} = ref) do
    case open_read_file(path) do
      {:ok, io} ->
        try do
          validate_open_segment_record(io, offset, size, ref)
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_open_segment_record(
         io,
         offset,
         size,
         %BlobRef{version: 2, checksum: expected_checksum}
       ) do
    header_offset = offset - @segment_header_bytes

    with true <- header_offset >= 0,
         {:ok, header} when byte_size(header) == @segment_header_bytes <-
           :file.pread(io, header_offset, @segment_header_bytes),
         {:ok, ^size, ^expected_checksum} <- decode_segment_header(header) do
      :ok
    else
      _ -> {:error, :segment_header_mismatch}
    end
  end

  defp open_file_range_matches_ref?(io, offset, size, %BlobRef{checksum: expected_checksum}) do
    case hash_file_range(io, offset, size, :crypto.hash_init(:sha256)) do
      {:ok, ^expected_checksum} -> :ok
      {:ok, _other_checksum} -> :mismatch
      {:error, reason} -> {:error, reason}
    end
  end

  defp hash_file(io, hash_state) do
    case :file.read(io, @hash_chunk_bytes) do
      {:ok, chunk} when is_binary(chunk) and byte_size(chunk) > 0 ->
        hash_file(io, :crypto.hash_update(hash_state, chunk))

      :eof ->
        {:ok, :crypto.hash_final(hash_state)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp hash_file_range(_io, _offset, 0, hash_state), do: {:ok, :crypto.hash_final(hash_state)}

  defp hash_file_range(io, offset, remaining, hash_state) do
    read_size = min(@hash_chunk_bytes, remaining)

    case :file.pread(io, offset, read_size) do
      {:ok, chunk} when is_binary(chunk) and byte_size(chunk) == read_size ->
        hash_file_range(
          io,
          offset + read_size,
          remaining - read_size,
          :crypto.hash_update(hash_state, chunk)
        )

      {:ok, _short} ->
        {:error, :size_mismatch}

      :eof ->
        {:error, :size_mismatch}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp recover_segment(path, can_truncate?) do
    case File.open(path, [:read, :write, :raw, :binary]) do
      {:ok, io} ->
        try do
          with {:ok, size} <- file_size(io),
               {:ok, valid_size} <- scan_segment(io, 0, size),
               {:ok, truncated_bytes} <-
                 maybe_truncate_segment(io, path, size, valid_size, can_truncate?) do
            {:ok, truncated_bytes}
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp file_size(io) do
    with {:ok, current} <- :file.position(io, :cur),
         {:ok, size} <- :file.position(io, :eof),
         {:ok, _} <- :file.position(io, current) do
      {:ok, size}
    end
  end

  defp scan_segment(_io, offset, size) when offset == size, do: {:ok, offset}

  defp scan_segment(_io, offset, size) when size - offset < @segment_header_bytes,
    do: {:ok, offset}

  defp scan_segment(io, offset, size) do
    case :file.pread(io, offset, @segment_header_bytes) do
      {:ok, header} when byte_size(header) == @segment_header_bytes ->
        case decode_segment_header(header) do
          {:ok, payload_size, checksum} ->
            payload_offset = offset + @segment_header_bytes
            next_offset = payload_offset + payload_size

            cond do
              next_offset > size ->
                {:ok, offset}

              segment_payload_matches?(io, payload_offset, payload_size, checksum) ->
                scan_segment(io, next_offset, size)

              true ->
                {:ok, offset}
            end

          :error ->
            {:ok, offset}
        end

      {:ok, _short} ->
        {:ok, offset}

      :eof ->
        {:ok, offset}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_segment_header(
         <<@segment_header_magic::binary, size::unsigned-big-64, checksum::binary-size(32)>>
       ),
       do: {:ok, size, checksum}

  defp decode_segment_header(_header), do: :error

  defp segment_payload_matches?(io, payload_offset, payload_size, checksum) do
    case hash_file_range(io, payload_offset, payload_size, :crypto.hash_init(:sha256)) do
      {:ok, ^checksum} -> true
      _ -> false
    end
  end

  defp maybe_truncate_segment(_io, _path, size, size, _can_truncate?), do: {:ok, 0}

  defp maybe_truncate_segment(_io, path, _size, _valid_size, false) do
    {:error, {:corrupt_immutable_blob_segment, path}}
  end

  defp maybe_truncate_segment(io, path, size, valid_size, true) do
    with {:ok, _} <- :file.position(io, valid_size),
         :ok <- :file.truncate(io),
         :ok <- fsync_file(path) do
      {:ok, size - valid_size}
    end
  end

  defp validate_blob_range(size, relative_offset, count)
       when is_integer(size) and size >= 0 and is_integer(relative_offset) and
              relative_offset >= 0 and is_integer(count) and count >= 0 and
              relative_offset + count <= size,
       do: :ok

  defp validate_blob_range(_size, _relative_offset, _count), do: {:error, :invalid_blob_range}

  defp verify_size(%BlobRef{size: size}, payload) do
    if byte_size(payload) == size do
      :ok
    else
      {:error, :size_mismatch}
    end
  end

  defp verify_checksum(%BlobRef{} = ref, payload) do
    if BlobRef.verify_payload?(ref, payload) do
      :ok
    else
      {:error, :checksum_mismatch}
    end
  end

  defp live_relative_paths(live_refs) do
    Enum.reduce(live_refs, MapSet.new(), fn
      %BlobRef{} = ref, acc -> MapSet.put(acc, BlobRef.relative_path(ref))
      _other, acc -> acc
    end)
  end

  defp ensure_next_segment_id_for_dead_segments(shard_path, paths, live_paths) do
    Enum.reduce_while(paths, {:ok, nil}, fn path, {:ok, max_dead_id} ->
      relative = Path.relative_to(path, shard_path)

      case segment_id_from_path(path) do
        {:ok, id} ->
          if MapSet.member?(live_paths, relative) do
            {:cont, {:ok, max_dead_id}}
          else
            {:cont, {:ok, max(id, max_dead_id || id)}}
          end

        :not_segment ->
          {:cont, {:ok, max_dead_id}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, nil} -> :ok
      {:ok, max_dead_id} -> ensure_next_segment_id_at_least(shard_path, max_dead_id + 1)
      {:error, _reason} = error -> error
    end
  end

  defp ensure_next_segment_id_at_least(shard_path, min_next_id) do
    segment_dir = Path.join(shard_path, "segments")

    with {:ok, current_next_id} <- read_next_segment_id(segment_dir) do
      if (current_next_id || 0) >= min_next_id do
        :ok
      else
        persist_next_segment_id(segment_dir, min_next_id)
      end
    end
  end

  defp read_next_segment_id(segment_dir) do
    path = Path.join(segment_dir, @segment_next_id_filename)

    case File.read(path) do
      {:ok, data} ->
        parse_next_segment_id(data, path)

      {:error, :enoent} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, {:blob_segment_next_id_read_failed, path, reason}}
    end
  end

  defp parse_next_segment_id(data, path) when is_binary(data) do
    case Integer.parse(String.trim(data)) do
      {id, ""} when id >= 0 -> {:ok, id}
      _other -> {:error, {:blob_segment_next_id_invalid, path}}
    end
  end

  defp persist_next_segment_id(segment_dir, next_id) when is_integer(next_id) and next_id >= 0 do
    path = Path.join(segment_dir, @segment_next_id_filename)
    tmp_path = path <> ".tmp"

    result =
      with :ok <- File.write(tmp_path, Integer.to_string(next_id) <> "\n", [:binary]),
           :ok <- fsync_file(tmp_path),
           :ok <- Ferricstore.FS.rename(tmp_path, path),
           :ok <- fsync_dir(segment_dir) do
        :ok
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        _ = Ferricstore.FS.rm(tmp_path)
        {:error, {:blob_segment_next_id_persist_failed, path, reason}}
    end
  end

  defp segment_id_from_path(path) do
    if Path.extname(path) == ".bloblog" do
      path
      |> Path.basename(".bloblog")
      |> Integer.parse()
      |> case do
        {id, ""} when id >= 0 -> {:ok, id}
        _other -> {:error, {:invalid_blob_segment_name, path}}
      end
    else
      :not_segment
    end
  end

  defp blob_files(shard_path) do
    with {:ok, legacy_paths} <- legacy_blob_files(shard_path),
         {:ok, segment_paths} <- segment_files(shard_path) do
      {:ok, legacy_paths ++ segment_paths}
    end
  end

  defp legacy_blob_files(shard_path) do
    if Ferricstore.FS.dir?(shard_path) do
      {:ok, Path.wildcard(Path.join(shard_path, "**/*.blob"))}
    else
      {:ok, []}
    end
  rescue
    error -> {:error, {:blob_list_failed, error}}
  end

  defp segment_files(shard_path) do
    segment_path = Path.join(shard_path, "segments")

    if Ferricstore.FS.dir?(segment_path) do
      {:ok, Path.wildcard(Path.join(segment_path, "*.bloblog"))}
    else
      {:ok, []}
    end
  rescue
    error -> {:error, {:blob_segment_list_failed, error}}
  end

  defp blob_tmp_files(shard_path) do
    if Ferricstore.FS.dir?(shard_path) do
      {:ok, Path.wildcard(Path.join(shard_path, "**/*.tmp"), match_dot: true)}
    else
      {:ok, []}
    end
  rescue
    error -> {:error, {:blob_tmp_list_failed, error}}
  end

  defp sweep_blob_paths(shard_path, paths, live_paths) do
    result =
      Enum.reduce_while(
        paths,
        {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 0}, MapSet.new()},
        fn path, {:ok, stats, dirs} ->
          relative = Path.relative_to(path, shard_path)

          if MapSet.member?(live_paths, relative) do
            {:cont, {:ok, %{stats | kept_files: stats.kept_files + 1}, dirs}}
          else
            case delete_blob_file(path) do
              {:ok, size} ->
                stats = %{
                  stats
                  | deleted_files: stats.deleted_files + 1,
                    deleted_bytes: stats.deleted_bytes + size
                }

                {:cont, {:ok, stats, MapSet.put(dirs, Path.dirname(path))}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end
        end
      )

    case result do
      {:ok, stats, dirs} ->
        with {:ok, tmp_stats, tmp_dirs} <- sweep_tmp_paths(shard_path),
             :ok <- fsync_deleted_dirs(MapSet.union(dirs, tmp_dirs)) do
          {:ok, Map.merge(stats, tmp_stats)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp sweep_tmp_paths(shard_path) do
    case blob_tmp_files(shard_path) do
      {:ok, paths} ->
        Enum.reduce_while(
          paths,
          {:ok, %{deleted_tmp_files: 0, deleted_tmp_bytes: 0}, MapSet.new()},
          fn path, {:ok, stats, dirs} ->
            if stale_tmp_file?(path) do
              case delete_blob_file(path) do
                {:ok, size} ->
                  stats = %{
                    stats
                    | deleted_tmp_files: stats.deleted_tmp_files + 1,
                      deleted_tmp_bytes: stats.deleted_tmp_bytes + size
                  }

                  {:cont, {:ok, stats, MapSet.put(dirs, Path.dirname(path))}}

                {:error, _reason} = error ->
                  {:halt, error}
              end
            else
              {:cont, {:ok, stats, dirs}}
            end
          end
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp stale_tmp_file?(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{type: :regular, mtime: mtime}} when is_integer(mtime) ->
        System.system_time(:second) - mtime >= @tmp_stale_after_seconds

      _ ->
        false
    end
  end

  defp delete_blob_file(path) do
    size =
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} -> size
        _ -> 0
      end

    case Ferricstore.FS.rm(path) do
      :ok -> {:ok, size}
      {:error, {:not_found, _message}} -> {:ok, 0}
      {:error, reason} -> {:error, {:blob_delete_failed, path, reason}}
    end
  end

  defp fsync_deleted_dirs(dirs) do
    Enum.reduce_while(dirs, :ok, fn dir, :ok ->
      case fsync_dir(dir) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:blob_delete_fsync_failed, dir, reason}}}
      end
    end)
  end
end
