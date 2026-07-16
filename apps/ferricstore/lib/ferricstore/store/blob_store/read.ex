defmodule Ferricstore.Store.BlobStore.Read do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore.TableOwner

      @doc "Reads and validates a blob by ref."
      @spec get(binary(), non_neg_integer(), BlobRef.t()) :: {:ok, binary()} | {:error, reason()}
      def get(data_dir, shard_index, %BlobRef{} = ref)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
        if BlobRef.valid?(ref), do: do_get(data_dir, shard_index, ref), else: invalid_blob_ref()
      end

      defp do_get(data_dir, shard_index, %BlobRef{size: size, offset: offset} = ref) do
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

      defp do_verify(
             data_dir,
             shard_index,
             %BlobRef{size: size, offset: offset} = ref
           ) do
        path = BlobRef.path(data_dir, shard_index, ref)

        result =
          with :ok <- stat_regular_min_size(path, offset + size),
               :ok <- verify_segment_record(path, offset, size, ref) do
            :ok
          end

        case result do
          :ok ->
            :ok

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
        with {:ok, unique_refs} <- unique_blob_refs(refs) do
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

      defp verify_segment_refs(data_dir, shard_index, refs) do
        refs
        |> Enum.group_by(& &1.segment_id)
        |> Enum.reduce_while(:ok, fn {segment_id, segment_refs}, :ok ->
          path = segment_path(data_dir, shard_index, segment_id)

          case verify_segment_refs_at_path(path, shard_index, segment_refs) do
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
        header_results = read_segment_headers(io, refs)

        refs
        |> Enum.zip(header_results)
        |> Enum.reduce_while(:ok, fn
          {%BlobRef{offset: offset, size: size} = ref, :ok}, :ok ->
            result = open_file_range_matches_ref?(io, offset, size, ref)

            case result do
              :ok ->
                {:cont, :ok}

              {:error, reason} = error ->
                emit_error(:verify, shard_index, path, ref, reason)
                {:halt, error}
            end

          {%BlobRef{} = ref, {:error, reason} = error}, :ok ->
            emit_error(:verify, shard_index, path, ref, reason)
            {:halt, error}
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
        put_segment_ref_results(%{}, data_dir, shard_index, refs)
      end

      defp put_segment_ref_results(results, _data_dir, _shard_index, []), do: results

      defp put_segment_ref_results(results, data_dir, shard_index, refs) do
        refs
        |> Enum.group_by(& &1.segment_id)
        |> Enum.reduce(results, fn {segment_id, segment_refs}, acc ->
          path = segment_path(data_dir, shard_index, segment_id)
          Map.merge(acc, get_segment_refs_at_path(path, shard_index, segment_refs))
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
        header_results = read_segment_headers(io, refs)
        payload_results = read_segment_payloads(io, refs, header_results)

        refs
        |> Enum.zip(payload_results)
        |> Enum.reduce(%{}, fn {ref, result}, acc ->
          case result do
            {:ok, _payload} = ok ->
              Map.put(acc, ref, ok)

            {:error, reason} = error ->
              emit_error(:get, shard_index, path, ref, reason)
              Map.put(acc, ref, error)
          end
        end)
      end

      defp read_segment_payloads(io, refs, header_results) do
        readable_refs =
          refs
          |> Enum.zip(header_results)
          |> Enum.flat_map(fn
            {ref, :ok} -> [ref]
            {_ref, {:error, _reason}} -> []
          end)

        payload_reads =
          Enum.map(readable_refs, fn %BlobRef{offset: offset, size: size} ->
            {offset, size}
          end)

        payload_results =
          case payload_reads do
            [] ->
              %{}

            _ ->
              case :file.pread(io, payload_reads) do
                {:ok, payloads} ->
                  readable_refs
                  |> Enum.zip(payloads)
                  |> Map.new(fn {ref, payload} ->
                    {ref, validate_segment_payload(payload, ref)}
                  end)

                {:error, reason} ->
                  Map.new(readable_refs, &{&1, {:error, reason}})
              end
          end

        refs
        |> Enum.zip(header_results)
        |> Enum.map(fn
          {ref, :ok} -> Map.fetch!(payload_results, ref)
          {_ref, {:error, _reason} = error} -> error
        end)
      end

      defp validate_segment_payload(payload, %BlobRef{size: size} = ref)
           when is_binary(payload) and byte_size(payload) == size do
        case verify_checksum(ref, payload) do
          :ok -> {:ok, payload}
          {:error, _reason} = error -> error
        end
      end

      defp validate_segment_payload(payload, %BlobRef{}) when is_binary(payload),
        do: {:error, :size_mismatch}

      defp validate_segment_payload(:eof, %BlobRef{}), do: {:error, :enoent}
      defp validate_segment_payload(_payload, %BlobRef{}), do: {:error, :size_mismatch}

      defp load_blob_file_refs(data_dir, shard_index, refs) do
        put_segment_file_ref_results(%{}, data_dir, shard_index, refs)
      end

      defp put_segment_file_ref_results(results, _data_dir, _shard_index, []), do: results

      defp put_segment_file_ref_results(results, data_dir, shard_index, refs) do
        refs
        |> Enum.group_by(& &1.segment_id)
        |> Enum.reduce(results, fn {segment_id, segment_refs}, acc ->
          path = segment_path(data_dir, shard_index, segment_id)
          Map.merge(acc, get_segment_file_refs_at_path(path, shard_index, segment_refs))
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
        header_results = read_segment_headers(io, refs)

        refs
        |> Enum.zip(header_results)
        |> Enum.reduce(%{}, fn {%BlobRef{offset: offset, size: size} = ref, header_result}, acc ->
          result =
            case header_result do
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

      defp read_segment_headers(io, refs) do
        readable_refs =
          Enum.filter(refs, fn %BlobRef{offset: offset} ->
            offset >= @segment_header_bytes
          end)

        header_reads =
          Enum.map(readable_refs, fn %BlobRef{offset: offset} ->
            {offset - @segment_header_bytes, @segment_header_bytes}
          end)

        read_results =
          case header_reads do
            [] ->
              %{}

            _ ->
              case :file.pread(io, header_reads) do
                {:ok, headers} ->
                  readable_refs
                  |> Enum.zip(headers)
                  |> Map.new(fn {ref, header} ->
                    {ref, validate_segment_header(header, ref)}
                  end)

                {:error, reason} ->
                  Map.new(readable_refs, &{&1, {:error, reason}})
              end
          end

        Enum.map(refs, &Map.get(read_results, &1, {:error, :segment_header_mismatch}))
      end

      defp validate_segment_header(
             header,
             %BlobRef{size: size, checksum: expected_checksum}
           )
           when is_binary(header) and byte_size(header) == @segment_header_bytes do
        case decode_segment_header(header) do
          {:ok, ^size, ^expected_checksum} -> :ok
          _other -> {:error, :segment_header_mismatch}
        end
      end

      defp validate_segment_header(_header, %BlobRef{}), do: {:error, :segment_header_mismatch}

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

      defp do_file_ref(
             data_dir,
             shard_index,
             %BlobRef{size: size, offset: offset} = ref
           ) do
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
      Reads a byte range from a blob ref.

      This materialized range API is used by commands that return bytes through
      BEAM. Segment-backed partial ranges validate the record header and pread only
      the requested bytes; full-range reads still validate the full payload checksum.
      `file_ref/3` remains the stat/header-validated streaming path for full
      large-value reads.
      """
      @spec get_range(
              binary(),
              non_neg_integer(),
              BlobRef.t(),
              non_neg_integer(),
              non_neg_integer()
            ) ::
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
             %BlobRef{size: size, offset: offset} = ref,
             relative_offset,
             count
           ) do
        path = BlobRef.path(data_dir, shard_index, ref)

        result =
          with :ok <- validate_blob_range(size, relative_offset, count),
               {:ok, slice} <-
                 read_segment_payload_range(path, offset, size, ref, relative_offset, count) do
            {:ok, slice}
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
    end
  end
end
