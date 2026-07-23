defmodule Ferricstore.Flow.Query.IndexRegistryJournal do
  @moduledoc false

  alias Ferricstore.TermCodec

  @tag :flow_query_index_registry_progress
  @version 1
  @relative_path "flow_query/index-registry-progress.journal"
  @digest_bytes 32
  @header_bytes 4 + @digest_bytes
  @max_event_bytes 64 * 1_024
  @max_bytes 8 * 1_024 * 1_024
  @max_events 65_536

  @spec path(map()) :: binary()
  def path(%{data_dir: data_dir}) when is_binary(data_dir) and data_dir != "",
    do: Path.join(data_dir, @relative_path)

  @spec read(map()) :: {:ok, [term()]} | {:error, term()}
  def read(ctx) do
    journal_path = path(ctx)

    case Ferricstore.FS.read_nofollow(journal_path, @max_bytes) do
      {:ok, contents} ->
        with {:ok, events, valid_bytes} <- decode_frames(contents),
             :ok <- repair(ctx, contents, valid_bytes) do
          {:ok, events}
        end

      {:error, {:not_found, _reason}} ->
        with :ok <- reset(ctx), do: {:ok, []}

      {:error, reason} ->
        {:error, {:query_index_registry_journal_read_failed, reason}}
    end
  end

  @spec append(map(), term()) :: :ok | {:error, term()}
  def append(ctx, event) do
    payload = TermCodec.encode({@tag, @version, event})

    if byte_size(payload) <= @max_event_bytes do
      digest = :crypto.hash(:sha256, payload)
      frame = <<byte_size(payload)::unsigned-big-32, digest::binary, payload::binary>>
      journal_path = path(ctx)

      with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(journal_path)),
           :ok <- Ferricstore.FS.append_sync_nofollow_bounded(journal_path, frame, @max_bytes) do
        :ok
      end
    else
      {:error, :query_index_registry_journal_event_too_large}
    end
  end

  @spec reset(map()) :: :ok | {:error, term()}
  def reset(ctx) do
    journal_path = path(ctx)

    with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(journal_path)) do
      Ferricstore.FS.atomic_replace_nofollow(journal_path, "", @max_bytes)
    end
  end

  defp decode_frames(contents) when is_binary(contents),
    do: decode_frames(contents, [], 0, 0)

  defp decode_frames(<<>>, events, valid_bytes, _count),
    do: {:ok, Enum.reverse(events), valid_bytes}

  defp decode_frames(rest, events, valid_bytes, _count)
       when byte_size(rest) < @header_bytes,
       do: {:ok, Enum.reverse(events), valid_bytes}

  defp decode_frames(
         <<size::unsigned-big-32, digest::binary-size(@digest_bytes), rest::binary>>,
         events,
         valid_bytes,
         count
       ) do
    cond do
      size == 0 or size > @max_event_bytes ->
        {:error, {:invalid_query_index_registry_journal, :invalid_frame_size}}

      count >= @max_events ->
        {:error, {:invalid_query_index_registry_journal, :too_many_events}}

      byte_size(rest) < size ->
        {:ok, Enum.reverse(events), valid_bytes}

      true ->
        <<payload::binary-size(size), tail::binary>> = rest

        with true <- :crypto.hash(:sha256, payload) == digest,
             {:ok, {@tag, @version, event}} <- TermCodec.decode(payload) do
          frame_bytes = @header_bytes + size
          decode_frames(tail, [event | events], valid_bytes + frame_bytes, count + 1)
        else
          false -> {:error, {:invalid_query_index_registry_journal, :checksum_mismatch}}
          _invalid -> {:error, {:invalid_query_index_registry_journal, :decode_failed}}
        end
    end
  end

  defp repair(_ctx, contents, valid_bytes) when valid_bytes == byte_size(contents), do: :ok

  defp repair(ctx, contents, valid_bytes) do
    valid_prefix = binary_part(contents, 0, valid_bytes)

    case Ferricstore.FS.atomic_replace_nofollow(path(ctx), valid_prefix, @max_bytes) do
      :ok -> :ok
      {:error, reason} -> {:error, {:query_index_registry_journal_repair_failed, reason}}
    end
  end
end
