defmodule Ferricstore.Flow.LMDB.SegmentPins do
  @moduledoc false

  alias Ferricstore.Flow.LMDB.{Access, ValueLocator}
  alias Ferricstore.TermCodec

  @u64_decimal_zero_pad "00000000000000000000"
  @max_u64 18_446_744_073_709_551_615
  @max_batch_entries 100_000

  def prefix, do: "flow-segment-value-pin:"

  def prefix(tag) when tag in [:waraft_segment, :waraft_apply_projection] do
    prefix() <> "batch:" <> Atom.to_string(tag) <> ":"
  end

  def encode_batch({tag, index} = file_id, entries)
      when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and index > 0 and
             index <= @max_u64 and is_list(entries) do
    if valid_batch_entries?(entries) do
      TermCodec.encode({:flow_segment_value_pin_batch, 1, file_id, entries})
    else
      raise ArgumentError, "invalid Flow segment pin batch"
    end
  end

  def encode_batch(_file_id, _entries),
    do: raise(ArgumentError, "invalid Flow segment pin batch")

  def batch_put_ops(entries) when is_list(entries) do
    normalized = normalize_entries(entries)

    value_ops =
      Enum.map(normalized, fn %{
                                key: key,
                                expire_at_ms: expire_at_ms,
                                file_id: file_id,
                                offset: offset,
                                value_size: value_size
                              } ->
        {:put, key, ValueLocator.encode(expire_at_ms, file_id, offset, value_size)}
      end)

    pin_ops =
      normalized
      |> Enum.group_by(& &1.file_id)
      |> Enum.map(fn {file_id, group} ->
        batch_entries =
          Enum.map(group, fn %{
                               key: key,
                               expire_at_ms: expire_at_ms,
                               offset: offset,
                               value_size: value_size
                             } ->
            {key, expire_at_ms, offset, value_size}
          end)

        {:put, batch_key(file_id, batch_entries), encode_batch(file_id, batch_entries)}
      end)

    value_ops ++ pin_ops
  end

  def entries_before(path, trim_index, limit)
      when is_binary(path) and is_integer(trim_index) and trim_index > 0 and is_integer(limit) and
             limit > 0 do
    case entries_before_page(path, trim_index, <<>>, limit) do
      {:ok, pins, _cursor, true} ->
        {:ok, pins}

      {:ok, _pins, _cursor, false} ->
        {:error, {:flow_segment_value_pin_scan_limit, limit}}

      {:error, _reason} = error ->
        error
    end
  end

  def entries_before(_path, _trim_index, _limit), do: {:ok, []}

  def entries_before_page(path, trim_index, after_key, limit)
      when is_binary(path) and is_integer(trim_index) and trim_index > 0 and
             is_binary(after_key) and is_integer(limit) and limit > 0 do
    with {:ok, entries, source_done?} <-
           entries_before_page_for_tags(path, trim_index, after_key, limit + 1) do
      decode_entries_before_page(entries, after_key, limit, source_done?)
    end
  end

  def entries_before_page(_path, _trim_index, after_key, _limit) when is_binary(after_key),
    do: {:ok, [], after_key, true}

  defp normalize_entries(entries) when length(entries) <= @max_batch_entries do
    Enum.map(entries, fn
      %{
        key: key,
        expire_at_ms: expire_at_ms,
        source_file_id: file_id,
        source_offset: offset,
        source_value_size: value_size
      } ->
        normalize_entry!(key, expire_at_ms, file_id, offset, value_size)

      %{
        key: key,
        expire_at_ms: expire_at_ms,
        file_id: file_id,
        offset: offset,
        value_size: value_size
      } ->
        normalize_entry!(key, expire_at_ms, file_id, offset, value_size)

      {key, expire_at_ms, file_id, offset, value_size} ->
        normalize_entry!(key, expire_at_ms, file_id, offset, value_size)

      _other ->
        raise ArgumentError, "invalid Flow segment pin entry"
    end)
  end

  defp normalize_entries(_entries),
    do: raise(ArgumentError, "Flow segment pin batch exceeds #{@max_batch_entries} entries")

  defp normalize_entry!(key, expire_at_ms, {tag, index} = file_id, offset, value_size)
       when tag in [:waraft_segment, :waraft_apply_projection] and is_binary(key) and
              is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64 and
              is_integer(index) and index > 0 and index <= @max_u64 and is_integer(offset) and
              offset >= 0 and offset <= @max_u64 and is_integer(value_size) and value_size >= 0 and
              value_size <= @max_u64 do
    %{
      key: key,
      expire_at_ms: expire_at_ms,
      file_id: file_id,
      offset: offset,
      value_size: value_size
    }
  end

  defp normalize_entry!(_key, _expire_at_ms, _file_id, _offset, _value_size),
    do: raise(ArgumentError, "invalid Flow segment pin entry")

  defp batch_key({tag, index} = file_id, entries)
       when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and
              index > 0 and is_list(entries) do
    digest = :crypto.hash(:sha256, TermCodec.encode({file_id, entries}))
    prefix(tag) <> pad_u64(index) <> <<0>> <> digest
  end

  defp entries_before_page_for_tags(path, trim_index, after_key, fetch_limit) do
    [:waraft_apply_projection, :waraft_segment]
    |> Enum.reduce_while({:ok, [], true}, fn tag, {:ok, acc, all_done?} ->
      case entries_for_tag(path, tag, after_key, fetch_limit) do
        {:ok, entries, cursor_past_tag?} ->
          case eligible_tag_entries(entries, tag, trim_index, fetch_limit, cursor_past_tag?) do
            {:ok, eligible, tag_done?} ->
              {:cont, {:ok, eligible ++ acc, all_done? and tag_done?}}

            {:error, _reason} = error ->
              {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, entries, done?} ->
        entries =
          entries
          |> Enum.uniq_by(fn {key, _value} -> key end)
          |> Enum.sort_by(fn {key, _value} -> key end)

        {:ok, entries, done?}

      {:error, _reason} = error ->
        error
    end
  end

  defp entries_for_tag(path, tag, after_key, fetch_limit) do
    tag_prefix = prefix(tag)

    cond do
      after_key == <<>> or after_key < tag_prefix ->
        case Access.prefix_entries(path, tag_prefix, fetch_limit) do
          {:ok, entries} -> {:ok, entries, false}
          {:error, _reason} = error -> error
        end

      String.starts_with?(after_key, tag_prefix) ->
        case Access.prefix_entries_after(path, tag_prefix, after_key, fetch_limit) do
          {:ok, entries} -> {:ok, entries, false}
          {:error, _reason} = error -> error
        end

      true ->
        {:ok, [], true}
    end
  end

  defp eligible_tag_entries(entries, tag, trim_index, fetch_limit, cursor_past_tag?)
       when is_list(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {key, value} = entry, {:ok, acc} when is_binary(key) and is_binary(value) ->
        case decode_key(key) do
          {:ok, {^tag, index}} when index < trim_index ->
            {:cont, {:ok, [entry | acc]}}

          {:ok, {^tag, index}} when index >= trim_index ->
            {:halt, {:done, acc}}

          _invalid ->
            {:halt, {:error, {:corrupt_flow_segment_value_pin, key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:corrupt_flow_segment_value_pin, invalid}}}
    end)
    |> case do
      {:done, reversed} ->
        {:ok, Enum.reverse(reversed), true}

      {:ok, reversed} ->
        done? = cursor_past_tag? or length(entries) < fetch_limit
        {:ok, Enum.reverse(reversed), done?}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_entries_before_page(entries, after_key, limit, source_done?) do
    collect_page(entries, after_key, limit, source_done?, [], 0)
  end

  defp collect_page([], last_key, _limit, source_done?, reversed_pins, _count),
    do: {:ok, Enum.reverse(reversed_pins), last_key, source_done?}

  defp collect_page(
         [{key, _blob} = entry | rest],
         last_key,
         limit,
         source_done?,
         reversed_pins,
         count
       ) do
    case decode_entry(entry) do
      {:ok, pin} ->
        pins = List.wrap(pin.pins)
        next_count = count + length(pins)

        cond do
          next_count > limit and count == 0 ->
            {:error, {:flow_segment_value_pin_scan_limit, limit}}

          next_count > limit ->
            {:ok, Enum.reverse(reversed_pins), last_key, false}

          next_count == limit ->
            done? = rest == [] and source_done?
            {:ok, Enum.reverse(:lists.reverse(pins, reversed_pins)), key, done?}

          true ->
            collect_page(
              rest,
              key,
              limit,
              source_done?,
              :lists.reverse(pins, reversed_pins),
              next_count
            )
        end

      {:error, _reason} = error ->
        error

      :skip ->
        {:error, {:corrupt_flow_segment_value_pin, key}}
    end
  end

  defp decode_entry({pin_key, blob}) when is_binary(pin_key) and is_binary(blob) do
    with {:ok, {tag, index}} <- decode_key(pin_key),
         {:ok, {{^tag, ^index} = file_id, entries}} <- decode_batch_value(blob),
         true <- pin_key == batch_key(file_id, entries) do
      pins =
        Enum.map(entries, fn {value_key, expire_at_ms, offset, value_size} ->
          %{
            key: value_key,
            expire_at_ms: expire_at_ms,
            file_id: file_id,
            offset: offset,
            value_size: value_size,
            pin_key: pin_key
          }
        end)

      {:ok, %{file_id: file_id, pin_key: pin_key, pins: pins}}
    else
      _ -> {:error, {:corrupt_flow_segment_value_pin, pin_key}}
    end
  end

  defp decode_entry(_entry), do: :skip

  defp decode_key(pin_key) do
    Enum.find_value([:waraft_apply_projection, :waraft_segment], :error, fn tag ->
      prefix = prefix(tag)
      prefix_size = byte_size(prefix)

      with true <- byte_size(pin_key) == prefix_size + 20 + 1 + 32,
           ^prefix <- binary_part(pin_key, 0, prefix_size),
           digits <- binary_part(pin_key, prefix_size, 20),
           <<0>> <- binary_part(pin_key, prefix_size + 20, 1),
           {index, ""} <- Integer.parse(digits),
           true <- index > 0 and index <= @max_u64 do
        {:ok, {tag, index}}
      else
        _ -> false
      end
    end)
  end

  defp decode_batch_value(blob) do
    case TermCodec.decode(blob) do
      {:ok, {:flow_segment_value_pin_batch, 1, {tag, index} = file_id, entries}}
      when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and
             index > 0 and index <= @max_u64 and is_list(entries) ->
        if valid_batch_entries?(entries), do: {:ok, {file_id, entries}}, else: :error

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp valid_batch_entries?([]), do: false

  defp valid_batch_entries?(entries) do
    valid_batch_entries?(entries, 0)
  end

  defp valid_batch_entries?(_entries, count) when count > @max_batch_entries, do: false
  defp valid_batch_entries?([], _count), do: true

  defp valid_batch_entries?(
         [{value_key, expire_at_ms, offset, value_size} | rest],
         count
       )
       when is_binary(value_key) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
              expire_at_ms <= @max_u64 and is_integer(offset) and offset >= 0 and
              offset <= @max_u64 and is_integer(value_size) and value_size >= 0 and
              value_size <= @max_u64 do
    valid_batch_entries?(rest, count + 1)
  end

  defp valid_batch_entries?(_entries, _count), do: false

  defp pad_u64(value) do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end
end
