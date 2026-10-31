defmodule Ferricstore.Commands.Stream.GroupArgs do
  @moduledoc false

  # Arguments of XCLAIM, XAUTOCLAIM, XGROUP SETID and XSETID, parsed and
  # answered as Redis answers them, into the replicated operations
  # `Stream.GroupWrites` and `Stream.AtomicMutation` apply.

  alias Ferricstore.Commands.Stream.ID

  @autoclaim_default_count 100

  # XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms]
  # [TIME unix-ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]
  @spec xclaim(binary(), binary(), binary(), [binary()]) :: {:ok, tuple()} | {:error, binary()}
  def xclaim(group, consumer, min_idle, args) do
    with {:ok, min_idle} <- min_idle(min_idle, "XCLAIM") do
      {ids, options} = Enum.split_while(args, &match?({:ok, _id}, ID.parse_full_id(&1)))

      opts = %{idle: nil, time: nil, retrycount: nil, force: false, justid: false, lastid: nil}

      with {:ok, opts} <- xclaim_options(options, opts) do
        {:ok, {:claim, group, consumer, min_idle, Enum.map(ids, &ID.canonical/1), opts}}
      end
    end
  end

  defp xclaim_options([], opts), do: {:ok, opts}

  defp xclaim_options([option | rest] = args, opts) do
    case {String.upcase(option), rest} do
      {"FORCE", rest} ->
        xclaim_options(rest, %{opts | force: true})

      {"JUSTID", rest} ->
        xclaim_options(rest, %{opts | justid: true})

      {name, [value | rest]} when name in ["IDLE", "TIME", "RETRYCOUNT"] ->
        case Integer.parse(value) do
          {integer, ""} ->
            field = name |> String.downcase() |> String.to_existing_atom()
            xclaim_options(rest, Map.put(opts, field, integer))

          _ ->
            {:error, "ERR Invalid #{name} option argument for XCLAIM"}
        end

      {"LASTID", [value | rest]} ->
        case ID.parse_full_id(value) do
          {:ok, _id} -> xclaim_options(rest, %{opts | lastid: ID.canonical(value)})
          {:error, _message} = error -> error
        end

      _unknown ->
        {:error, "ERR Unrecognized XCLAIM option '#{hd(args)}'"}
    end
  end

  # XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
  @spec xautoclaim(binary(), binary(), binary(), binary(), [binary()]) ::
          {:ok, tuple()} | {:error, binary()}
  def xautoclaim(group, consumer, min_idle, start, args) do
    with {:ok, min_idle} <- min_idle(min_idle, "XAUTOCLAIM"),
         {:ok, start} <- ID.parse_range_id(start, :min),
         {:ok, count, justid} <- xautoclaim_options(args, @autoclaim_default_count, false) do
      {:ok, {:autoclaim, group, consumer, min_idle, start, count, justid}}
    end
  end

  defp xautoclaim_options([], count, justid), do: {:ok, count, justid}

  defp xautoclaim_options([option | rest], count, justid) do
    case {String.upcase(option), rest} do
      {"JUSTID", rest} ->
        xautoclaim_options(rest, count, true)

      {"COUNT", [value | rest]} ->
        case Integer.parse(value) do
          {count, ""} when count > 0 -> xautoclaim_options(rest, count, justid)
          {_count, ""} -> {:error, "ERR COUNT must be > 0"}
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _unknown ->
        {:error, "ERR syntax error"}
    end
  end

  # XGROUP SETID key group id|$ [ENTRIESREAD entries-read]
  @spec setid_options([binary()]) :: :ok | {:error, binary()}
  def setid_options([]), do: :ok

  def setid_options([option, value]) do
    with true <- String.upcase(option) == "ENTRIESREAD",
         {_entries_read, ""} <- Integer.parse(value) do
      :ok
    else
      false -> {:error, "ERR syntax error"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def setid_options(_options), do: {:error, "ERR syntax error"}

  # XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
  @spec xsetid(binary(), [binary()]) :: {:ok, tuple()} | {:error, binary()}
  def xsetid(id_str, options) do
    with {:ok, {ms, seq}} <- ID.parse_full_id(id_str),
         :ok <- xsetid_options(options) do
      {:ok, {:set_last_id, ms, seq}}
    end
  end

  defp xsetid_options([]), do: :ok

  defp xsetid_options([option, value | rest]) do
    case String.upcase(option) do
      "ENTRIESADDED" ->
        case Integer.parse(value) do
          {added, ""} when added >= 0 -> xsetid_options(rest)
          _ -> {:error, "ERR entries_added must be positive"}
        end

      "MAXDELETEDID" ->
        with {:ok, _id} <- ID.parse_full_id(value), do: xsetid_options(rest)

      _unknown ->
        {:error, "ERR syntax error"}
    end
  end

  defp xsetid_options(_options), do: {:error, "ERR syntax error"}

  defp min_idle(value, command) do
    case Integer.parse(value) do
      {min_idle, ""} -> {:ok, max(min_idle, 0)}
      _ -> {:error, "ERR Invalid min-idle-time argument for #{command}"}
    end
  end
end
