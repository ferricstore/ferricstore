defmodule Ferricstore.Commands.Bitfield do
  @moduledoc """
  BITFIELD and BITFIELD_RO, as Redis answers them: GET, SET and INCRBY on
  signed (`i1`..`i64`) and unsigned (`u1`..`u63`) fields at bit offsets
  (`#N` for N fields of the type), with OVERFLOW WRAP, SAT or FAIL for the
  writes after it.

  A read of fields is a plain read. A write reads the string, changes it and
  writes it back with its TTL, so it must apply with nothing in between: on
  the instance it runs as a compound command entry from cluster feature
  level 12 (older releases do not have it) and is refused below it; in the
  state machine (that entry, or MULTI) it runs on the store it is given.
  """

  import Bitwise

  alias Ferricstore.Commands.Bitmap.{Args, Destination}
  alias Ferricstore.Store.{Ops, ReadResult}

  @level 12
  @type_error "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not " <>
                "supported but i64 is."
  @offset_error "ERR bit offset is not an integer or out of range"
  @value_error "ERR value is not an integer or out of range"
  @min_i64 -9_223_372_036_854_775_808
  @max_i64 9_223_372_036_854_775_807

  @type field :: {:signed | :unsigned, pos_integer()}
  @type op ::
          {:get, field(), non_neg_integer()}
          | {:set | :incrby, field(), non_neg_integer(), integer(), :wrap | :sat | :fail}

  @spec handle(binary(), [binary()], term()) :: term()
  def handle(command, [key | args], store) when command in ["BITFIELD", "BITFIELD_RO"] do
    mode = if command == "BITFIELD_RO", do: :read, else: :write

    with {:ok, ops} <- parse(args, mode),
         :ok <- Destination.ensure_string_key(key, store) do
      if Enum.any?(ops, &(elem(&1, 0) != :get)),
        do: write(key, args, ops, store),
        else: read(key, ops, store)
    end
  end

  def handle(command, _args, _store),
    do: {:error, "ERR wrong number of arguments for '#{String.downcase(command)}' command"}

  @spec handle_ast(term(), term()) :: term()
  def handle_ast({:bitfield, key, args}, store), do: handle("BITFIELD", [key | args], store)

  # ---------------------------------------------------------------------------
  # Parsing

  @doc false
  @spec parse([binary()], :read | :write) :: {:ok, [op()]} | {:error, binary()}
  def parse(args, mode), do: parse(args, mode, :wrap, [])

  defp parse([], _mode, _overflow, ops), do: {:ok, Enum.reverse(ops)}

  defp parse([subcommand | rest], mode, overflow, ops) do
    case {String.upcase(subcommand), rest} do
      {"GET", [type, offset | rest]} ->
        with {:ok, field} <- field_type(type),
             {:ok, offset} <- offset(offset, field),
             do: parse(rest, mode, overflow, [{:get, field, offset} | ops])

      {name, _rest} when mode == :read and name in ["SET", "INCRBY", "OVERFLOW"] ->
        {:error, "ERR BITFIELD_RO only supports the GET subcommand"}

      {name, [type, offset, value | rest]} when name in ["SET", "INCRBY"] ->
        op = if name == "SET", do: :set, else: :incrby

        with {:ok, field} <- field_type(type),
             {:ok, offset} <- offset(offset, field),
             {:ok, value} <- value(value),
             do: parse(rest, mode, overflow, [{op, field, offset, value, overflow} | ops])

      {"OVERFLOW", [kind | rest]} ->
        case String.upcase(kind) do
          "WRAP" -> parse(rest, mode, :wrap, ops)
          "SAT" -> parse(rest, mode, :sat, ops)
          "FAIL" -> parse(rest, mode, :fail, ops)
          _other -> {:error, "ERR Invalid OVERFLOW type specified"}
        end

      _other ->
        {:error, "ERR syntax error"}
    end
  end

  defp field_type(<<sign, bits::binary>>) when sign in [?i, ?I, ?u, ?U] do
    signed? = sign in [?i, ?I]

    case Integer.parse(bits) do
      {bits, ""} when signed? and bits in 1..64 -> {:ok, {:signed, bits}}
      {bits, ""} when not signed? and bits in 1..63 -> {:ok, {:unsigned, bits}}
      _invalid -> {:error, @type_error}
    end
  end

  defp field_type(_type), do: {:error, @type_error}

  defp offset("#" <> index, {_sign, bits}) do
    case Integer.parse(index) do
      {index, ""} when index >= 0 -> checked_offset(index * bits)
      _invalid -> {:error, @offset_error}
    end
  end

  defp offset(raw, _field) do
    case Integer.parse(raw) do
      {offset, ""} when offset >= 0 -> checked_offset(offset)
      _invalid -> {:error, @offset_error}
    end
  end

  defp checked_offset(offset) do
    with :ok <- Args.check_bit_offset(offset), do: {:ok, offset}
  end

  defp value(raw) do
    case Integer.parse(raw) do
      {value, ""} when value >= @min_i64 and value <= @max_i64 -> {:ok, value}
      _invalid -> {:error, @value_error}
    end
  end

  # ---------------------------------------------------------------------------
  # Running

  defp read(key, ops, store) do
    with {:ok, value, _expire_at_ms} <- current(key, store) do
      {_value, replies, _changed?} = run(value, ops)
      replies
    end
  end

  defp write(key, args, _ops, %FerricStore.Instance{} = ctx) do
    case Ops.compound_command(ctx, key, {:bitfield, key, args}, @level) do
      :unsupported ->
        {:error,
         "ERR BITFIELD with SET or INCRBY needs cluster feature level #{@level} " <>
           "(CLUSTER.FEATURES.RAISE #{@level} once every node runs this release)"}

      answer ->
        answer
    end
  end

  defp write(key, _args, ops, store) do
    with {:ok, value, expire_at_ms} <- current(key, store) do
      case run(value, ops) do
        {_value, replies, false} ->
          replies

        {new_value, replies, true} ->
          with :ok <- check_size(new_value, store),
               :ok <- Ops.put(store, key, new_value, expire_at_ms),
               do: replies
      end
    end
  end

  defp current(key, store) do
    case Ops.get_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
      nil -> {:ok, <<>>, 0}
      {value, expire_at_ms} when is_binary(value) -> {:ok, value, expire_at_ms}
    end
  end

  defp check_size(value, %{max_value_size: limit}) when is_integer(limit) and limit > 0 do
    if byte_size(value) > limit,
      do: {:error, "ERR value too large (#{byte_size(value)} bytes, max #{limit} bytes)"},
      else: :ok
  end

  defp check_size(_value, _store), do: :ok

  @doc false
  # Applies `ops` to `value` in order: the new value, one reply per GET, SET
  # or INCRBY, and whether the value changed (a write past the end extends
  # it with zero bytes, as Redis does).
  @spec run(binary(), [op()]) :: {binary(), [integer() | nil], boolean()}
  def run(value, ops) do
    {new_value, replies} =
      Enum.reduce(ops, {value, []}, fn op, {value, replies} ->
        {value, reply} = apply_op(value, op)
        {value, [reply | replies]}
      end)

    {new_value, Enum.reverse(replies), new_value != value}
  end

  defp apply_op(value, {:get, field, offset}), do: {value, get(value, field, offset)}

  defp apply_op(value, {op, {sign, bits} = field, offset, operand, overflow}) do
    old = get(value, field, offset)

    {target, reply} =
      if op == :set, do: {set_target(sign, operand), old}, else: {old + operand, nil}

    case fit(sign, bits, target, overflow) do
      :fail ->
        {value, nil}

      {:ok, stored} ->
        reply = if op == :set, do: reply, else: stored
        {put(value, field, offset, stored), reply}
    end
  end

  # An unsigned SET reads its value as a 64-bit unsigned, as Redis does:
  # -1 is 2^64 - 1, past every field's maximum.
  defp set_target(:unsigned, operand) when operand < 0, do: operand + (1 <<< 64)
  defp set_target(_sign, operand), do: operand

  # Redis's checkSignedBitfieldOverflow / checkUnsignedBitfieldOverflow, on
  # exact integers: WRAP keeps the low bits (two's complement for signed
  # fields), SAT clamps to the field's range, FAIL writes nothing.
  defp fit(sign, bits, target, overflow) do
    {min, max} =
      case sign do
        :signed -> {-(1 <<< (bits - 1)), (1 <<< (bits - 1)) - 1}
        :unsigned -> {0, (1 <<< bits) - 1}
      end

    cond do
      target >= min and target <= max -> {:ok, target}
      overflow == :fail -> :fail
      overflow == :sat and target > max -> {:ok, max}
      overflow == :sat -> {:ok, min}
      true -> {:ok, Integer.mod(target - min, 1 <<< bits) + min}
    end
  end

  defp get(value, {sign, bits}, offset) do
    padded = pad(value, offset + bits)
    <<_::bitstring-size(^offset), field::bitstring-size(^bits), _::bitstring>> = padded

    case sign do
      :signed ->
        <<signed::signed-size(^bits)>> = field
        signed

      :unsigned ->
        <<unsigned::unsigned-size(^bits)>> = field
        unsigned
    end
  end

  defp put(value, {_sign, bits}, offset, stored) do
    padded = pad(value, offset + bits)
    <<prefix::bitstring-size(^offset), _old::bitstring-size(^bits), rest::bitstring>> = padded
    <<prefix::bitstring, stored::size(bits), rest::bitstring>>
  end

  # Zero bytes up to the byte holding bit `bit_end - 1`.
  defp pad(value, bit_end) do
    needed = div(bit_end + 7, 8)

    if byte_size(value) >= needed,
      do: value,
      else: value <> :binary.copy(<<0>>, needed - byte_size(value))
  end
end
