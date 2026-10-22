defmodule Ferricstore.Raft.CommandAtoms do
  @moduledoc false

  @spec preload!() :: :ok
  def preload! do
    :ok = ensure_application_loaded()

    case Application.spec(:ferricstore, :modules) do
      modules when is_list(modules) ->
        Enum.each(modules, &preload_module_atoms!/1)
        :ok

      _missing ->
        raise "ferricstore application module catalog is unavailable"
    end
  end

  defp ensure_application_loaded do
    case Application.load(:ferricstore) do
      :ok -> :ok
      {:error, {:already_loaded, :ferricstore}} -> :ok
      {:error, reason} -> raise "cannot load ferricstore application catalog: #{inspect(reason)}"
    end
  end

  defp preload_module_atoms!(module) do
    case :code.which(module) do
      path when is_list(path) ->
        case :beam_lib.chunks(path, [:atoms]) do
          {:ok, {^module, [atoms: atoms]}} when is_list(atoms) ->
            preload_literal_atoms!(module, path)

          {:error, _module, reason} ->
            raise "cannot read #{inspect(module)} atoms: #{inspect(reason)}"

          other ->
            raise "invalid #{inspect(module)} atom table: #{inspect(other)}"
        end

      location ->
        raise "cannot locate #{inspect(module)} bytecode: #{inspect(location)}"
    end
  end

  # The compiler keeps an atom used only inside a literal term (a list of
  # accepted options, say) in the literal table, not the atom table. Decoding
  # the module's own literals creates those atoms too.
  defp preload_literal_atoms!(module, path) do
    case :beam_lib.chunks(path, [~c"LitT"]) do
      {:ok, {^module, [{~c"LitT", chunk}]}} ->
        chunk |> literal_table() |> decode_literals!()

      {:error, :beam_lib, {:missing_chunk, _path, _chunk}} ->
        :ok

      {:error, _module, reason} ->
        raise "cannot read #{inspect(module)} literals: #{inspect(reason)}"
    end
  end

  # Older compilers compress the table; its size field is 0 when it is not.
  defp literal_table(<<0::32, table::binary>>), do: table
  defp literal_table(<<_size::32, compressed::binary>>), do: :zlib.uncompress(compressed)

  defp decode_literals!(<<_count::32, literals::binary>>), do: decode_each_literal!(literals)

  defp decode_each_literal!(<<>>), do: :ok

  defp decode_each_literal!(<<size::32, literal::binary-size(size), rest::binary>>) do
    _ = :erlang.binary_to_term(literal)
    decode_each_literal!(rest)
  end
end
