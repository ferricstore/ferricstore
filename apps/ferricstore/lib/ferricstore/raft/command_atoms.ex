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
            preload_literal_atoms!(module)

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
  # accepted options, say) in the literal table, not the atom table. Loading
  # the module creates those atoms too, as a release does at boot.
  defp preload_literal_atoms!(module) do
    case :code.ensure_loaded(module) do
      {:module, ^module} -> :ok
      {:error, reason} -> raise "cannot load #{inspect(module)} literals: #{inspect(reason)}"
    end
  end
end
