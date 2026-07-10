defmodule Ferricstore.Commands.KeyDiscovery do
  @moduledoc false

  @type result :: {:ok, [binary()]} | :not_dynamic

  @spec extract(binary(), [binary()]) :: result()
  def extract(command, args)

  def extract(command, [destination, count | rest])
      when command in ["CMS.MERGE", "TDIGEST.MERGE"] do
    {:ok, [destination | counted_keys(count, rest)]}
  end

  def extract(command, [destination | _args])
      when command in ["CMS.MERGE", "TDIGEST.MERGE"],
      do: {:ok, [destination]}

  def extract(command, []) when command in ["CMS.MERGE", "TDIGEST.MERGE"], do: {:ok, []}

  def extract("SINTERCARD", [count | rest]), do: {:ok, counted_keys(count, rest)}
  def extract("SINTERCARD", []), do: {:ok, []}
  def extract("PFCOUNT", keys), do: {:ok, keys}

  def extract("OBJECT", [subcommand, key | _args]) do
    if String.upcase(subcommand) in ["ENCODING", "FREQ", "IDLETIME", "REFCOUNT"] do
      {:ok, [key]}
    else
      {:ok, []}
    end
  end

  def extract("OBJECT", _args), do: {:ok, []}

  def extract("MEMORY", [subcommand, key | _args]) do
    if String.upcase(subcommand) == "USAGE", do: {:ok, [key]}, else: {:ok, []}
  end

  def extract("MEMORY", _args), do: {:ok, []}

  def extract("XINFO", [subcommand, key | _args]) do
    if String.upcase(subcommand) in ["STREAM", "GROUPS", "CONSUMERS"] do
      {:ok, [key]}
    else
      {:ok, []}
    end
  end

  def extract("XINFO", _args), do: {:ok, []}

  def extract("XGROUP", [subcommand, key | _args]) do
    if String.upcase(subcommand) in [
         "CREATE",
         "SETID",
         "DESTROY",
         "CREATECONSUMER",
         "DELCONSUMER"
       ] do
      {:ok, [key]}
    else
      {:ok, []}
    end
  end

  def extract("XGROUP", _args), do: {:ok, []}
  def extract(_command, _args), do: :not_dynamic

  defp counted_keys(count, keys) do
    case Integer.parse(count) do
      {parsed, ""} when parsed >= 0 -> Enum.take(keys, parsed)
      _invalid -> []
    end
  end
end
