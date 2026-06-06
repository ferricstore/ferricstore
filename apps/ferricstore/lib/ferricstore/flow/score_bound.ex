defmodule Ferricstore.Flow.ScoreBound do
  @moduledoc false

  def parse("-inf"), do: :neg_inf
  def parse("+inf"), do: :pos_inf

  def parse("(" <> rest) do
    case Float.parse(rest) do
      {score, ""} -> {:exclusive, score}
      _ -> {:error, "ERR min or max is not a float"}
    end
  end

  def parse(value) when is_binary(value) do
    case Float.parse(value) do
      {score, ""} -> {:inclusive, score}
      _ -> {:error, "ERR min or max is not a float"}
    end
  end
end
