defmodule Ferricstore.Store.RateLimit do
  @moduledoc false

  @spec effective_count(
          non_neg_integer(),
          non_neg_integer(),
          integer(),
          pos_integer()
        ) :: non_neg_integer()
  def effective_count(current_count, previous_count, elapsed_ms, window_ms)
      when is_integer(current_count) and current_count >= 0 and is_integer(previous_count) and
             previous_count >= 0 and is_integer(elapsed_ms) and is_integer(window_ms) and
             window_ms > 0 do
    remaining_ms = window_ms - min(max(elapsed_ms, 0), window_ms)

    weighted_previous =
      div(previous_count * remaining_ms + div(window_ms, 2), window_ms)

    current_count + weighted_previous
  end
end
