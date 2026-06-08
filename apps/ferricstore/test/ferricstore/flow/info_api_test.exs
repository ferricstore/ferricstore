defmodule Ferricstore.Flow.InfoAPITest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.InfoAPI

  test "info rejects non-keyword opts" do
    assert InfoAPI.info(:ctx, "type", ["bad"]) == {:error, "ERR flow opts must be a keyword list"}
  end

  test "info rejects invalid type" do
    assert InfoAPI.info(:ctx, "", []) == {:error, "ERR flow type must be a non-empty string"}
  end
end
