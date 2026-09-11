defmodule FerricstoreHttp.CommandErrorTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.CommandError

  test "preserves only bounded structured diagnostic fields" do
    diagnostic = %{
      "code" => "invalid_syntax",
      "message" => "ERR FQL1 invalid syntax",
      "detail" => "Expected a field name.",
      "hint" => "Remove the trailing comma.",
      "position" => %{"byte" => 12, "line" => 1, "column" => 13},
      "context" => %{"token" => ",", "expected" => ["identifier", nil]},
      "retryable" => false,
      "safe_to_retry" => false,
      "retry_after_ms" => 0,
      "private_backend_state" => "must not cross the HTTP boundary"
    }

    assert CommandError.normalize(:bad_request, diagnostic) ==
             Map.drop(diagnostic, ["private_backend_state"])
  end

  test "drops malformed optional diagnostic fields without losing the public error" do
    oversized_context =
      for index <- 1..17, into: %{}, do: {"field-#{index}", index}

    diagnostic = %{
      "code" => "invalid_syntax",
      "message" => "ERR FQL1 invalid syntax",
      "position" => %{"byte" => 1, "line" => 1, "column" => 1, "extra" => true},
      "context" => oversized_context,
      "retryable" => "yes",
      "safe_to_retry" => nil,
      "retry_after_ms" => -1
    }

    assert CommandError.normalize(:bad_request, diagnostic) == %{
             "code" => "invalid_syntax",
             "message" => "ERR FQL1 invalid syntax"
           }
  end

  test "only emits bounded UTF-8 text and JSON-safe context" do
    invalid_utf8 = <<0xFF>>

    diagnostic = %{
      "code" => "invalid_syntax",
      "message" => "ERR FQL1 invalid syntax",
      "detail" => invalid_utf8,
      "hint" => String.duplicate("x", 1_025),
      "context" => %{"invalid" => invalid_utf8}
    }

    assert CommandError.normalize(:bad_request, diagnostic) == %{
             "code" => "invalid_syntax",
             "message" => "ERR FQL1 invalid syntax"
           }

    assert CommandError.normalize(:error, String.duplicate("x", 1_025)) == %{
             "code" => "error",
             "message" => "FerricStore command failed"
           }

    assert CommandError.normalize(:error, invalid_utf8) == %{
             "code" => "error",
             "message" => "FerricStore command failed"
           }
  end

  test "redacts opaque backend maps" do
    assert CommandError.normalize(:error, %{password: "secret", reason: :failure}) == %{
             "code" => "error",
             "message" => "FerricStore command failed"
           }
  end

  test "enforces context shape, item, node, and scalar bounds" do
    public_error = %{"code" => "query_failed", "message" => "ERR query failed"}

    assert CommandError.normalize(:error, public_error) == public_error

    assert CommandError.normalize(:error, Map.put(public_error, "context", %{"count" => 1})) ==
             Map.put(public_error, "context", %{"count" => 1})

    for invalid_context <- [
          %{"items" => Enum.to_list(1..33)},
          %{"float" => 1.5},
          %{"improper" => [1 | :tail]},
          %{"nodes" => List.duplicate(%{"items" => List.duplicate(true, 32)}, 32)}
        ] do
      assert CommandError.normalize(:error, Map.put(public_error, "context", invalid_context)) ==
               public_error
    end

    assert CommandError.normalize(:error, :authentication_unavailable) == %{
             "code" => "error",
             "message" => "authentication_unavailable"
           }
  end
end
