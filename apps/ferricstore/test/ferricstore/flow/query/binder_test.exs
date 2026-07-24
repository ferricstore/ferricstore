defmodule Ferricstore.Flow.Query.BinderTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Binder, Request}

  test "binds every value in a bounded collection request with exact types" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, parameter(:keyword, "tenant")},
          {:in, :state,
           [parameter(:keyword, "first_state"), parameter(:keyword, "second_state")]},
          {:time_window, :updated_at_ms, parameter(:integer, "from"),
           parameter(:integer, "until")},
          {:eq, {:attribute, "attempt_group"}, parameter(:dynamic, "group")}
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok, bound} =
             Binder.bind(request, %{
               "tenant" => "tenant-a",
               "first_state" => "failed",
               "second_state" => "completed",
               "from" => 100,
               "until" => 200,
               "group" => 3
             })

    assert bound.predicate ==
             {:and,
              [
                {:eq, :partition_key, literal(:keyword, "tenant-a")},
                {:in, :state, [literal(:keyword, "failed"), literal(:keyword, "completed")]},
                {:time_window, :updated_at_ms, literal(:integer, 100), literal(:integer, 200)},
                {:eq, {:attribute, "attempt_group"}, literal(:integer, 3)}
              ]}

    assert :ok = Request.validate_bound(bound)
  end

  test "preserves the validated return projection while binding values" do
    request =
      Request.collection(
        :execute,
        [{:eq, :partition_key, parameter(:keyword, "tenant")}],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )
      |> Map.put(:projection, [:run_id, :state, {:attribute, "customer"}])

    assert {:ok, bound} = Binder.bind(request, %{"tenant" => "tenant-a"})
    assert bound.projection == request.projection
    assert :ok = Request.validate_bound(bound)
  end

  test "binds repeated parameters once and rejects missing, unused, or mistyped values" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, parameter(:keyword, "tenant")},
          {:range, :updated_at_ms, parameter(:integer, "edge"), parameter(:integer, "edge")}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:ok, _bound} = Binder.bind(request, %{"tenant" => "tenant-a", "edge" => 100})
    assert {:error, :missing_parameter} = Binder.bind(request, %{"tenant" => "tenant-a"})

    assert {:error, :unexpected_parameter} =
             Binder.bind(request, %{"tenant" => "tenant-a", "edge" => 100, "secret" => "x"})

    assert {:error, :invalid_parameter_type} =
             Binder.bind(request, %{"tenant" => "tenant-a", "edge" => "100"})
  end

  test "ordinary dynamic comparisons cannot bind null or unsupported compound values" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, literal(:keyword, "tenant-a")},
          {:eq, {:attribute, "region"}, parameter(:dynamic, "region")}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:error, :invalid_parameter_type} = Binder.bind(request, %{"region" => nil})
    assert {:error, :invalid_parameter_type} = Binder.bind(request, %{"region" => %{}})
    assert {:error, :unexpected_parameter} = Binder.bind(request, %{"region" => "eu", 1 => "x"})
  end

  test "text binding decodes only the type declared by the query field" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, parameter(:keyword, "tenant")},
          {:range, :updated_at_ms, parameter(:integer, "from"), parameter(:integer, "until")},
          {:eq, {:attribute, "group"}, parameter(:dynamic, "group")}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:ok, bound} =
             Binder.bind_text(request, %{
               "tenant" => "tenant-a",
               "from" => "-9223372036854775808",
               "until" => "9223372036854775807",
               "group" => "42"
             })

    assert {:and,
            [
              {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
              {:range, :updated_at_ms, {:literal, :integer, -9_223_372_036_854_775_808},
               {:literal, :integer, 9_223_372_036_854_775_807}},
              {:eq, {:attribute, "group"}, {:literal, :keyword, "42"}}
            ]} = bound.predicate

    assert {:error, :invalid_parameter_type} =
             Binder.bind_text(request, %{
               "tenant" => "tenant-a",
               "from" => String.duplicate("9", 8_000),
               "until" => "10",
               "group" => "42"
             })
  end

  test "binds the cursor as an exact bounded keyword parameter" do
    request =
      Request.collection(
        :execute,
        [{:eq, :partition_key, parameter(:keyword, "tenant")}],
        [{:updated_at_ms, :asc}],
        10,
        :record,
        parameter(:keyword, "cursor")
      )

    token = String.duplicate("a", 128)

    assert {:ok, %Request{cursor: {:literal, :keyword, ^token}} = bound} =
             Binder.bind(request, %{"tenant" => "tenant-a", "cursor" => token})

    assert :ok = Request.validate_bound(bound)
    assert {:error, :missing_parameter} = Binder.bind(request, %{"tenant" => "tenant-a"})

    assert {:error, :unexpected_parameter} =
             request
             |> Map.put(:cursor, nil)
             |> Binder.bind(%{"tenant" => "tenant-a", "cursor" => token})

    assert {:error, :query_cursor_too_large} =
             Binder.bind(request, %{
               "tenant" => "tenant-a",
               "cursor" => String.duplicate("a", 4_097)
             })
  end

  defp parameter(type, name), do: {:parameter, type, name}
  defp literal(type, value), do: {:literal, type, value}
end
