defmodule Ferricstore.Flow.PipelineWriteTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineWrite

  test "create_attrs_from_commands accepts unique flow_create commands" do
    attrs_a = %{id: "a"}
    attrs_b = %{id: "b"}

    assert PipelineWrite.create_attrs_from_commands([
             {"key-a", {:flow_create, "state-a", attrs_a}},
             {"key-b", {:flow_create, "state-b", attrs_b}}
           ]) == {:ok, [attrs_a, attrs_b]}
  end

  test "create_attrs_from_commands rejects duplicate keys and mixed commands" do
    attrs = %{id: "a"}

    assert PipelineWrite.create_attrs_from_commands([
             {"key-a", {:flow_create, "state-a", attrs}},
             {"key-a", {:flow_create, "state-a", attrs}}
           ]) == :generic

    assert PipelineWrite.create_attrs_from_commands([
             {"key-a", {:flow_transition, "state-a", attrs}}
           ]) == :generic
  end

  test "start_and_claim_attrs_from_commands accepts unique flow_start_and_claim commands" do
    attrs_a = %{id: "a"}
    attrs_b = %{id: "b"}

    assert PipelineWrite.start_and_claim_attrs_from_commands([
             {"key-a", {:flow_start_and_claim, "state-a", attrs_a}},
             {"key-b", {:flow_start_and_claim, "state-b", attrs_b}}
           ]) == {:ok, [attrs_a, attrs_b]}
  end

  test "start_and_claim_attrs_from_commands rejects duplicate keys and mixed commands" do
    attrs = %{id: "a"}

    assert PipelineWrite.start_and_claim_attrs_from_commands([
             {"key-a", {:flow_start_and_claim, "state-a", attrs}},
             {"key-a", {:flow_start_and_claim, "state-a", attrs}}
           ]) == :generic

    assert PipelineWrite.start_and_claim_attrs_from_commands([
             {"key-a", {:flow_transition, "state-a", attrs}}
           ]) == :generic
  end

  test "named_value_put_attrs_from_commands accepts unique owned value put commands" do
    attrs_a = %{id: "a", name: "reservation"}
    attrs_b = %{id: "b", name: "reservation"}

    assert PipelineWrite.named_value_put_attrs_from_commands([
             {"key-a", {:flow_named_value_put, "state-a", attrs_a}},
             {"key-b", {:flow_named_value_put, "state-b", attrs_b}}
           ]) == {:ok, [attrs_a, attrs_b]}
  end

  test "named_value_put_attrs_from_commands preserves success-only return mode" do
    attrs = %{id: "a", name: "reservation", return: :ok_on_success}

    assert PipelineWrite.named_value_put_attrs_from_commands([
             {"key-a", {:flow_named_value_put, "state-a", attrs}}
           ]) == {:ok, [attrs]}
  end

  test "named_value_put_attrs_from_commands rejects duplicate keys and mixed commands" do
    attrs = %{id: "a", name: "reservation"}

    assert PipelineWrite.named_value_put_attrs_from_commands([
             {"key-a", {:flow_named_value_put, "state-a", attrs}},
             {"key-a", {:flow_named_value_put, "state-a", attrs}}
           ]) == :generic

    assert PipelineWrite.named_value_put_attrs_from_commands([
             {"key-a", {:flow_transition, "state-a", attrs}}
           ]) == :generic
  end

  test "transition_attrs_from_commands accepts only transition commands" do
    attrs_a = %{id: "a"}
    attrs_b = %{id: "b"}

    assert PipelineWrite.transition_attrs_from_commands([
             {"key-a", {:flow_transition, "state-a", attrs_a}},
             {"key-b", {:flow_transition, "state-b", attrs_b}}
           ]) == {:ok, [attrs_a, attrs_b]}

    assert PipelineWrite.transition_attrs_from_commands([
             {"key-a", {:flow_create, "state-a", attrs_a}}
           ]) == :generic
  end
end
