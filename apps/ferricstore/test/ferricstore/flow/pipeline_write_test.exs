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
