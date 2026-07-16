defmodule FerricstoreServer.ApplicationSupervisionTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  test "resource governors and listeners share a rest-for-one failure boundary" do
    supervisor_state = :sys.get_state(FerricstoreServer.Supervisor)
    assert elem(supervisor_state, 2) == :rest_for_one

    child_ids =
      FerricstoreServer.Supervisor
      |> Supervisor.which_children()
      |> Enum.map(fn {id, _pid, _type, _modules} -> id end)

    assert FerricstoreServer.Native.Admission in child_ids
    assert FerricstoreServer.Native.ResourceBudget in child_ids

    assert {:ranch_embedded_sup, FerricstoreServer.Native.Listener} in child_ids
  end
end
