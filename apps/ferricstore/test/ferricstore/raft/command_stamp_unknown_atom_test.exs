defmodule Ferricstore.Raft.CommandStampUnknownAtomTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.CommandStamp

  # A replicated command can carry user terms such as payload maps with atom
  # keys. After a restart the VM may not have those atoms yet; replaying the
  # node's own log must still decode them instead of failing recovery.
  test "decode_ttb accepts atoms this VM has not created yet" do
    placeholder = :command_stamp_placeholder_atom_0000000000
    {:ttb, encoded} = CommandStamp.to_ttb({:flow_create, %{placeholder => 1}})

    fresh_name =
      "command_stamp_unseen_" <>
        Base.encode16(:crypto.strong_rand_bytes(10), case: :lower)

    assert byte_size(fresh_name) == byte_size(Atom.to_string(placeholder))

    patched =
      :binary.replace(encoded, Atom.to_string(placeholder), fresh_name, [:global])

    assert {:ok, {{:flow_create, payload}, _metadata}} = CommandStamp.decode_ttb(patched)
    assert [{key, 1}] = Map.to_list(payload)
    assert Atom.to_string(key) == fresh_name
  end
end
