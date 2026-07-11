defmodule Ferricstore.Raft.StateMachineTest.Sections.PromotedCompoundMemberIndex do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.{CompoundKey, Promotion}
      alias Ferricstore.Store.Shard.CompoundMemberIndex

      @tag :promoted_compound_member_index
      test "promoted compound mutations keep the member index exact", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-member-index"
        prefix = CompoundKey.set_prefix(redis_key)
        member_a = CompoundKey.set_member(redis_key, "a")
        member_b = CompoundKey.set_member(redis_key, "b")
        member_c = CompoundKey.set_member(redis_key, "c")
        index = state.compound_member_index_name

        dedicated_path =
          Promotion.dedicated_path(state.data_dir, shard_index, :set, redis_key)

        File.mkdir_p!(dedicated_path)
        File.touch!(Path.join(dedicated_path, "00000.log"))
        CompoundMemberIndex.ensure_table!(index)

        on_exit(fn -> safe_delete_ets(index) end)

        {state, :ok} =
          StateMachine.apply(%{}, {:compound_put, member_a, "1", 0}, state)

        {state, {:ok, [:ok, :ok]}} =
          StateMachine.apply(
            %{},
            {:compound_batch_put, redis_key, [{member_b, "1", 0}, {member_c, "1", 0}]},
            state
          )

        assert indexed_members(index, ets, prefix) == ["a", "b", "c"]

        {state, :ok} = StateMachine.apply(%{}, {:compound_delete, member_a}, state)

        {state, {:ok, [:ok]}} =
          StateMachine.apply(%{}, {:compound_batch_delete, redis_key, [member_b]}, state)

        assert indexed_members(index, ets, prefix) == ["c"]

        {_state, :ok} =
          StateMachine.apply(%{}, {:compound_delete_prefix, prefix}, state)

        assert indexed_members(index, ets, prefix) == []
      end

      defp indexed_members(index, keydir, prefix) do
        {:ok, entries} =
          CompoundMemberIndex.scan_entries(index, %{keydir: keydir}, prefix)

        entries
        |> Enum.map(&elem(&1, 0))
        |> Enum.sort()
      end
    end
  end
end
