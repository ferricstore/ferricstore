defmodule Ferricstore.Commands.TransactionPolicy do
  @moduledoc false

  @local_no_key_commands MapSet.new(~w(PING ECHO CLUSTER.KEYSLOT))

  # These handlers depend on request/process state, perform effects outside the
  # supplied store, block, or use node-local randomness/runtime configuration.
  @request_commands MapSet.new(~w(
    AUTH HELLO CLIENT QUIT RESET MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH PUBSUB
    BLPOP BRPOP BLMOVE BLMPOP
    XADD XLEN XRANGE XREVRANGE XREAD XTRIM XDEL XINFO XGROUP XREADGROUP XACK
    KEY_INFO FERRICSTORE.KEY_INFO
    CAS LOCK UNLOCK EXTEND RATELIMIT.ADD
    FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR
    HRANDFIELD SRANDMEMBER SPOP ZRANDMEMBER RANDOMKEY
    ROUTE ROUTE_BATCH
  ))

  @type mode :: :local | :request

  @spec mode(binary(), term(), :none | :keys | :coordinated) :: mode()
  def mode(_command, _ast, :coordinated), do: :request

  def mode(
        _command,
        {:extension_command, _module, _name, _args, _access},
        _routing_scope
      ),
      do: :request

  def mode(command, _ast, routing_scope) when is_binary(command) do
    cond do
      request_command?(command) ->
        :request

      routing_scope == :keys ->
        :local

      routing_scope == :none and MapSet.member?(@local_no_key_commands, command) ->
        :local

      true ->
        :request
    end
  end

  def mode(_command, _ast, _routing_scope), do: :request

  defp request_command?("BF." <> _rest), do: true
  defp request_command?("CF." <> _rest), do: true
  defp request_command?("CMS." <> _rest), do: true
  defp request_command?("TOPK." <> _rest), do: true
  defp request_command?("TDIGEST." <> _rest), do: true
  defp request_command?("FLOW." <> _rest), do: true
  defp request_command?(command), do: MapSet.member?(@request_commands, command)

  @spec safe?(binary(), term(), :none | :keys | :coordinated) :: boolean()
  def safe?(command, ast, routing_scope), do: mode(command, ast, routing_scope) == :local

  @spec error(binary()) :: {:error, binary()}
  def error(command) when is_binary(command) do
    {:error, "ERR command '#{String.downcase(command)}' is not supported inside transactions"}
  end
end
