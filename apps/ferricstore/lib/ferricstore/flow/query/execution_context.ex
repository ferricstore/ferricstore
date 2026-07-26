defmodule Ferricstore.Flow.Query.ExecutionContext do
  @moduledoc false

  @enforce_keys [:instance_ctx]
  defstruct [:instance_ctx, :deadline_ms, request_context: %{}, response_codec: :native_value]

  @type response_codec :: :native_value | :flow_query_result_v1

  @type t :: %__MODULE__{
          instance_ctx: FerricStore.Instance.t() | map(),
          deadline_ms: pos_integer() | nil,
          request_context: map(),
          response_codec: response_codec()
        }

  @spec attach(FerricStore.Instance.t(), map()) :: FerricStore.Instance.t() | t()
  def attach(%FerricStore.Instance{} = instance_ctx, request_context),
    do: attach(instance_ctx, request_context, nil)

  @spec attach(FerricStore.Instance.t(), map(), non_neg_integer() | nil) ::
          FerricStore.Instance.t() | t()
  def attach(%FerricStore.Instance{} = instance_ctx, request_context, deadline_ms),
    do: attach(instance_ctx, request_context, deadline_ms, :native_value)

  @spec attach(FerricStore.Instance.t(), map(), non_neg_integer() | nil, response_codec()) ::
          FerricStore.Instance.t() | t()
  def attach(
        %FerricStore.Instance{} = instance_ctx,
        request_context,
        deadline_ms,
        :native_value
      )
      when is_map(request_context) and map_size(request_context) == 0 and
             deadline_ms in [nil, 0],
      do: instance_ctx

  def attach(%FerricStore.Instance{} = instance_ctx, request_context, deadline_ms, response_codec)
      when is_map(request_context) and
             response_codec in [:native_value, :flow_query_result_v1] and
             (is_nil(deadline_ms) or
                (is_integer(deadline_ms) and deadline_ms >= 0)) do
    %__MODULE__{
      instance_ctx: instance_ctx,
      request_context: request_context,
      deadline_ms: normalize_deadline(deadline_ms),
      response_codec: response_codec
    }
  end

  @spec instance_ctx(term()) :: term()
  def instance_ctx(%__MODULE__{instance_ctx: instance_ctx}), do: instance_ctx
  def instance_ctx(ctx), do: ctx

  defp normalize_deadline(deadline_ms) when is_integer(deadline_ms) and deadline_ms > 0,
    do: deadline_ms

  defp normalize_deadline(_deadline_ms), do: nil
end
