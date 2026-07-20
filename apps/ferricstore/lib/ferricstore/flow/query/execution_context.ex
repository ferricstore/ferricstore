defmodule Ferricstore.Flow.Query.ExecutionContext do
  @moduledoc false

  @enforce_keys [:instance_ctx]
  defstruct [:instance_ctx, :deadline_ms, request_context: %{}]

  @type t :: %__MODULE__{
          instance_ctx: FerricStore.Instance.t(),
          deadline_ms: pos_integer() | nil,
          request_context: map()
        }

  @spec attach(FerricStore.Instance.t(), map()) :: FerricStore.Instance.t() | t()
  def attach(%FerricStore.Instance{} = instance_ctx, request_context),
    do: attach(instance_ctx, request_context, nil)

  @spec attach(FerricStore.Instance.t(), map(), non_neg_integer() | nil) ::
          FerricStore.Instance.t() | t()
  def attach(%FerricStore.Instance{} = instance_ctx, request_context, deadline_ms)
      when is_map(request_context) and map_size(request_context) == 0 and
             deadline_ms in [nil, 0],
      do: instance_ctx

  def attach(%FerricStore.Instance{} = instance_ctx, request_context, deadline_ms)
      when is_map(request_context) and
             (is_nil(deadline_ms) or
                (is_integer(deadline_ms) and deadline_ms >= 0)) do
    %__MODULE__{
      instance_ctx: instance_ctx,
      request_context: request_context,
      deadline_ms: normalize_deadline(deadline_ms)
    }
  end

  @spec instance_ctx(term()) :: term()
  def instance_ctx(%__MODULE__{instance_ctx: instance_ctx}), do: instance_ctx
  def instance_ctx(ctx), do: ctx

  defp normalize_deadline(deadline_ms) when is_integer(deadline_ms) and deadline_ms > 0,
    do: deadline_ms

  defp normalize_deadline(_deadline_ms), do: nil
end
