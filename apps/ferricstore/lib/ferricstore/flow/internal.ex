defmodule Ferricstore.Flow.Internal do
  @moduledoc false

  @internal_option :__ferricstore_internal__
  @capability_key {__MODULE__, :capability}
  @schedule_type "__ferricstore_schedule"
  @schedule_id_prefix "__ferricstore_schedule__:"

  @on_load :init_capability

  def init_capability do
    case :persistent_term.get(@capability_key, :missing) do
      :missing -> :persistent_term.put(@capability_key, make_ref())
      _capability -> :ok
    end

    :ok
  end

  def put(opts) when is_list(opts), do: Keyword.put(opts, @internal_option, capability())

  def allowed?(opts) when is_list(opts) do
    case Keyword.fetch(opts, @internal_option) do
      {:ok, token} when is_reference(token) -> token === capability()
      _other -> false
    end
  end

  def allowed?(_opts), do: false

  def reserved_type?(@schedule_type), do: true
  def reserved_type?(_type), do: false

  def reserved_id?(id) when is_binary(id), do: String.starts_with?(id, @schedule_id_prefix)
  def reserved_id?(_id), do: false

  def reject_reserved_type(type, opts) do
    if reserved_type?(type) and not allowed?(opts) do
      {:error, "ERR flow type is reserved for internal use"}
    else
      :ok
    end
  end

  def reject_reserved_id(id, opts) do
    if reserved_id?(id) and not allowed?(opts) do
      {:error, "ERR flow id is reserved for internal use"}
    else
      :ok
    end
  end

  defp capability, do: :persistent_term.get(@capability_key)
end
