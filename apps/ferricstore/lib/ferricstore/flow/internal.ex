defmodule Ferricstore.Flow.Internal do
  @moduledoc false

  @internal_option :__ferricstore_internal__
  @schedule_type "__ferricstore_schedule"
  @schedule_id_prefix "__ferricstore_schedule__:"

  def put(opts) when is_list(opts), do: Keyword.put(opts, @internal_option, true)

  def allowed?(opts) when is_list(opts), do: Keyword.get(opts, @internal_option) == true
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
end
