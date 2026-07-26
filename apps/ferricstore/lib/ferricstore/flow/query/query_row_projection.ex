defmodule Ferricstore.Flow.Query.QueryRowProjection do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Plan, Request}

  @paths [:count_scan, :ordered_range, :ordered_range_union, :ordered_filter]

  @spec eligible?(Request.t(), Plan.t()) :: boolean()
  def eligible?(
        %Request{source: :runs, return: :count},
        %Plan{path: path}
      )
      when path in @paths,
      do: true

  def eligible?(
        %Request{source: :runs, return: :record, projection: [_field | _rest]},
        %Plan{path: path}
      )
      when path in @paths,
      do: true

  def eligible?(%Request{} = request, %Plan{path: path}), do: eligible?(request, path)

  @spec eligible?(Request.t(), atom()) :: boolean()
  def eligible?(%Request{source: :runs, return: :count}, path) when path in @paths, do: true

  def eligible?(%Request{source: :runs, return: :record, projection: [_field | _rest]}, path)
      when path in @paths,
      do: true

  def eligible?(_request, _path), do: false
end
