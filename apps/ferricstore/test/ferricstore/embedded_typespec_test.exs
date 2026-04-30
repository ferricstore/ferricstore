defmodule Ferricstore.EmbeddedTypespecTest do
  use ExUnit.Case, async: true

  test "embedded write APIs expose structured unknown-outcome timeout errors" do
    assert {:ok, types} = Code.Typespec.fetch_types(FerricStore)
    assert type_defined?(types, :write_error)

    assert {:ok, specs} = Code.Typespec.fetch_specs(FerricStore)

    for fun <- [set: 3, batch_set: 1] do
      assert spec_references_type?(specs, fun, :write_error),
             "#{inspect(fun)} spec must include FerricStore.write_error/0"
    end

    for module <- [FerricStore.Pipe, FerricStore.Tx] do
      assert {:ok, specs} = Code.Typespec.fetch_specs(module)

      assert spec_references_type?(specs, {:execute, 1}, :write_error),
             "#{inspect(module)}.execute/1 spec must include FerricStore.write_error/0"
    end
  end

  defp type_defined?(types, name) do
    Enum.any?(types, fn
      {:type, {^name, _type, _args}} -> true
      _ -> false
    end)
  end

  defp spec_references_type?(specs, fun, type_name) do
    specs
    |> Enum.filter(fn {spec_fun, _specs} -> spec_fun == fun end)
    |> Enum.map(fn {_spec_fun, specs} -> specs end)
    |> Enum.any?(&term_references_type?(&1, type_name))
  end

  defp term_references_type?({:user_type, _line, type_name, _args}, type_name), do: true
  defp term_references_type?({:atom, _line, type_name}, type_name), do: true

  defp term_references_type?(term, type_name) when is_tuple(term) do
    term
    |> Tuple.to_list()
    |> Enum.any?(&term_references_type?(&1, type_name))
  end

  defp term_references_type?(term, type_name) when is_list(term) do
    Enum.any?(term, &term_references_type?(&1, type_name))
  end

  defp term_references_type?(_term, _type_name), do: false
end
