defmodule FerricstoreBench.MetadataRouteVariant do
  @moduledoc false
  @module :ferricstore_waraft_spike_segment_log
  @base "apps/ferricstore/src"
  @promotion "apps/ferricstore/lib/ferricstore/store/promotion.ex"

  def prepare do
    mode = System.get_env("BENCH_METADATA_ROUTE", "raw")
    paths = Path.wildcard("#{@base}/ferricstore_waraft_spike_segment_log/sections/*.hrl")
    original = Map.new(paths, &{&1, File.read!(&1)})
    promotion = File.read!(@promotion)

    case mode do
      "raw" ->
        {mode, original, promotion, nil}

      "server" ->
        archived =
          "bench/results/metadata-route-pilot-100-server-1.json"
          |> File.read!()
          |> Jason.decode!()

        original = Map.fetch!(archived, "metadata_source")
        promotion = Map.fetch!(archived, "promotion_source")
        root = Path.join([System.tmp_dir!(), "opencode", "metadata-route-#{System.pid()}"])
        if File.exists?(root), do: raise("control fixture exists")
        sections = Path.join(root, "ferricstore_waraft_spike_segment_log/sections")
        File.mkdir_p!(sections)

        changed =
          Map.new(original, fn {path, source} ->
            File.write!(Path.join(sections, Path.basename(path)), source)
            {path, source}
          end)

        source = File.read!("#{@base}/#{@module}.erl")
        path = Path.join(root, "#{@module}.erl")
        File.write!(path, source)

        {:ok, @module, beam, warnings} =
          :compile.file(
            String.to_charlist(path),
            [:binary, :return_errors, :return_warnings, {:i, String.to_charlist(root)}]
          )

        if warnings != [], do: IO.inspect(warnings, label: "METADATA_CONTROL_WARNINGS")
        beam_path = Path.join(root, "#{@module}.beam")
        File.write!(beam_path, beam)
        {:module, @module} = :code.load_binary(@module, String.to_charlist(beam_path), beam)
        Code.compiler_options(ignore_module_conflict: true)

        [{Ferricstore.Store.Promotion, promotion_beam}] =
          Code.compile_string(promotion, @promotion)

        promotion_path = Path.join(root, "Elixir.Ferricstore.Store.Promotion.beam")
        File.write!(promotion_path, promotion_beam)

        {:module, Ferricstore.Store.Promotion} =
          :code.load_binary(
            Ferricstore.Store.Promotion,
            String.to_charlist(promotion_path),
            promotion_beam
          )

        {mode, changed, promotion, root}

      other ->
        raise("invalid metadata route: #{other}")
    end
  end
end
