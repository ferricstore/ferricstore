defmodule FerricstoreServer.Health.Dashboard.Render.DurationInput do
  @moduledoc false
  import FerricstoreServer.Health.Dashboard.Format, only: [escape_attr: 1, escape: 1]

  def units(name, selected \\ "milliseconds", disabled \\ false) do
    label =
      Map.get(
        %{
          "base_ms" => "Initial delay",
          "max_ms" => "Maximum delay",
          "max_active_ms" => "Maximum active duration",
          "retention_ttl_ms" => "Terminal retention",
          "every_ms" => "Interval",
          "delay_ms" => "Delay"
        },
        name,
        name
      )

    options =
      Enum.map_join(~w(milliseconds seconds minutes hours days), fn unit ->
        ~s(<option value="#{unit}"#{if selected == unit, do: " selected", else: ""}>#{String.capitalize(unit)}</option>)
      end)

    invalid =
      if selected in ~w(milliseconds seconds minutes hours days),
        do: "",
        else:
          ~s(<option value="#{escape_attr(selected)}" selected>Invalid: #{escape(selected)}</option>)

    ~s(<select class="flow-search-input" name="#{name}_unit" data-duration-unit="#{name}" aria-label="#{escape_attr(label)} unit"#{if disabled, do: " disabled", else: ""}>#{invalid}#{options}</select>)
  end
end
