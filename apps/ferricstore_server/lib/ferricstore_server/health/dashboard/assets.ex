defmodule FerricstoreServer.Health.Dashboard.Assets do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.{Layout, Layout.Styles}

  @css Styles.stylesheet()
  @js Layout.dashboard_live_script()
      |> String.trim()
      |> String.replace_prefix(~s(<script id="dashboard-live.js">), "")
      |> String.replace_suffix("</script>", "")
      |> String.trim()

  @css_path "/dashboard/assets/" <>
              Base.encode16(:crypto.hash(:sha256, @css), case: :lower) <> ".css"
  @js_path "/dashboard/assets/" <>
             Base.encode16(:crypto.hash(:sha256, @js), case: :lower) <> ".js"

  def path(:css), do: @css_path
  def path(:js), do: @js_path

  def fetch(@css_path), do: {:ok, "text/css; charset=utf-8", @css}
  def fetch(@js_path), do: {:ok, "text/javascript; charset=utf-8", @js}
  def fetch(_path), do: :error
end
