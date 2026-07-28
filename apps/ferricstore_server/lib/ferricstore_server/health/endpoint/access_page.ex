defmodule FerricstoreServer.Health.Endpoint.AccessPage do
  @moduledoc false

  @spec render(map()) :: binary()
  def render(
        %{
          title: title,
          kicker: kicker,
          heading: heading,
          copy: copy,
          form_html: form_html,
          context_heading: context_heading,
          context_items: context_items,
          footer_items: footer_items
        } = options
      )
      when is_binary(title) and is_binary(kicker) and is_binary(heading) and is_binary(copy) and
             is_binary(form_html) and is_binary(context_heading) and is_list(context_items) and
             is_list(footer_items) do
    error_html = render_error(Map.get(options, :error))

    context_html =
      Enum.map_join(context_items, "", fn {label, value} ->
        "<div><dt>#{escape(label)}</dt><dd>#{escape(value)}</dd></div>"
      end)

    footer_html =
      Enum.map_join(footer_items, "", fn item -> "<span>#{escape(item)}</span>" end)

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta name="color-scheme" content="dark">
      <title>#{escape(title)}</title>
      <style>#{styles()}</style>
    </head>
    <body class="auth-body">
      <main class="auth-shell">
        <header class="auth-brand">
          <span class="auth-mark" aria-hidden="true">Fe</span>
          <div><strong>FerricStore</strong><span>OSS control plane</span></div>
        </header>
        <div class="auth-surface">
          <section class="auth-panel">
            <div class="auth-kicker">#{escape(kicker)}</div>
            <h1>#{escape(heading)}</h1>
            <p class="auth-copy">#{escape(copy)}</p>
            #{error_html}
            #{form_html}
            <footer>#{footer_html}</footer>
          </section>
          <aside class="auth-context" aria-label="Access context">
            <div class="context-status"><span aria-hidden="true"></span>Protected access</div>
            <h2>#{escape(context_heading)}</h2>
            <dl>#{context_html}</dl>
          </aside>
        </div>
      </main>
    </body>
    </html>
    """
  end

  @spec escape(term()) :: binary()
  def escape(value) when is_binary(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  def escape(value), do: value |> to_string() |> escape()

  defp render_error(error) when is_binary(error),
    do: "<p class=\"auth-error\" role=\"alert\">#{escape(error)}</p>"

  defp render_error(_error), do: ""

  defp styles do
    """
    *{box-sizing:border-box;letter-spacing:0}html{background:#0b0d0e}body{margin:0}.auth-body{min-height:100vh;min-height:100dvh;display:grid;place-items:center;padding:28px;background:#0b0d0e;color:#d5ddd8;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.auth-body::before{content:"";position:fixed;inset:0 0 auto;height:3px;background:#2b9b6c}.auth-shell{width:min(100%,820px)}.auth-brand{display:flex;align-items:center;gap:12px;margin:0 0 18px;color:#f4f7f5}.auth-brand div{display:flex;flex-direction:column;gap:2px}.auth-brand strong{font-size:.98rem}.auth-brand span:not(.auth-mark){color:#7d8981;font-size:.72rem}.auth-mark{display:grid;place-items:center;width:38px;height:38px;border:1px solid #3b765b;background:#12251c;color:#72d6a6;font:700 .78rem/1 ui-monospace,SFMono-Regular,Menlo,monospace}.auth-surface{display:grid;grid-template-columns:minmax(0,1fr) 252px;border:1px solid #303633;background:#151817;box-shadow:0 18px 55px rgba(0,0,0,.28)}.auth-panel{min-width:0;padding:34px 38px}.auth-kicker{margin-bottom:10px;color:#68cc9d;font-size:.7rem;font-weight:700;text-transform:uppercase}.auth-panel h1{margin:0 0 10px;color:#f7faf8;font-size:1.55rem;line-height:1.25}.auth-copy{max-width:54ch;margin:0 0 24px;color:#98a39c;font-size:.88rem;line-height:1.6}.auth-panel label{display:block;margin:15px 0 7px;color:#dce4df;font-size:.78rem;font-weight:600}.auth-panel input{width:100%;height:43px;border:1px solid #3a413d;border-radius:4px;background:#0d100f;color:#f5f8f6;padding:0 12px;font:inherit;outline:none}.auth-panel input:hover{border-color:#4a554f}.auth-panel input:focus{border-color:#4fc18b;box-shadow:0 0 0 3px rgba(79,193,139,.14)}.auth-panel button{width:100%;height:43px;margin-top:22px;border:1px solid #45af7d;border-radius:4px;background:#27835d;color:#fff;font-family:inherit;font-size:.86rem;font-weight:600;cursor:pointer}.auth-panel button:hover{background:#2c9569}.auth-panel button:focus-visible{outline:3px solid rgba(105,213,162,.3);outline-offset:2px}.field-help{margin:7px 0 0;color:#78847c;font-size:.72rem;line-height:1.45}.auth-error{margin:0 0 18px;border-left:3px solid #df6a63;background:#271716;padding:11px 13px;color:#f5bbb7;font-size:.8rem;line-height:1.45}.auth-panel footer{display:flex;justify-content:space-between;gap:12px;margin-top:22px;padding-top:16px;border-top:1px solid #2a2f2c;color:#737e77;font-size:.67rem;line-height:1.4}.auth-context{min-width:0;border-left:1px solid #303633;background:#101311;padding:30px 26px}.context-status{display:flex;align-items:center;gap:8px;color:#8d9991;font-size:.7rem;font-weight:650;text-transform:uppercase}.context-status span{width:7px;height:7px;border-radius:50%;background:#47bd84;box-shadow:0 0 0 3px rgba(71,189,132,.12)}.auth-context h2{margin:24px 0 28px;color:#e7ece9;font-size:1rem;line-height:1.4}.auth-context dl{display:grid;gap:0;margin:0}.auth-context dl div{padding:13px 0;border-top:1px solid #272c29}.auth-context dt{margin:0 0 5px;color:#6f7b73;font-size:.67rem}.auth-context dd{margin:0;color:#c8d0cb;font:600 .72rem/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}@media(max-width:720px){.auth-body{place-items:start center;padding:20px 16px}.auth-shell{width:min(100%,520px)}.auth-surface{grid-template-columns:1fr}.auth-panel{padding:27px 23px}.auth-context{border-top:1px solid #303633;border-left:0;padding:22px 23px}.auth-context h2{margin:16px 0}.auth-context dl{grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.auth-context dl div{padding:11px 0 0}.auth-context dd{font-size:.67rem}}@media(max-width:440px){.auth-panel footer{flex-direction:column}.auth-context dl{grid-template-columns:1fr}.auth-context dl div{padding:10px 0}.auth-brand{margin-bottom:14px}}
    """
  end
end
