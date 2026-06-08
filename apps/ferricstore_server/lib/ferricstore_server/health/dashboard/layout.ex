defmodule FerricstoreServer.Health.Dashboard.Layout do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  alias FerricstoreServer.Health.Dashboard.Layout.Styles

  def page_head(title, refresh_seconds) do
    page_head(title, refresh_seconds, [])
  end

  def page_head(title, refresh_seconds, opts) do
    _poll_interval_hint = refresh_seconds
    _chartjs_removed = Keyword.get(opts, :chartjs, false)

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta name="description" content="FerricStore operational dashboard">
      <title>#{escape(title)}</title>
      <style>
      #{Styles.stylesheet()}
    </style>
      #{dashboard_live_script()}
    </head>
    """
  end

  def render_live_component(name, html) do
    ~s(<div data-live-component="#{escape_attr(name)}">#{html}</div>)
  end

  def dashboard_live_script do
    """
      <script id="dashboard-live.js">
        (function () {
          function onReady(fn) {
            if (document.readyState === "loading") {
              document.addEventListener("DOMContentLoaded", fn, { once: true });
            } else {
              fn();
            }
          }

          function findComponent(name) {
            var nodes = document.querySelectorAll("[data-live-component]");
            for (var i = 0; i < nodes.length; i += 1) {
              if (nodes[i].getAttribute("data-live-component") === name) {
                return nodes[i];
              }
            }
            return null;
          }

          function dashboardInteractionPaused() {
            var modal = document.getElementById("flow-value-modal");
            if (modal && !modal.hidden) { return true; }

            var active = document.activeElement;
            if (!active || !active.closest) { return false; }
            return !!active.closest("input, textarea, select, button, [data-dashboard-live-pause]");
          }

          function patchComponents(components) {
            if (!components || dashboardInteractionPaused()) { return; }
            Object.keys(components).forEach(function (name) {
              var target = findComponent(name);
              var nextHtml = components[name];
              if (!target || typeof nextHtml !== "string") { return; }
              if (target.innerHTML !== nextHtml) {
                target.innerHTML = nextHtml;
              }
            });
          }

          function setupFlowValueInspector() {
            var modal = document.getElementById("flow-value-modal");
            if (!modal || modal.dataset.bound === "1") { return; }
            modal.dataset.bound = "1";

            var refNode = document.getElementById("flow-value-modal-ref");
            var bodyNode = document.getElementById("flow-value-modal-body");
            var copyButton = document.getElementById("flow-value-modal-copy");
            var copyStatus = document.getElementById("flow-value-modal-copy-status");

            function setCopyStatus(text) {
              if (copyStatus) { copyStatus.textContent = text || ""; }
            }

            function closeModal() {
              modal.hidden = true;
              setCopyStatus("");
            }

            function fallbackCopy(text) {
              var textarea = document.createElement("textarea");
              textarea.value = text;
              textarea.setAttribute("readonly", "readonly");
              textarea.style.position = "fixed";
              textarea.style.left = "-9999px";
              document.body.appendChild(textarea);
              textarea.select();
              try {
                document.execCommand("copy");
                setCopyStatus("Copied");
              } catch (_error) {
                setCopyStatus("Copy failed");
              } finally {
                document.body.removeChild(textarea);
              }
            }

            function copyValue() {
              var text = bodyNode ? bodyNode.textContent : "";
              if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(text)
                  .then(function () { setCopyStatus("Copied"); })
                  .catch(function () { fallbackCopy(text); });
              } else {
                fallbackCopy(text);
              }
            }

            function openFromRow(row, link) {
              var preview = row ? row.querySelector("[data-flow-value-preview]") : null;
              var ref = link.getAttribute("data-flow-value-ref") || (row && row.getAttribute("data-flow-value-ref")) || link.getAttribute("title") || "";
              var label = link.getAttribute("data-flow-value-label") || (row && row.getAttribute("data-flow-value-label")) || link.textContent || "value";
              var value = preview ? preview.textContent : "Value is not loaded on this page.";

              if (refNode) { refNode.textContent = label + " · " + ref; }
              if (bodyNode) { bodyNode.textContent = value; }
              setCopyStatus("");
              modal.hidden = false;
              if (copyButton) { copyButton.focus(); }
              return true;
            }

            function flowValueAnchorFromHref(link) {
              var href = link.getAttribute("href") || "";
              if (href.charAt(0) === "#") { return href.slice(1); }

              try {
                var url = new URL(href, window.location.href);
                if (url.pathname !== window.location.pathname || !url.hash) { return ""; }
                return decodeURIComponent(url.hash.slice(1));
              } catch (_error) {
                return "";
              }
            }

            function flowValueRefFromAnchor(anchor) {
              if (!anchor || anchor.indexOf("flow-value-") !== 0) { return ""; }

              try {
                var encoded = anchor.slice("flow-value-".length).replace(/-/g, "+").replace(/_/g, "/");
                while (encoded.length % 4 !== 0) { encoded += "="; }
                var binary = atob(encoded);
                var escaped = "";
                for (var i = 0; i < binary.length; i += 1) {
                  escaped += "%" + ("00" + binary.charCodeAt(i).toString(16)).slice(-2);
                }
                return decodeURIComponent(escaped);
              } catch (_error) {
                return "";
              }
            }

            function flowValueRequestUrl(ref, link) {
              var sourceUrl;

              try {
                var href = link && link.getAttribute ? link.getAttribute("href") : "";
                sourceUrl = new URL(href || window.location.href, window.location.href);
              } catch (_error) {
                sourceUrl = new URL(window.location.href);
              }

              var match = sourceUrl.pathname.match(/^\\/dashboard\\/flow\\/(.+)$/);
              if (!match) { return ""; }

              var params = new URLSearchParams();
              params.set("flow", decodeURIComponent(match[1]));
              params.set("ref", ref);

              var partition = sourceUrl.searchParams.get("partition_key");
              if (!partition) {
                partition = new URLSearchParams(window.location.search).get("partition_key");
              }
              if (partition) { params.set("partition_key", partition); }

              return "/dashboard/api/flow/value?" + params.toString();
            }

            function openFromRef(ref, label, link) {
              var url = flowValueRequestUrl(ref, link);
              if (!ref || !url) { return false; }

              if (refNode) { refNode.textContent = (label || "value") + " · " + ref; }
              if (bodyNode) { bodyNode.textContent = "Loading value..."; }
              setCopyStatus("");
              modal.hidden = false;
              if (copyButton) { copyButton.focus(); }

              fetch(url, {
                cache: "no-store",
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (!response.ok) { throw new Error("value request failed"); }
                  return response.json();
                })
                .then(function (payload) {
                  if (!payload || payload.status !== "ok") {
                    throw new Error((payload && payload.error) || "value unavailable");
                  }
                  if (bodyNode) { bodyNode.textContent = payload.value || ""; }
                })
                .catch(function (error) {
                  if (bodyNode) { bodyNode.textContent = error.message || "Value unavailable."; }
                });

              return true;
            }

            function findValueLinkForAnchor(anchor) {
              var links = document.querySelectorAll(".flow-value-ref-link");
              for (var i = 0; i < links.length; i += 1) {
                if (flowValueAnchorFromHref(links[i]) === anchor) { return links[i]; }
              }
              return null;
            }

            function openFromLink(link) {
              var anchor = flowValueAnchorFromHref(link);
              if (!anchor) { return false; }

              var row = document.getElementById(anchor);
              if (row) { return openFromRow(row, link); }

              return openFromRef(
                link.getAttribute("data-flow-value-ref") || flowValueRefFromAnchor(anchor),
                link.getAttribute("data-flow-value-label") || link.textContent || "value",
                link
              );
            }

            function openFromHash() {
              var hash = window.location.hash || "";
              if (hash.length < 2) { return; }

              var anchor = decodeURIComponent(hash.slice(1));
              var row = document.getElementById(anchor);
              var link = findValueLinkForAnchor(anchor);

              if (!row || !row.hasAttribute("data-flow-value-ref")) {
                var ref = link ? link.getAttribute("data-flow-value-ref") : flowValueRefFromAnchor(anchor);
                if (ref) {
                  openFromRef(
                    ref,
                    (link && (link.getAttribute("data-flow-value-label") || link.textContent)) || "value",
                    link
                  );
                }
                return;
              }

              if (!link) {
                link = {
                  getAttribute: function (name) {
                    if (name === "data-flow-value-ref" || name === "title") { return row.getAttribute("data-flow-value-ref"); }
                    if (name === "data-flow-value-label") { return row.getAttribute("data-flow-value-label"); }
                    return "";
                  },
                  textContent: row.getAttribute("data-flow-value-label") || "value"
                };
              }

              openFromRow(row, link);
            }

            document.addEventListener("click", function (event) {
              var closeTarget = event.target.closest("[data-flow-value-modal-close]");
              if (closeTarget) {
                event.preventDefault();
                closeModal();
                return;
              }

              var link = event.target.closest(".flow-value-ref-link");
              if (link && openFromLink(link)) {
                event.preventDefault();
              }
            });

            document.addEventListener("keydown", function (event) {
              if (event.key === "Escape" && !modal.hidden) {
                event.preventDefault();
                closeModal();
              }
            });

            if (copyButton) {
              copyButton.addEventListener("click", copyValue);
            }

            window.addEventListener("hashchange", openFromHash);
            openFromHash();
          }

          onReady(function () {
            setupFlowValueInspector();

            var root = document.body;
            if (!root || !root.dataset || !root.dataset.dashboardLiveUrl) { return; }

            var url = root.dataset.dashboardLiveUrl;
            var intervalMs = parseInt(root.dataset.dashboardLiveIntervalMs || "2000", 10);
            if (!Number.isFinite(intervalMs) || intervalMs < 500) { intervalMs = 2000; }

            var inFlight = false;

            function tick() {
              if (document.hidden || inFlight) { return; }
              inFlight = true;

              fetch(url, {
                cache: "no-store",
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (!response.ok) { throw new Error("dashboard live request failed"); }
                  return response.json();
                })
                .then(function (payload) {
                  patchComponents(payload.components);
                  root.dataset.dashboardLiveLastUpdateMs = String(payload.generated_at_ms || Date.now());
                })
                .catch(function () {
                  root.dataset.dashboardLiveError = "1";
                })
                .finally(function () {
                  inFlight = false;
                });
            }

            window.setInterval(tick, intervalMs);
          });
        }());
      </script>
    """
  end

  def render_subpage_header(title) do
    """
    <div class="subpage-header">
      <h1 class="subpage-title">#{escape(title)}</h1>
    </div>
    """
  end

  def render_page_intro(title, body) do
    """
    <section class="page-intro" aria-label="#{escape_attr(title)} page purpose">
      <div class="page-intro-title">#{escape(title)}</div>
      <p>#{escape(body)}</p>
    </section>
    """
  end

  def render_kv_subnav(active) do
    items = [
      {"keyspace", "/dashboard/keyspace", "Keyspace", "Find keys and inspect metadata"},
      {"reads", "/dashboard/reads", "Read Path", "Hot-cache and cold-read health"},
      {"commands", "/dashboard/commands", "Commands", "Traffic, slowlog, and command groups"},
      {"prefixes", "/dashboard/prefixes", "Prefixes", "Sampled prefix distribution"},
      {"storage", "/dashboard/storage", "Storage", "Disk files, segments, and shard usage"}
    ]

    links =
      Enum.map_join(items, "\n", fn {key, href, label, title} ->
        active_class = if key == active, do: " active", else: ""
        current = if key == active, do: ~s( aria-current="page"), else: ""

        ~s(<a class="flow-tab#{active_class}" href="#{href}"#{current} title="#{escape_attr(title)}">#{escape(label)}</a>)
      end)

    ~s(<nav class="flow-tabs" aria-label="KV dashboard sections">#{links}</nav>)
  end

  # Sidebar with live badge data (used on main dashboard)
  def render_sidebar(data, active) do
    slowlog_count = length(data.slowlog)
    slowlog_badge = if slowlog_count == 0, do: "", else: "#{slowlog_count}"

    active_merges = Enum.count(data.merge, & &1.merging)
    merge_badge = if active_merges > 0, do: "#{active_merges}", else: ""

    config_count = length(data.namespace_config)
    config_badge = if config_count == 0, do: "", else: "#{config_count}"

    conns = data.connections
    conns_badge = if conns.active > 0, do: "#{conns.active}", else: ""

    storage_badge = format_bytes(data.storage_summary.total_disk_bytes)
    flow_active = Map.get(data.flow_summary, :active, 0)
    flow_badge = if flow_active > 0, do: "#{flow_active}", else: ""

    sidebar_html(active, %{
      "slowlog" => slowlog_badge,
      "merge" => merge_badge,
      "flow" => flow_badge,
      "config" => config_badge,
      "clients" => conns_badge,
      "storage" => storage_badge,
      "keyspace" => "",
      "reads" => "",
      "commands" => "",
      "doctor" => ""
    })
  end

  # Sidebar without live data (used on sub-pages to avoid expensive data collection)
  def render_sidebar_static(active) do
    sidebar_html(active, %{
      "slowlog" => "",
      "merge" => "",
      "flow" => "",
      "config" => "",
      "clients" => "",
      "storage" => "",
      "keyspace" => "",
      "reads" => "",
      "commands" => "",
      "doctor" => ""
    })
  end

  def sidebar_html(active, badges) do
    items = [
      {"overview", "/dashboard", "Overview"},
      {"flow", "/dashboard/flow", "FerricFlow"},
      {"keyspace", "/dashboard/keyspace", "Keyspace"},
      {"reads", "/dashboard/reads", "Read Path"},
      {"commands", "/dashboard/commands", "Commands"},
      {"slowlog", "/dashboard/slowlog", "Slow Log"},
      {"merge", "/dashboard/merge", "Merge Status"},
      {"storage", "/dashboard/storage", "Storage"},
      {"doctor", "/dashboard/doctor", "Doctor"},
      {"raft", "/dashboard/raft", "Consensus"},
      {"config", "/dashboard/config", "Config"},
      {"clients", "/dashboard/clients", "Clients"},
      {"prefixes", "/dashboard/prefixes", "Key Prefixes"}
    ]

    links =
      Enum.map_join(items, "\n", fn {key, href, label} ->
        active_class = if key == active, do: " active", else: ""
        current_attr = if key == active, do: ~s( aria-current="page"), else: ""
        badge_val = Map.get(badges, key, "")

        badge_html =
          if badge_val != "" and badge_val != nil do
            ~s(<span class="nav-badge">#{escape(to_string(badge_val))}</span>)
          else
            ""
          end

        ~s(<a class="#{active_class}" href="#{href}"#{current_attr}><span class="nav-label">#{escape(label)}</span>#{badge_html}</a>)
      end)

    """
    <nav class="sidebar" aria-label="Dashboard sections">
      #{links}
    </nav>
    """
  end
end
