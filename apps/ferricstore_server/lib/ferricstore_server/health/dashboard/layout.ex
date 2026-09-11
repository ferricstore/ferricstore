defmodule FerricstoreServer.Health.Dashboard.Layout do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  alias FerricstoreServer.Acl

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
      <link rel="preconnect" href="https://fonts.googleapis.com">
      <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
      <link rel="stylesheet" href="#{FerricstoreServer.Health.Dashboard.Assets.path(:css)}">
      <script src="#{FerricstoreServer.Health.Dashboard.Assets.path(:js)}" defer></script>
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
          window.dashboardCopyText = async function (text, options) {
            options = options || {};
            var previousFocus = options.restoreFocus || document.activeElement;
            var textarea = null;
            try {
              if (navigator.clipboard && navigator.clipboard.writeText) {
                try { await navigator.clipboard.writeText(text); return true; }
                catch (_clipboardError) {}
              }
              textarea = document.createElement("textarea");
              textarea.value = text;
              textarea.setAttribute("readonly", "readonly");
              textarea.style.position = "fixed";
              textarea.style.left = "-9999px";
              (options.container || document.querySelector("dialog[open]") || document.body).appendChild(textarea);
              textarea.select();
              return document.execCommand("copy") === true;
            } catch (_error) {
              return false;
            } finally {
              if (textarea) { textarea.remove(); }
              if (textarea && previousFocus && previousFocus.isConnected) { previousFocus.focus({ preventScroll: true }); }
            }
          };

          function onReady(fn) {
            if (document.readyState === "loading") {
              document.addEventListener("DOMContentLoaded", fn, { once: true });
            } else {
              fn();
            }
          }

          function clearTransientQueryParams() {
            var flash = document.querySelector("[data-dashboard-transient-query]");
            if (!flash || !window.history || !window.history.replaceState) { return; }

            var url = new URL(window.location.href);
            var changed = false;
            var names = (flash.dataset.dashboardTransientQuery || "").split(",");

            for (var i = 0; i < names.length; i += 1) {
              var name = names[i].trim();
              if (name && url.searchParams.has(name)) {
                url.searchParams.delete(name);
                changed = true;
              }
            }

            if (changed) {
              var query = url.searchParams.toString();
              var cleanUrl = url.pathname + (query ? "?" + query : "") + url.hash;
              window.history.replaceState(window.history.state, "", cleanUrl);
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
            if (document.querySelector('dialog[open], .flow-action-confirm[open]')) { return true; }
            var modal = document.getElementById("flow-value-modal");
            if (modal && !modal.hidden) { return true; }

            var selection = window.getSelection && window.getSelection();
            if (selection && !selection.isCollapsed) { return true; }

            var active = document.activeElement;
            if (!active || !active.closest) { return false; }
            return !!active.closest("input, textarea, select, [data-dashboard-live-pause]");
          }

          function componentStateKey(node, index) {
            return node.id || node.getAttribute("data-dashboard-scroll-key") ||
              node.getAttribute("data-dashboard-disclosure-key") || node.getAttribute("data-dashboard-nav-group") ||
              (node.matches('details') ? null : String(index));
          }

          function captureComponentState(target) {
            var state = { disclosures: new Map(), scroll: new Map() };
            target.querySelectorAll("details").forEach(function (node, index) {
              var key = componentStateKey(node, index);
              if (key) { state.disclosures.set(key, node.open); }
            });
            target.querySelectorAll("[data-dashboard-scroll-key], .sidebar, .table-scroll, .flow-query-table-wrap, .flow-journal-inspector, .flow-value-preview, .payload-box-body").forEach(function (node, index) {
              state.scroll.set(componentStateKey(node, index), { top: node.scrollTop, left: node.scrollLeft, focused: node === document.activeElement });
            });
            return state;
          }

          function restoreComponentState(target, state) {
            target.querySelectorAll("details").forEach(function (node, index) {
              var key = componentStateKey(node, index);
              if (state.disclosures.has(key)) { node.open = state.disclosures.get(key); }
            });
            target.querySelectorAll("[data-dashboard-scroll-key], .sidebar, .table-scroll, .flow-query-table-wrap, .flow-journal-inspector, .flow-value-preview, .payload-box-body").forEach(function (node, index) {
              var saved = state.scroll.get(componentStateKey(node, index));
              if (!saved) { return; }
              node.scrollTop = saved.top;
              node.scrollLeft = saved.left;
              if (saved.focused) { node.focus({ preventScroll: true }); }
            });
          }

          var componentHtml = new WeakMap();
          function patchComponents(components) {
            if (!components || dashboardInteractionPaused()) { return false; }
            var complete = true;
            Object.keys(components).forEach(function (name) {
              var target = findComponent(name);
              var nextHtml = components[name];
              if (!target || typeof nextHtml !== "string") { return; }
              if ((componentHtml.get(target) || target.innerHTML) !== nextHtml) {
                var active = document.activeElement;
                var liveStatus = target.querySelector('[data-dashboard-live-status]');
                var statusFocused = liveStatus && liveStatus.contains(active);
                if (active && target.contains(active) && !statusFocused && !active.matches(".table-scroll, [data-dashboard-scroll-key]")) {
                  complete = false;
                  return;
                }
                var retainedState = captureComponentState(target);
                target.innerHTML = nextHtml;
                if (liveStatus) {
                  (target.querySelector('.subpage-header, .top-bar') || target).appendChild(liveStatus);
                  if (statusFocused) { active.focus({ preventScroll: true }); }
                }
                componentHtml.set(target, nextHtml);
                if (typeof applyJournalState === "function") {
                  applyJournalState(target);
                }
                restoreComponentState(target, retainedState);
              }
            });
            preserveWorkflowScope();
            applyTableFilters();
            updateDisclosureCounts();
            labelMetricHeaders();
            ensureSkipLink();
            syncShortcutPreferences();
            initializeFilterDrafts();
            if (complete && window.dashboardRecentRates) { window.dashboardRecentRates.update(); }
            return complete;
          }

          function ensureSkipLink() {
            var main = document.querySelector('main');
            if (!main) { return; }
            if (!main.id) { main.id = 'dashboard-main'; }
            if (!main.hasAttribute('tabindex')) { main.tabIndex = -1; }
            var links = document.querySelectorAll('.dashboard-skip-link');
            if (!links.length) { return; }
            var skipLink = links[0];
            skipLink.href = '#' + main.id;
            if (document.body.firstElementChild !== skipLink) {
              document.body.insertBefore(skipLink, document.body.firstChild);
            }
            Array.from(links).slice(1).forEach(function (duplicate) { duplicate.remove(); });
          }

          function preserveWorkflowScope() {
            var renderedScope = document.querySelector("[data-dashboard-workflow-scope]");
            if (renderedScope) {
              renderedScope.querySelectorAll("a[data-dashboard-route][href]").forEach(function (sourceLink) {
                var destination = new URL(sourceLink.href, window.location.origin);
                if (destination.origin !== window.location.origin || !destination.pathname.startsWith('/dashboard/')) { return; }
                document.querySelectorAll('.sidebar a[href]').forEach(function (link) {
                  if (new URL(link.href).pathname !== sourceLink.dataset.dashboardRoute) { return; }
                  link.setAttribute('href', sourceLink.getAttribute('href'));
                  ['title', 'aria-description'].forEach(function (attribute) {
                    if (sourceLink.hasAttribute(attribute)) { link.setAttribute(attribute, sourceLink.getAttribute(attribute)); }
                    else { link.removeAttribute(attribute); }
                  });
                });
              });
              return;
            }
            var source = new URL(window.location.href);
            if (!source.pathname.startsWith('/dashboard/flow')) { return; }
            var routes = ['/dashboard/flow', '/dashboard/flow/states', '/dashboard/flow/workers',
              '/dashboard/flow/due', '/dashboard/flow/failures', '/dashboard/flow/query',
              '/dashboard/flow/signals', '/dashboard/flow/lineage'];
            document.querySelectorAll('.sidebar a[href]').forEach(function (link) {
              var target = new URL(link.href, window.location.origin);
              if (!routes.includes(target.pathname)) { return; }
              ['type', 'partition_key'].forEach(function (name) {
                if (source.searchParams.has(name)) { target.searchParams.set(name, source.searchParams.get(name)); }
              });
              link.href = target.pathname + target.search;
            });
          }

          function navigateScoped(path) {
            var link = Array.from(document.querySelectorAll('.sidebar a[href]')).find(function (item) {
              return new URL(item.href).pathname === path;
            });
            window.location.assign(link ? link.href : path);
          }

          var tableFilterValues = new Map();
          function applyTableFilters() {
            document.querySelectorAll('[data-dashboard-table-filter]').forEach(function (input) {
              var key = input.dataset.dashboardFilterTarget;
              if (tableFilterValues.has(key)) { input.value = tableFilterValues.get(key); }
              var target = document.querySelector(key);
              if (!target) { return; }
              var query = input.value.toLocaleLowerCase();
              var rows = Array.from(target.querySelectorAll('tbody tr')).filter(function (row) {
                if (row.hasAttribute('data-table-filter-empty')) { return false; }
                return !(row.cells.length === 1 && row.cells[0].hasAttribute('colspan'));
              });
              var visible = 0;
              rows.forEach(function (row) {
                row.hidden = !!query && !row.textContent.toLocaleLowerCase().includes(query);
                if (!row.hidden) { visible += 1; }
              });
              var empty = target.querySelector('[data-table-filter-empty]');
              if (query && rows.length > 0 && visible === 0 && !empty) {
                empty = document.createElement('tr');
                empty.setAttribute('data-table-filter-empty', '');
                var cell = document.createElement('td');
                cell.colSpan = target.querySelector('thead tr')?.cells.length || rows[0].cells.length;
                cell.className = 'c-muted';
                cell.textContent = 'No loaded rows match this filter.';
                empty.appendChild(cell);
                target.querySelector('tbody').appendChild(empty);
              }
              if (empty) { empty.hidden = !(query && rows.length > 0 && visible === 0); }
              var status = input.closest('[data-dashboard-filter-control]');
              status = status && status.querySelector('[data-dashboard-filter-status]');
              if (status) { status.textContent = query ? visible + ' of ' + rows.length + ' loaded rows' : 'Loaded rows'; }
            });
          }

          function updateActionSnapshot(snapshot) {
            if (!snapshot) { return; }
            document.querySelectorAll('[data-flow-action-snapshot-version]').forEach(function (panel) {
              var changed = snapshot.available === false || String(snapshot.version) !== panel.dataset.flowActionSnapshotVersion;
              if (!changed) { return; }
              panel.dataset.flowActionStaleState = 'true';
              var notice = panel.querySelector('[data-flow-action-stale]');
              if (notice) { notice.hidden = false; }
              var summary = panel.matches('details') && panel.querySelector(':scope > summary');
              if (summary && !summary.querySelector('[data-flow-action-stale-summary]')) {
                var summaryNotice = document.createElement('span');
                summaryNotice.dataset.flowActionStaleSummary = '';
                summaryNotice.className = 'badge badge-warning';
                summaryNotice.setAttribute('role', 'status');
                summaryNotice.textContent = 'Review required';
                summary.appendChild(summaryNotice);
              }
              panel.querySelectorAll('button[type=submit], input[type=submit]').forEach(function (button) { button.disabled = true; });
            });
          }

          function labelMetricHeaders() {
            document.querySelectorAll('th:has(.info-icon)').forEach(function (header) {
              var name = header.getAttribute('aria-label');
              if (!name) {
                var label = header.cloneNode(true);
                label.querySelectorAll('.info-icon').forEach(function (icon) { icon.remove(); });
                name = label.textContent.trim();
                header.setAttribute('aria-label', name);
              }
              header.querySelectorAll('.info-icon').forEach(function (icon) {
                icon.setAttribute('aria-label', 'About ' + name);
              });
            });
          }

          function setupMetricHelp() {
            var tooltip = document.createElement('div');
            tooltip.id = 'dashboard-metric-tooltip';
            tooltip.className = 'dashboard-tooltip';
            tooltip.setAttribute('data-dashboard-tooltip', '');
            tooltip.setAttribute('role', 'tooltip');
            tooltip.setAttribute('popover', 'manual');
            tooltip.hidden = true;
            document.body.appendChild(tooltip);
            var trigger = null;
            function hide() {
              if (trigger) { trigger.removeAttribute('aria-describedby'); }
              trigger = null;
              if (tooltip.hidePopover && tooltip.matches(':popover-open')) { tooltip.hidePopover(); }
              tooltip.hidden = true;
            }
            function position() {
              if (!trigger || !trigger.isConnected) { hide(); return; }
              var rect = trigger.getBoundingClientRect();
              var bounds = tooltip.getBoundingClientRect();
              var cssWidth = parseFloat(window.getComputedStyle(tooltip).width);
              var scale = cssWidth > 0 ? bounds.width / cssWidth : 1;
              tooltip.style.maxWidth = Math.min(320, (window.innerWidth - 16) / scale) + 'px';
              tooltip.style.maxHeight = (window.innerHeight - 16) / scale + 'px';
              bounds = tooltip.getBoundingClientRect();
              var left = Math.max(8, Math.min(rect.left + rect.width / 2 - bounds.width / 2, window.innerWidth - bounds.width - 8));
              var top = rect.top - bounds.height - 8;
              if (top < 8) { top = Math.min(rect.bottom + 8, window.innerHeight - bounds.height - 8); }
              tooltip.style.left = left / scale + 'px';
              tooltip.style.top = Math.max(8, Math.min(top, window.innerHeight - bounds.height - 8)) / scale + 'px';
            }
            function show(node) {
              if (trigger && trigger !== node) { trigger.removeAttribute('aria-describedby'); }
              trigger = node;
              tooltip.textContent = node.dataset.tooltip;
              node.setAttribute('aria-describedby', tooltip.id);
              tooltip.hidden = false;
              if (tooltip.showPopover && !tooltip.matches(':popover-open')) { tooltip.showPopover(); }
              position();
            }
            document.addEventListener('mouseover', function (event) {
              var node = event.target.closest('.info-icon[data-tooltip]');
              if (node) { show(node); }
            });
            document.addEventListener('mouseout', function (event) {
              if (!trigger) { return; }
              var next = event.relatedTarget;
              if (next && (trigger.contains(next) || tooltip.contains(next))) { return; }
              if (document.activeElement !== trigger) { hide(); }
            });
            document.addEventListener('focusin', function (event) {
              var node = event.target.closest('.info-icon[data-tooltip]');
              if (node) { show(node); } else { hide(); }
            });
            document.addEventListener('click', function (event) {
              var node = event.target.closest('.info-icon[data-tooltip]');
              if (node) { show(node); }
            });
            document.addEventListener('keydown', function (event) { if (event.key === 'Escape') { hide(); } });
            document.addEventListener('scroll', position, true);
            window.addEventListener('resize', position);
            labelMetricHeaders();
          }

          function updateDisclosureCounts() {
            document.querySelectorAll('[data-dashboard-disclosure-count]').forEach(function (badge) {
              var component = findComponent(badge.dataset.dashboardDisclosureCount);
              var count = component && component.querySelector('[data-dashboard-row-count]');
              if (count) { badge.textContent = count.dataset.dashboardRowCount; }
            });
          }

          function setupSnapshot() {
            var snapshot = document.querySelector('[data-dashboard-snapshot]');
            if (!snapshot) { return; }
            snapshot.hidden = !!document.body.dataset.dashboardLiveUrl;
          }

          function setupAccountProfiles() {
            document.querySelectorAll('[data-acl-profile-form]').forEach(function (form) {
              var preview = form.querySelector('[data-acl-profile-preview]');
              function update() {
                var role = form.elements.namedItem('role').value;
                form.querySelectorAll('[data-acl-profile]').forEach(function (section) {
                  section.disabled = section.dataset.aclProfile !== role;
                  section.hidden = section.disabled;
                });
                var text = role === 'admin' ? 'Administrator: all commands, keys, and channels. No scope restrictions.' :
                  role === 'custom' ? 'Custom: explicit ACL modifiers only; no implicit read access.' :
                  'Observer: read access to keys ' + (form.elements.namedItem('key_pattern').value || '(pattern required)') +
                  '; channels ' + (form.elements.namedItem('channel_pattern').value || '(pattern required)') + '.';
                if (preview && preview.textContent !== text) { preview.textContent = text; }
              }
              form.addEventListener('change', update);
              form.addEventListener('input', update);
              form.addEventListener('reset', function () { queueMicrotask(update); });
              window.addEventListener('pageshow', update);
              update();
            });
          }

          function decodeDashboardHash(value) {
            try { return decodeURIComponent(value); }
            catch (_error) { return ""; }
          }

          function setupWorkflowActionNavigation() {
            function reveal(hash) {
              if (decodeDashboardHash((hash || '').slice(1)) !== 'workflow-actions') { return; }
              var section = document.getElementById('workflow-actions');
              var panel = section && section.querySelector('.flow-operations-panel');
              if (panel) { panel.open = true; }
            }
            document.addEventListener('click', function (event) {
              if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) { return; }
              var link = event.target.closest('a[href="#workflow-actions"]');
              if (link) { reveal(link.hash); }
            });
            window.addEventListener('hashchange', function () { reveal(window.location.hash); });
            reveal(window.location.hash);
          }

          function setupFlowValueInspector() {
            var modal = document.getElementById("flow-value-modal");
            if (!modal || modal.dataset.bound === "1") { return; }
            modal.dataset.bound = "1";

            var refNode = document.getElementById("flow-value-modal-ref");
            var provenance = document.getElementById('flow-value-modal-provenance');
            var bodyNode = document.getElementById("flow-value-modal-body");
            var copyButton = document.getElementById("flow-value-modal-copy");
            var copyStatus = document.getElementById("flow-value-modal-copy-status");
            var statusNode = document.getElementById("flow-value-modal-status");
            var retryButton = document.getElementById("flow-value-modal-retry");
            var modalOpener = null;
            var requestGeneration = 0;
            var requestController = null;
            var requestTimer = null;
            var selection = null;

            function setProvenance(link, row) {
              if (!provenance) { return; }
              var context = document.querySelector('[data-flow-workflow]') || document.body;
              function field(name, fallback) {
                var key = 'data-flow-value-' + name;
                if (link && link.hasAttribute(key)) { return link.getAttribute(key); }
                if (row && row.hasAttribute(key)) { return row.getAttribute(key); }
                return fallback;
              }
              var source = field('source', '');
              var fields = [['Source', source === 'historical' ? 'Historical event' : source === 'current' ? 'Current record' : 'Value reference']];
              var workflow = field('workflow', context.getAttribute('data-flow-workflow'));
              var partition = field('partition', context.getAttribute('data-flow-partition'));
              if (workflow !== null && workflow !== undefined) { fields.push(['Workflow', workflow]); }
              if (partition !== null && partition !== undefined) { fields.push(['Partition', partition === '' ? 'Automatic routing' : partition]); }
              if (source === 'historical') {
                [['event', 'Event'], ['action', 'Action'], ['time', 'Occurred']].forEach(function (entry) {
                  var value = field(entry[0], '');
                  if (value) { fields.push([entry[1], value]); }
                });
              }
              provenance.replaceChildren();
              fields.forEach(function (entry) {
                var group = document.createElement('div');
                var term = document.createElement('dt');
                var value = document.createElement('dd');
                term.textContent = entry[0];
                value.textContent = entry[1];
                group.append(term, value);
                provenance.appendChild(group);
              });
            }

            function cancelValueRequest() {
              requestGeneration += 1;
              if (requestController) { requestController.abort(); requestController = null; }
              if (requestTimer) { window.clearTimeout(requestTimer); requestTimer = null; }
            }

            function setValueState(state, value, message, truncated) {
              modal.dataset.state = state;
              modal.setAttribute("aria-busy", state === "loading" ? "true" : "false");
              if (bodyNode) { bodyNode.textContent = state === "ready" ? value : ""; bodyNode.hidden = state !== "ready"; }
              if (statusNode) { statusNode.textContent = message || ""; statusNode.hidden = !message; }
              if (copyButton) {
                copyButton.disabled = state !== "ready";
                copyButton.textContent = truncated ? "Copy preview" : "Copy";
              }
              if (retryButton) { retryButton.hidden = !selection || (state !== "missing" && state !== "error"); }
              setCopyStatus("");
            }

            function setCopyStatus(text) {
              if (copyStatus) { copyStatus.textContent = text || ""; }
            }

            function closeModal() {
              cancelValueRequest();
              selection = null;
              modal.close();
              modal.hidden = true;
              setCopyStatus("");
              if (modalOpener && modalOpener.isConnected) { modalOpener.focus(); }
              modalOpener = null;
            }

            function showModal(link) {
              if (!modal.open) {
                modalOpener = link && typeof link.focus === "function" ? link : document.activeElement;
                modal.hidden = false;
                modal.showModal();
              }
              modal.querySelector("button[data-flow-value-modal-close]").focus();
            }

            function copyValue() {
              if (modal.dataset.state !== "ready") { return; }
              var generation = requestGeneration;
              var text = bodyNode ? bodyNode.textContent : "";
              window.dashboardCopyText(text, { container: modal, restoreFocus: copyButton }).then(function (copied) {
                if (generation === requestGeneration && modal.open) {
                  setCopyStatus(copied ? "Copied" : "Copy failed. Select and copy the value manually.");
                }
              });
            }

            function openFromRow(row, link) {
              setProvenance(link, row);
              var preview = row ? row.querySelector("[data-flow-value-preview]") : null;
              var ref = link.getAttribute("data-flow-value-ref") || (row && row.getAttribute("data-flow-value-ref")) || link.getAttribute("title") || "";
              var label = link.getAttribute("data-flow-value-label") || (row && row.getAttribute("data-flow-value-label")) || link.textContent || "value";
              if (!preview || row.getAttribute("data-flow-value-state") !== "ready") {
                return openFromRef(ref, label, link);
              }

              cancelValueRequest();
              selection = null;
              if (refNode) { refNode.textContent = label + " · " + ref; }
              var truncated = row.getAttribute("data-flow-value-truncated") === "true";
              setValueState("ready", preview.textContent, truncated ? "Preview truncated to 8 KiB." : "", truncated);
              showModal(link);
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
                var encoded = anchor.split(":event:")[0].slice("flow-value-".length).replace(/-/g, "+").replace(/_/g, "/");
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
              if (!partition && sourceUrl.pathname === window.location.pathname) {
                partition = new URLSearchParams(window.location.search).get("partition_key");
              }
              if (partition) { params.set("partition_key", partition); }
              ["history_count", "history_before", "history_after", "history_event"].forEach(function (key) {
                var value = sourceUrl.searchParams.get(key);
                if (!value && sourceUrl.pathname === window.location.pathname) {
                  value = new URLSearchParams(window.location.search).get(key);
                }
                if (value) { params.set(key, value); }
              });

              return "/dashboard/api/flow/value?" + params.toString();
            }

            function openFromRef(ref, label, link) {
              var url = flowValueRequestUrl(ref, link);
              if (!ref || !url) { return false; }

              cancelValueRequest();
              var generation = requestGeneration;
              var controller = new AbortController();
              requestController = controller;
              selection = { ref: ref, label: label, link: link };
              setProvenance(link, null);
              if (refNode) { refNode.textContent = (label || "value") + " · " + ref; }
              setValueState("loading", "", "Loading value...");
              showModal(link);
              requestTimer = window.setTimeout(function () {
                if (generation !== requestGeneration || !modal.open) { return; }
                cancelValueRequest();
                setValueState("error", "", "Value request timed out. Retry to load it again.");
              }, 15000);

              fetch(url, {
                cache: "no-store",
                signal: controller.signal,
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (generation !== requestGeneration || !modal.open) { return null; }
                  if (response.status === 401) {
                    var next = new URL(window.location.href);
                    var anchor = link && flowValueAnchorFromHref(link);
                    if (anchor) { next.hash = anchor; }
                    window.location.assign("/dashboard/login?next=" + encodeURIComponent(next.pathname + next.search + next.hash));
                    return null;
                  }
                  if (!response.ok) { throw new Error("Value request failed. Retry to load it again."); }
                  return response.json();
                })
                .then(function (payload) {
                  if (generation !== requestGeneration || !modal.open || !payload) { return; }
                  if (payload.status === "missing") {
                    setValueState("missing", "", "No stored value is available for this reference.");
                  } else if (payload.status === "ok" && typeof payload.value === "string") {
                    setValueState("ready", payload.value, payload.truncated ? "Preview truncated to 8 KiB." : (payload.value === "" ? "Empty value." : ""), payload.truncated);
                  } else {
                    throw new Error(payload.error || "Value unavailable. Retry to load it again.");
                  }
                })
                .catch(function (error) {
                  if (generation !== requestGeneration || !modal.open || error.name === "AbortError") { return; }
                  setValueState("error", "", error.message || "Value unavailable. Retry to load it again.");
                })
                .finally(function () {
                  if (generation !== requestGeneration) { return; }
                  window.clearTimeout(requestTimer);
                  requestTimer = null;
                  requestController = null;
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

              var row = document.getElementById(anchor) || document.getElementById(anchor.split(":event:")[0]);
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

              var anchor = decodeDashboardHash(hash.slice(1));
              if (!anchor) { return; }
              var link = findValueLinkForAnchor(anchor);
              var row = document.getElementById(anchor) || (link && document.getElementById(anchor.split(":event:")[0]));

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
              if (link && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey && openFromLink(link)) {
                event.preventDefault();
              }
            });

            modal.addEventListener("cancel", function (event) {
              event.preventDefault();
              closeModal();
            });

            if (retryButton) {
              retryButton.addEventListener("click", function () {
                if (selection && modal.dataset.state !== "loading") {
                  openFromRef(selection.ref, selection.label, selection.link);
                }
              });
            }

            modal.addEventListener("keydown", function (event) {
              if (event.key !== "Tab") { return; }
              var controls = Array.from(modal.querySelectorAll("button:not([disabled]), [href], [tabindex='0']"))
                .filter(function (node) { return node.getClientRects().length > 0; });
              var first = controls[0];
              var last = controls[controls.length - 1];
              if (event.shiftKey && document.activeElement === first) {
                event.preventDefault();
                last.focus();
              } else if (!event.shiftKey && document.activeElement === last) {
                event.preventDefault();
                first.focus();
              }
            });

            if (copyButton) {
              copyButton.addEventListener("click", copyValue);
            }

            window.addEventListener("hashchange", openFromHash);
            openFromHash();
          }

          var activeJournalMode = (function () {
            try {
              var mode = localStorage.getItem("ferricstore_journal_mode");
              return mode === "table" ? "table" : "tree";
            } catch (_e) { return "tree"; }
          })();
          var activeSelectedStepId = null;

          function selectJournalStepFromHash() {
            var anchor = decodeDashboardHash((window.location.hash || "").slice(1));
            var target = document.getElementById(anchor);
            if (target) {
              var parent = target.parentElement;
              while (parent) {
                if (parent.tagName === "DETAILS") { parent.open = true; }
                parent = parent.parentElement;
              }
              if (target.tagName === "DETAILS") { target.open = true; }
            }
            if (anchor.indexOf("journal-flow-event-") !== 0) { return; }

            var step = document.getElementById(anchor);
            if (!step || !step.classList.contains("journal-step")) { return; }

            activeJournalMode = "tree";
            activeSelectedStepId = step.getAttribute("data-flow-event-id");
            applyJournalState(document);
          }

          function applyJournalState(container) {
            var root = container || document;
            var cards = root.matches && root.matches(".flow-journal-card") ? [root] : root.querySelectorAll(".flow-journal-card");
            cards.forEach(function (card) {
              var buttons = card.querySelectorAll("[data-journal-view-toggle]");
              buttons.forEach(function (btn) {
                var match = btn.getAttribute("data-journal-view-toggle") === activeJournalMode;
                btn.classList.toggle("active", match);
                btn.setAttribute("aria-selected", match ? "true" : "false");
                btn.tabIndex = match ? 0 : -1;
              });

              var views = card.querySelectorAll("[data-journal-view]");
              views.forEach(function (view) {
                var match = view.getAttribute("data-journal-view") === activeJournalMode;
                view.hidden = !match;
              });

              var steps = card.querySelectorAll(".journal-step");
              var hasSelectedStep = false;
              steps.forEach(function (step) {
                var isSelected = step.getAttribute("data-flow-event-id") === activeSelectedStepId;
                hasSelectedStep = hasSelectedStep || isSelected;
                var trigger = step.querySelector(".journal-step-trigger");
                var inspector = trigger && document.getElementById(trigger.getAttribute("aria-controls"));
                step.classList.toggle("is-selected", isSelected);
                if (trigger) { trigger.setAttribute("aria-expanded", isSelected ? "true" : "false"); }
                if (inspector) { inspector.hidden = !isSelected; }
              });
              var workspace = card.querySelector(".flow-journal-workspace");
              var inspectorPanel = card.querySelector(".flow-journal-inspector");
              if (workspace) { workspace.classList.toggle("has-selected-event", hasSelectedStep); }
              if (inspectorPanel) { inspectorPanel.hidden = !hasSelectedStep; }
            });
          }

          function toggleJournalStep(step) {
            var eventId = step && step.getAttribute("data-flow-event-id");
            if (!eventId) { return; }
            activeSelectedStepId = activeSelectedStepId === eventId ? null : eventId;
            applyJournalState(document);
          }

          function setupFlowJournalInteractions() {
            window.addEventListener("hashchange", selectJournalStepFromHash);

            document.addEventListener("click", function (event) {
              var eventLink = event.target.closest('a[href^="#journal-flow-event-"]');
              if (eventLink) {
                var linkedStep = document.getElementById(decodeDashboardHash(eventLink.getAttribute("href").slice(1)));
                if (linkedStep) {
                  activeJournalMode = "tree";
                  activeSelectedStepId = linkedStep.getAttribute("data-flow-event-id");
                  applyJournalState(document);
                }
              }
              var toggleBtn = event.target.closest("[data-journal-view-toggle]");
              if (toggleBtn) {
                event.preventDefault();
                activeJournalMode = toggleBtn.getAttribute("data-journal-view-toggle") || "tree";
                try { localStorage.setItem("ferricstore_journal_mode", activeJournalMode); } catch (_e) {}
                applyJournalState(document);
                return;
              }

              var trigger = event.target.closest(".journal-step-trigger");
              if (trigger) {
                toggleJournalStep(trigger.closest(".journal-step"));
              }
            });

            document.addEventListener("keydown", function (event) {
              var tab = event.target.closest("[data-journal-view-toggle]");
              if (tab && ["ArrowRight", "ArrowDown", "ArrowLeft", "ArrowUp", "Home", "End"].indexOf(event.key) >= 0) {
                var tabs = Array.from(tab.closest("[role=tablist]").querySelectorAll("[data-journal-view-toggle]"));
                var index = tabs.indexOf(tab);
                var backwards = event.key === "ArrowLeft" || event.key === "ArrowUp";
                var next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (backwards ? -1 : 1) + tabs.length) % tabs.length;
                event.preventDefault();
                tabs[next].click();
                tabs[next].focus();
                return;
              }
              var trigger = event.target.closest(".journal-step-trigger");
              if (!trigger) { return; }

              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                toggleJournalStep(trigger.closest(".journal-step"));
              } else if (event.key === "Escape" && activeSelectedStepId) {
                event.preventDefault();
                activeSelectedStepId = null;
                applyJournalState(document);
                trigger.focus();
              }
            });
          }

          var filterDrafts = new WeakMap();
          var filterDraftsReady = false;
          function initializeFilterDrafts() {
            if (!filterDraftsReady) { return; }
            document.querySelectorAll('form.flow-filter-form[method="get"]:not([data-flow-query-form])').forEach(function (form) {
              if (filterDrafts.has(form) || (form.getAttribute('action') || '').endsWith('/lookup')) { return; }
              var notice = document.createElement('p');
              notice.dataset.dashboardFilterDraft = '';
              notice.className = 'dashboard-filter-draft';
              notice.setAttribute('role', 'status');
              notice.hidden = true;
              notice.textContent = 'Filters not applied. Results still use the previous scope.';
              form.appendChild(notice);
              filterDrafts.set(form, { applied: new URLSearchParams(new FormData(form)).toString(), notice: notice });
            });
          }

          function setupFilterDrafts() {
            filterDraftsReady = true;
            initializeFilterDrafts();
            function update(event) {
              var form = event.target.closest('form');
              var draft = form && filterDrafts.get(form);
              if (!draft) { return; }
              Promise.resolve().then(function () {
                draft.notice.hidden = new URLSearchParams(new FormData(form)).toString() === draft.applied;
              });
            }
            document.addEventListener('input', update);
            document.addEventListener('change', update);
            document.addEventListener('reset', function (event) { setTimeout(function () { update(event); }, 0); });
          }

          function setupSidebarPreferences() {
            var key = 'ferricstore.sidebar.groups.v1';
            var groups = Array.from(document.querySelectorAll('.sidebar [data-dashboard-nav-group]'));
            var saved = {};
            try {
              var raw = sessionStorage.getItem(key);
              var parsed = raw && raw.length <= 4096 ? JSON.parse(raw) : null;
              if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) { saved = parsed; }
            } catch (_error) {}
            function saveGroups() {
              var current = {};
              document.querySelectorAll('.sidebar [data-dashboard-nav-group]').forEach(function (item) { current[item.dataset.dashboardNavGroup] = item.open; });
              try { sessionStorage.setItem(key, JSON.stringify(current)); } catch (_error) {}
            }
            groups.forEach(function (group) {
              var name = group.dataset.dashboardNavGroup;
              if (typeof saved[name] === 'boolean') { group.open = saved[name]; }
              if (group.querySelector('[aria-current="page"]')) { group.open = true; }
            });
            document.addEventListener('toggle', function (event) {
              if (event.target.matches('.sidebar [data-dashboard-nav-group]')) { saveGroups(); }
            }, true);
            window.addEventListener('pagehide', saveGroups);
            var active = document.querySelector('.sidebar [aria-current="page"]');
            if (active) {
              var sidebar = active.closest('.sidebar');
              var itemBounds = active.getBoundingClientRect();
              var bounds = sidebar.getBoundingClientRect();
              if (itemBounds.bottom > bounds.bottom) { sidebar.scrollTop += itemBounds.bottom - bounds.bottom + 8; }
              else if (itemBounds.top < bounds.top) { sidebar.scrollTop -= bounds.top - itemBounds.top + 8; }
            }
          }

          var characterShortcuts = true;
          function syncShortcutPreferences() {
            document.querySelectorAll('[data-dashboard-character-shortcuts]').forEach(function (preference) {
              preference.checked = characterShortcuts;
            });
          }

          function setupGlobalShortcutsAndCopy() {
            try { characterShortcuts = localStorage.getItem('ferricstore_character_shortcuts') !== 'off'; } catch (_error) {}
            syncShortcutPreferences();
            document.addEventListener('change', function (event) {
              if (event.target.matches('[data-dashboard-character-shortcuts]')) {
                characterShortcuts = event.target.checked;
                try { localStorage.setItem('ferricstore_character_shortcuts', characterShortcuts ? 'on' : 'off'); } catch (_error) {}
                syncShortcutPreferences();
              }
            });
            document.addEventListener("click", async function (event) {
              var copyBtn = event.target.closest(".copy-btn-inline");
              if (copyBtn) {
                event.preventDefault();
                var text = copyBtn.getAttribute("data-copy-text") || copyBtn.textContent;
                var originalHtml = copyBtn.innerHTML;
                var originalTitle = copyBtn.title;
                if (copyBtn.dataset.copyPending === '1') { return; }
                copyBtn.dataset.copyPending = '1';
                try {
                  if (!await window.dashboardCopyText(text, { restoreFocus: copyBtn })) { throw new Error('Clipboard unavailable'); }
                  copyBtn.classList.add("copied");
                  copyBtn.textContent = "Copied";
                } catch (_error) {
                  copyBtn.textContent = "Copy failed";
                  copyBtn.title = "Clipboard unavailable. Select and copy the value manually.";
                }
                copyBtn.setAttribute('aria-live', 'polite');
                setTimeout(function () {
                  copyBtn.classList.remove("copied");
                  copyBtn.innerHTML = originalHtml;
                  copyBtn.title = originalTitle;
                  delete copyBtn.dataset.copyPending;
                }, 1400);
              }

              if (event.target.closest('[data-dashboard-refresh]')) {
                event.preventDefault();
                var refreshEvent = new CustomEvent('dashboard:before-refresh', { cancelable: true });
                if (document.dispatchEvent(refreshEvent)) { window.location.reload(); }
              }
              var download = event.target.closest('[data-dashboard-download-json]');
              if (download) {
                var data = document.getElementById(download.dataset.dashboardDownloadJson);
                var downloadStatus = download.parentElement.querySelector('[data-dashboard-download-status]');
                var objectUrl = null;
                try {
                  if (!data || data.type !== 'application/json') { throw new Error('Missing result'); }
                  var blob = new Blob([data.textContent], { type: 'application/json' });
                  objectUrl = URL.createObjectURL(blob);
                  var link = document.createElement('a');
                  link.href = objectUrl;
                  link.download = 'ferricstore-query-page.json';
                  link.click();
                  if (downloadStatus) { downloadStatus.textContent = 'Download started'; }
                } catch (_error) {
                  if (downloadStatus) { downloadStatus.textContent = 'Download failed. Try again.'; }
                } finally {
                  if (objectUrl) { setTimeout(function () { URL.revokeObjectURL(objectUrl); }, 1000); }
                }
              }
              if (event.target.closest('[data-dashboard-shortcuts-open]')) {
                var help = document.getElementById('keyboard-shortcuts-modal');
                if (help && !help.open) { help.showModal(); }
              }
              var closeKeyboardModal = event.target.closest("[data-keyboard-modal-close]");
              if (closeKeyboardModal) {
                var modal = document.getElementById("keyboard-shortcuts-modal");
                if (modal) { modal.close(); }
              }
            });

            var pendingKey = "";
            document.addEventListener("keydown", function (event) {
              var activeModal = document.getElementById('keyboard-shortcuts-modal');
              if (activeModal && activeModal.open && event.key === 'Tab') {
                var buttons = Array.from(activeModal.querySelectorAll('button:not([disabled]), input:not([disabled]), a[href], [tabindex="0"]'));
                var first = buttons[0];
                var last = buttons[buttons.length - 1];
                if ((!event.shiftKey && document.activeElement === last) || (event.shiftKey && document.activeElement === first)) {
                  event.preventDefault();
                  (event.shiftKey ? last : first).focus();
                }
                return;
              }
              if (document.querySelector('dialog[open]')) { pendingKey = ''; return; }
              var tag = event.target.tagName;
              if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
                if (event.key === "Escape") { event.target.blur(); }
                return;
              }

              if (!characterShortcuts || event.ctrlKey || event.metaKey || event.altKey || event.target.isContentEditable) { pendingKey = ''; return; }

              var modal = document.getElementById("keyboard-shortcuts-modal");
              if (event.key === "?" || (event.key === "/" && event.shiftKey)) {
                event.preventDefault();
                if (modal) { if (modal.open) { modal.close(); } else { modal.showModal(); } }
                return;
              }

              if (event.key === "Escape") {
                if (modal && modal.open) { modal.close(); return; }
              }

              if (event.key === "/") {
                var search = Array.from(document.querySelectorAll("input.flow-search-input:not([type=hidden]):not([disabled])"))
                  .find(function (input) { return input.getClientRects().length > 0; });
                if (search) {
                  event.preventDefault();
                  search.focus();
                  if (typeof search.select === 'function') { search.select(); }
                }
                return;
              }

              if (event.key === "g") {
                pendingKey = "g";
                setTimeout(function () { pendingKey = ""; }, 1000);
                return;
              }

              if (pendingKey === "g") {
                pendingKey = "";
                if (event.key === "f") { navigateScoped("/dashboard/flow/query"); }
                if (event.key === "d") { navigateScoped("/dashboard/flow/due"); }
                if (event.key === "w") { navigateScoped("/dashboard/flow/workers"); }
                if (event.key === "l") { navigateScoped("/dashboard/flow/lineage"); }
                if (event.key === "o") { window.location.href = "/dashboard"; }
              }
            });
          }

          // Capture applicable fields after all page-specific DOM-ready controls settle.
          if (document.readyState === 'complete') {
            setTimeout(setupFilterDrafts, 0);
          } else {
            document.addEventListener('DOMContentLoaded', function () { setTimeout(setupFilterDrafts, 0); }, { once: true });
          }

          onReady(function () {
            var returnedDraftSubmitting = false;
            window.addEventListener('beforeunload', function (event) {
              if (!returnedDraftSubmitting && document.querySelector('form[data-dashboard-returned-draft="true"]')) {
                event.preventDefault();
                event.returnValue = '';
              }
            });
            document.addEventListener('submit', function (event) {
              if (!event.target.matches('form[data-dashboard-returned-draft="true"]')) { return; }
              queueMicrotask(function () { returnedDraftSubmitting = !event.defaultPrevented; });
            });
            window.addEventListener('pageshow', function () { returnedDraftSubmitting = false; });
            ensureSkipLink();
            document.querySelectorAll("[data-dashboard-instance]").forEach(function (instance) {
              instance.textContent = window.location.host;
              instance.title = window.location.origin;
            });
            clearTransientQueryParams();
            setupFlowValueInspector();
            setupWorkflowActionNavigation();
            setupFlowJournalInteractions();
            setupSidebarPreferences();
            setupGlobalShortcutsAndCopy();
            setupMetricHelp();
            preserveWorkflowScope();
            applyTableFilters();
            updateDisclosureCounts();
            setupSnapshot();
            setupAccountProfiles();
            document.addEventListener('input', function (event) {
              if (event.target.matches('[data-dashboard-table-filter]')) {
                tableFilterValues.set(event.target.dataset.dashboardFilterTarget, event.target.value);
                applyTableFilters();
              }
            });
            applyJournalState(document);
            selectJournalStepFromHash();

            document.addEventListener("submit", function (event) {
              var form = event.target.closest && event.target.closest("[data-dashboard-single-submit]");
              if (!form) { return; }
              if (event.defaultPrevented) { return; }
              if (form.closest('[data-flow-action-stale-state="true"]')) { event.preventDefault(); return; }
              if (form.dataset.dashboardSubmitting === "1") {
                event.preventDefault();
                return;
              }
              form.dataset.dashboardSubmitting = "1";
              form.setAttribute('aria-busy', 'true');
              var submitter = event.submitter;
              if (submitter && submitter.name) {
                var submittedAction = document.createElement('input');
                submittedAction.type = 'hidden';
                submittedAction.name = submitter.name;
                submittedAction.value = submitter.value;
                submittedAction.setAttribute('data-dashboard-submitted-action', '');
                form.appendChild(submittedAction);
              }
              var buttons = form.querySelectorAll("button[type=submit], input[type=submit]");
              for (var i = 0; i < buttons.length; i += 1) {
                if (!buttons[i].disabled) { buttons[i].setAttribute('data-dashboard-submit-disabled', ''); buttons[i].disabled = true; }
              }
            });
            window.addEventListener('pageshow', function (event) {
              if (!event.persisted) { return; }
              document.querySelectorAll('[data-dashboard-single-submit]').forEach(function (form) {
                delete form.dataset.dashboardSubmitting;
                form.removeAttribute('aria-busy');
                form.querySelectorAll('[data-dashboard-submitted-action]').forEach(function (input) { input.remove(); });
                form.querySelectorAll('[data-dashboard-submit-disabled]').forEach(function (button) {
                  button.removeAttribute('data-dashboard-submit-disabled');
                  button.disabled = !!form.closest('[data-flow-action-stale-state="true"]');
                });
              });
            });

            var root = document.body;
            if (!root || !root.dataset || !root.dataset.dashboardLiveUrl) { return; }

            var url = root.dataset.dashboardLiveUrl;
            var intervalMs = parseInt(root.dataset.dashboardLiveIntervalMs || "2000", 10);
            if (!Number.isFinite(intervalMs) || intervalMs < 500) { intervalMs = 2000; }

            var inFlight = false;
            var detailUnavailable = false;
            var failureCount = 0;
            var lastSuccessAt = null;
            var timer = null;
            var ageTimer = null;
            var userPaused = false;
            var requestController = null;
            var requestTimer = null;
            var requestGeneration = 0;
            var requestTimeoutMs = 15000;
            var status = document.createElement("div");
            status.className = "dashboard-live-status";
            status.setAttribute("data-dashboard-live-status", "connecting");
            status.setAttribute("role", "status");
            status.setAttribute("aria-live", "polite");
            status.innerHTML = '<span class="dashboard-live-dot" aria-hidden="true"></span>' +
              '<span data-dashboard-live-message>Connecting</span>' +
              '<span data-dashboard-live-age aria-live="off"></span>' +
              '<button type="button" data-dashboard-live-retry hidden>Retry</button>' +
              '<button type="button" data-dashboard-live-toggle aria-label="Pause live updates">Pause</button>';

            function ensureLiveStatusMounted() {
              if (status.isConnected) { return; }
              var statusHost = document.querySelector(".subpage-header") || document.querySelector(".top-bar") || root;
              statusHost.appendChild(status);
            }

            ensureLiveStatusMounted();

            var statusMessage = status.querySelector("[data-dashboard-live-message]");
            var retryButton = status.querySelector("[data-dashboard-live-retry]");
            var ageLabel = status.querySelector("[data-dashboard-live-age]");
            var toggleButton = status.querySelector("[data-dashboard-live-toggle]");

            function formatFreshness() {
              if (!lastSuccessAt) { return "No successful refresh"; }
              var ageSeconds = Math.max(0, Math.floor((Date.now() - lastSuccessAt) / 1000));
              if (ageSeconds < 5) { return "Updated just now"; }
              if (ageSeconds < 60) { return "Updated " + ageSeconds + "s ago"; }
              return "Updated " + Math.floor(ageSeconds / 60) + "m ago";
            }

            function setLiveStatus(state, message, canRetry) {
              status.setAttribute("data-dashboard-live-status", state);
              statusMessage.textContent = message;
              ageLabel.textContent = formatFreshness();
              retryButton.hidden = !canRetry;
              retryButton.disabled = inFlight && !canRetry;
              var paused = userPaused || state === 'paused';
              toggleButton.textContent = paused ? 'Resume' : 'Pause';
              toggleButton.setAttribute('aria-label', paused ? 'Resume live updates' : 'Pause live updates');
              toggleButton.hidden = state === 'unavailable' || state === 'expired';
            }

            function watchFreshness() {
              var freshness = formatFreshness();
              if (ageLabel.textContent !== freshness) { ageLabel.textContent = freshness; }
              if (status.dataset.dashboardLiveStatus === 'live' && lastSuccessAt &&
                  Date.now() - lastSuccessAt > Math.max(intervalMs * 3, 5000)) {
                setLiveStatus('stale', 'Stale', true);
              }
              ageTimer = window.setTimeout(watchFreshness, 1000);
            }

            function cancelRequest() {
              requestGeneration += 1;
              if (requestTimer) { window.clearTimeout(requestTimer); requestTimer = null; }
              if (requestController) { requestController.abort(); requestController = null; }
              inFlight = false;
            }

            function pauseMessage() {
              var active = document.activeElement;
              return active && active.closest('input, textarea, select') ? 'Updates paused while editing' : 'Updates paused while interacting';
            }

            function schedule(delayMs) {
              if (timer) { window.clearTimeout(timer); }
              timer = window.setTimeout(tick, delayMs);
            }

            function retryDelayMs() {
              var exponential = intervalMs * Math.pow(2, failureCount - 1);
              var bounded = Math.min(exponential, 60000);
              return bounded + Math.floor(Math.random() * Math.max(1, bounded * 0.15));
            }

            function redirectToLogin() {
              var next = window.location.pathname + window.location.search + window.location.hash;
              window.location.assign("/dashboard/login?next=" + encodeURIComponent(next));
            }

            function tick() {
              if (detailUnavailable) { return; }
              if (userPaused) { setLiveStatus('paused', 'Updates paused', false); return; }
              if (document.hidden) { schedule(intervalMs); return; }
              if (inFlight) { return; }
              if (dashboardInteractionPaused()) {
                setLiveStatus("paused", pauseMessage(), false);
                schedule(intervalMs);
                return;
              }
              inFlight = true;
              retryButton.disabled = status.dataset.dashboardLiveStatus !== 'stale';
              var generation = ++requestGeneration;
              requestController = new AbortController();
              requestTimer = window.setTimeout(function () {
                if (generation !== requestGeneration) { return; }
                cancelRequest();
                failureCount += 1;
                root.dataset.dashboardLiveError = '1';
                setLiveStatus('stale', 'Refresh timed out', true);
                schedule(retryDelayMs());
              }, requestTimeoutMs);

              fetch(url, {
                cache: "no-store",
                signal: requestController.signal,
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (generation !== requestGeneration) { return null; }
                  if (response.status === 401) {
                    setLiveStatus("expired", "Session expired", false);
                    redirectToLogin();
                    throw { dashboardSessionExpired: true };
                  }
                  if (!response.ok) { throw new Error("dashboard live request failed"); }
                  return response.json();
                })
                .then(function (payload) {
                  if (generation !== requestGeneration || !payload) { return; }
                  updateActionSnapshot(payload.action_snapshot);
                  if (payload.detail_unavailable) {
                    detailUnavailable = true;
                    patchComponents(payload.components);
                    document.querySelectorAll('#workflow-timeline, #workflow-metadata, .flow-debug-disclosure, #workflow-actions').forEach(function (section) { section.remove(); });
                    setLiveStatus('unavailable', 'Workflow unavailable. Refresh to retry.', true);
                    return;
                  }
                  var componentsPatched = patchComponents(payload.components);
                  ensureLiveStatusMounted();
                  if (componentsPatched) {
                    lastSuccessAt = Math.min(Number(payload.generated_at_ms) || Date.now(), Date.now());
                    root.dataset.dashboardLiveLastUpdateMs = String(lastSuccessAt);
                    root.dataset.dashboardLiveError = "";
                    failureCount = 0;
                    setLiveStatus("live", "Live", false);
                  } else {
                    setLiveStatus("paused", pauseMessage(), false);
                  }
                  schedule(intervalMs);
                })
                .catch(function (error) {
                  if (generation !== requestGeneration) { return; }
                  if (error && error.dashboardSessionExpired) { return; }
                  failureCount += 1;
                  root.dataset.dashboardLiveError = "1";
                  setLiveStatus("stale", "Stale", true);
                  schedule(retryDelayMs());
                })
                .finally(function () {
                  if (generation !== requestGeneration) { return; }
                  window.clearTimeout(requestTimer);
                  requestTimer = null;
                  requestController = null;
                  inFlight = false;
                  retryButton.disabled = false;
                });
            }

            retryButton.addEventListener("click", function () {
              if (detailUnavailable) { window.location.reload(); return; }
              cancelRequest();
              failureCount = 0;
              setLiveStatus("connecting", "Retrying", false);
              schedule(0);
            });

            toggleButton.addEventListener('click', function () {
              userPaused = status.dataset.dashboardLiveStatus !== 'paused';
              if (userPaused) {
                cancelRequest();
                if (timer) { window.clearTimeout(timer); timer = null; }
                setLiveStatus('paused', 'Updates paused', false);
              } else {
                setLiveStatus('connecting', 'Resuming', false);
                schedule(0);
              }
            });

            document.addEventListener("visibilitychange", function () {
              if (!document.hidden) { schedule(0); }
            });

            window.addEventListener('pagehide', function () {
              cancelRequest();
              window.clearTimeout(timer);
              window.clearTimeout(ageTimer);
            });
            window.addEventListener('pageshow', function (event) {
              if (event.persisted) { watchFreshness(); schedule(0); }
            });

            watchFreshness();
            tick();
          });
        }());
        #{FerricstoreServer.Health.Dashboard.Render.RecentRates.script()}
      </script>
    """
  end

  def render_subpage_header(title) do
    """
    <div class="subpage-header">
      <a class="dashboard-brand" href="/dashboard">FerricStore</a>
      <h1 class="subpage-title">#{escape(title)}</h1>
      <span class="dashboard-instance mono" data-dashboard-instance aria-label="Connected instance"></span>
      #{render_snapshot()}
    </div>
    """
  end

  def render_page_intro(title, body) do
    """
    <section class="page-intro" aria-label="#{escape_attr(title)} page purpose">
      <p>#{escape(body)}</p>
    </section>
    """
  end

  def render_dashboard_disclosure(title, badge, content, opts \\ [])
      when is_binary(title) and is_binary(content) and is_list(opts) do
    open_attr = if Keyword.get(opts, :open, false), do: " open", else: ""

    count_attr =
      case Keyword.get(opts, :live_count) do
        nil -> ""
        component -> ~s( data-dashboard-disclosure-count="#{escape_attr(component)}")
      end

    """
    <details class="dashboard-disclosure"#{open_attr}>
      <summary><span>#{escape(title)}</span><span class="badge badge-idle"#{count_attr}>#{escape(to_string(badge))}</span></summary>
      <div class="dashboard-disclosure-body">#{content}</div>
    </details>
    """
  end

  def render_kv_subnav(active) do
    links =
      Enum.map_join(kv_sections(), "\n", fn {key, href, label, title} ->
        active_class = if key == active, do: " active", else: ""
        current = if key == active, do: ~s( aria-current="page"), else: ""

        ~s(<a class="flow-tab#{active_class}" href="#{href}"#{current} title="#{escape_attr(title)}">#{escape(label)}</a>)
      end)

    ~s(<nav class="flow-tabs" aria-label="KV dashboard sections">#{links}</nav>)
  end

  defp kv_sections do
    [
      {"keyspace", "/dashboard/keyspace", "Keyspace", "Find keys and inspect metadata"},
      {"prefixes", "/dashboard/prefixes", "Prefixes", "Sampled prefix distribution"},
      {"reads", "/dashboard/reads", "Read Path", "Hot-cache and cold-read health"},
      {"storage", "/dashboard/storage", "Storage", "Disk files, segments, and shard usage"},
      {"commands", "/dashboard/commands", "Command Catalog",
       "Traffic, slowlog, and command groups"}
    ]
  end

  def render_messaging_subnav(active) do
    items = [
      {"streams", "/dashboard/streams", "Streams", "Append and stream mutation activity"},
      {"pubsub", "/dashboard/pubsub", "Pub/Sub", "Active subscriptions and publish activity"}
    ]

    links =
      Enum.map_join(items, "\n", fn {key, href, label, title} ->
        active_class = if key == active, do: " active", else: ""
        current = if key == active, do: ~s( aria-current="page"), else: ""

        ~s(<a class="flow-tab#{active_class}" href="#{href}"#{current} title="#{escape_attr(title)}">#{escape(label)}</a>)
      end)

    ~s(<nav class="flow-tabs" aria-label="Messaging dashboard sections">#{links}</nav>)
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
      "streams" => "",
      "keyspace" => "",
      "reads" => "",
      "commands" => "",
      "capabilities" => "",
      "security" => "",
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
      "streams" => "",
      "keyspace" => "",
      "reads" => "",
      "commands" => "",
      "capabilities" => "",
      "security" => "",
      "doctor" => ""
    })
  end

  def sidebar_html(active, badges) do
    links = Enum.map_join(sidebar_sections(), "\n", &render_sidebar_section(&1, active, badges))
    session = render_sidebar_session()

    """
    #{render_skip_link()}
    <nav class="sidebar" aria-label="Dashboard sections">
      #{links}
      #{session}
      <button type="button" class="dashboard-help-button" data-dashboard-shortcuts-open>Keyboard help</button>
    </nav>
    #{render_keyboard_shortcuts_modal()}
    """
  end

  def render_skip_link do
    ~s(<a class="dashboard-skip-link" href="#dashboard-main">Skip to main content</a>)
  end

  defp render_snapshot do
    """
    <div class="dashboard-snapshot" data-dashboard-snapshot>
      <time data-dashboard-captured-at="#{DateTime.utc_now() |> DateTime.to_iso8601()}">Snapshot #{Calendar.strftime(DateTime.utc_now(), "%H:%M:%S UTC")}</time>
      <a href="" class="flow-search-button" data-dashboard-refresh>Refresh</a>
    </div>
    """
  end

  defp render_sidebar_session do
    if Acl.protected_mode?() do
      """
      <div class="sidebar-session">
        <span>Protected session</span>
        <strong class="mono">#{escape(FerricstoreServer.Health.Endpoint.Session.current_username() || "Not signed in")}</strong>
        <form action="/dashboard/logout" method="post">
          <button type="submit">Sign out</button>
        </form>
      </div>
      """
    else
      ""
    end
  end

  defp sidebar_sections do
    [
      {"System", [{"overview", "/dashboard", "Overview", :primary}]},
      {"Workflows",
       [
         {:subgroup, "workflow-nav-operate", "Operate",
          [
            {"flow", "/dashboard/flow", "Overview", :primary},
            {"flow_states", "/dashboard/flow/states", "States / FIFO", :sub},
            {"flow_due", "/dashboard/flow/due", "Due Work", :sub},
            {"flow_workers", "/dashboard/flow/workers", "Workers", :sub},
            {"flow_schedules", "/dashboard/flow/schedules", "Schedules", :sub}
          ]},
         {:subgroup, "workflow-nav-investigate", "Investigate",
          [
            {"flow_failures", "/dashboard/flow/failures", "Failures", :sub},
            {"flow_query", "/dashboard/flow/query", "Query Studio", :sub},
            {"flow_lineage", "/dashboard/flow/lineage", "Lineage", :sub},
            {"flow_signals", "/dashboard/flow/signals", "Signals", :sub}
          ]},
         {:subgroup, "workflow-nav-configure", "Configure",
          [
            {"flow_policies", "/dashboard/flow/policies", "Policies", :sub},
            {"flow_governance", "/dashboard/flow/governance", "Governance", :sub},
            {"flow_retention", "/dashboard/flow/retention", "Retention", :sub}
          ]}
       ]},
      {"KV / Data",
       Enum.map(kv_sections(), fn {key, href, label, _title} ->
         {key, href, label, if(key == "keyspace", do: :primary, else: :sub)}
       end)},
      {"Messaging",
       [
         {"streams", "/dashboard/streams", "Streams", :primary},
         {"pubsub", "/dashboard/pubsub", "Pub/Sub", :primary}
       ]},
      {"Operations",
       [
         {"slowlog", "/dashboard/slowlog", "Slow Log", :primary},
         {"merge", "/dashboard/merge", "Merge Status", :primary},
         {"clients", "/dashboard/clients", "Clients", :primary},
         {"raft", "/dashboard/raft", "Consensus", :primary}
       ]},
      {"Control Plane",
       [
         {"security", "/dashboard/security", "Security", :primary},
         {"capabilities", "/dashboard/capabilities", "Capabilities", :primary},
         {"config", "/dashboard/config", "Config", :primary},
         {"doctor", "/dashboard/doctor", "Doctor", :primary}
       ]}
    ]
  end

  defp render_sidebar_section({section, items}, active, badges) do
    open? = Enum.any?(items, &sidebar_entry_active?(&1, active))

    open_attr = if open?, do: " open", else: ""

    rendered_items = Enum.map_join(items, "\n", &render_sidebar_entry(&1, active, badges))

    """
    <details class="nav-group" data-dashboard-nav-group="#{escape_attr(section)}"#{open_attr}>
      <summary>#{escape(section)}</summary>
      <div class="nav-group-links">#{rendered_items}</div>
    </details>
    """
  end

  defp sidebar_entry_active?({:subgroup, _id, _label, items}, active),
    do: Enum.any?(items, &sidebar_entry_active?(&1, active))

  defp sidebar_entry_active?({key, _href, _label, _depth}, active),
    do: sidebar_item_active?(key, active)

  defp render_sidebar_entry({:subgroup, id, label, items}, active, badges) do
    links = Enum.map_join(items, "\n", &render_sidebar_entry(&1, active, badges))

    """
    <div class="nav-subgroup" role="group" aria-labelledby="#{escape_attr(id)}">
      <div class="nav-subgroup-label" id="#{escape_attr(id)}">#{escape(label)}</div>
      #{links}
    </div>
    """
  end

  defp render_sidebar_entry({key, href, label, depth}, active, badges),
    do: render_sidebar_link(key, href, label, depth, active, badges)

  defp render_sidebar_link(key, href, label, depth, active, badges) do
    active? = sidebar_item_active?(key, active)
    active_class = if active?, do: " active", else: ""
    depth_class = if depth == :sub, do: " nav-subitem", else: ""
    current_attr = if active?, do: ~s( aria-current="page"), else: ""
    badge_val = Map.get(badges, key, "")

    badge_html =
      if badge_val != "" and badge_val != nil do
        ~s(<span class="nav-badge">#{escape(to_string(badge_val))}</span>)
      else
        ""
      end

    ~s(<a class="#{active_class}#{depth_class}" href="#{href}"#{current_attr}><span class="nav-label">#{escape(label)}</span>#{badge_html}</a>)
  end

  def render_keyboard_shortcuts_modal do
    """
    <dialog id="keyboard-shortcuts-modal" class="keyboard-modal" aria-label="Keyboard Shortcuts">
      <div class="keyboard-card">
        <div class="keyboard-header">
          <div class="keyboard-title">Keyboard Shortcuts</div>
          <button type="button" class="dashboard-modal-close" data-keyboard-modal-close aria-label="Close shortcuts dialog">Close</button>
        </div>
        <label class="keyboard-shortcut-setting">
          <input type="checkbox" data-dashboard-character-shortcuts checked>
          Enable character shortcuts
        </label>
        <div class="keyboard-row">
          <span>Focus search (when available)</span>
          <div class="keyboard-keys"><kbd>/</kbd></div>
        </div>
        <div class="keyboard-row">
          <span>Go to Query Studio</span>
          <div class="keyboard-keys"><kbd>G</kbd> <kbd>F</kbd></div>
        </div>
        <div class="keyboard-row">
          <span>Go to Due Work</span>
          <div class="keyboard-keys"><kbd>G</kbd> <kbd>D</kbd></div>
        </div>
        <div class="keyboard-row">
          <span>Go to Workers</span>
          <div class="keyboard-keys"><kbd>G</kbd> <kbd>W</kbd></div>
        </div>
        <div class="keyboard-row">
          <span>Go to Lineage</span>
          <div class="keyboard-keys"><kbd>G</kbd> <kbd>L</kbd></div>
        </div>
        <div class="keyboard-row">
          <span>Go to System Overview</span>
          <div class="keyboard-keys"><kbd>G</kbd> <kbd>O</kbd></div>
        </div>
        <div class="keyboard-row">
          <span>Close any modal or dialog</span>
          <div class="keyboard-keys"><kbd>Esc</kbd></div>
        </div>
      </div>
    </dialog>
    """
  end

  defp sidebar_item_active?("flow", "flow_detail"), do: true
  defp sidebar_item_active?(key, active), do: key == active
end
