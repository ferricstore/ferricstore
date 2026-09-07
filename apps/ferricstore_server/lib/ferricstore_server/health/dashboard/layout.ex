defmodule FerricstoreServer.Health.Dashboard.Layout do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  alias FerricstoreServer.Acl
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
      <link rel="preconnect" href="https://fonts.googleapis.com">
      <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
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
            var modal = document.getElementById("flow-value-modal");
            if (modal && !modal.hidden) { return true; }

            var selection = window.getSelection && window.getSelection();
            if (selection && !selection.isCollapsed) { return true; }

            var active = document.activeElement;
            if (!active || !active.closest) { return false; }
            return !!active.closest("input, textarea, select, [data-dashboard-live-pause], .table-scroll, .flow-journal-card");
          }

          function patchComponents(components) {
            if (!components || dashboardInteractionPaused()) { return false; }
            Object.keys(components).forEach(function (name) {
              var target = findComponent(name);
              var nextHtml = components[name];
              if (!target || typeof nextHtml !== "string") { return; }
              if (target.innerHTML !== nextHtml) {
                var openNavGroups = [];
                var openDisclosures = [];
                target.querySelectorAll("details[data-dashboard-disclosure-key][open]").forEach(function (group) {
                  openDisclosures.push(group.getAttribute("data-dashboard-disclosure-key"));
                });
                target.querySelectorAll("details[data-dashboard-nav-group][open]").forEach(function (group) {
                  openNavGroups.push(group.getAttribute("data-dashboard-nav-group"));
                });
                target.innerHTML = nextHtml;
                openDisclosures.forEach(function (key) {
                  var group = target.querySelector('details[data-dashboard-disclosure-key="' + CSS.escape(key) + '"]');
                  if (group) { group.open = true; }
                });
                openNavGroups.forEach(function (groupName) {
                  var group = target.querySelector('details[data-dashboard-nav-group="' + CSS.escape(groupName) + '"]');
                  if (group) { group.open = true; }
                });
                if (typeof applyJournalState === "function") {
                  applyJournalState(target);
                }
              }
            });
            return true;
          }

          function decodeDashboardHash(value) {
            try { return decodeURIComponent(value); }
            catch (_error) { return ""; }
          }

          function setupFlowValueInspector() {
            var modal = document.getElementById("flow-value-modal");
            if (!modal || modal.dataset.bound === "1") { return; }
            modal.dataset.bound = "1";

            var refNode = document.getElementById("flow-value-modal-ref");
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

            function fallbackCopy(text) {
              var textarea = document.createElement("textarea");
              textarea.value = text;
              textarea.setAttribute("readonly", "readonly");
              textarea.style.position = "fixed";
              textarea.style.left = "-9999px";
              modal.appendChild(textarea);
              textarea.select();
              try {
                document.execCommand("copy");
                setCopyStatus("Copied");
              } catch (_error) {
                setCopyStatus("Copy failed");
              } finally {
                textarea.remove();
                if (!modal.hidden && copyButton) { copyButton.focus(); }
              }
            }

            function copyValue() {
              if (modal.dataset.state !== "ready") { return; }
              var generation = requestGeneration;
              var text = bodyNode ? bodyNode.textContent : "";
              if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(text)
                  .then(function () { if (generation === requestGeneration && modal.open) { setCopyStatus("Copied"); } })
                  .catch(function () { if (generation === requestGeneration && modal.open) { fallbackCopy(text); } });
              } else {
                fallbackCopy(text);
              }
            }

            function openFromRow(row, link) {
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
              if (!partition && sourceUrl.pathname === window.location.pathname) {
                partition = new URLSearchParams(window.location.search).get("partition_key");
              }
              if (partition) { params.set("partition_key", partition); }
              ["history_count", "history_before", "history_after"].forEach(function (key) {
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

              var anchor = decodeDashboardHash(hash.slice(1));
              if (!anchor) { return; }
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

          function setupGlobalShortcutsAndCopy() {
            document.addEventListener("click", function (event) {
              var copyBtn = event.target.closest(".copy-btn-inline");
              if (copyBtn) {
                event.preventDefault();
                var text = copyBtn.getAttribute("data-copy-text") || copyBtn.textContent;
                var originalHtml = copyBtn.innerHTML;
                if (navigator.clipboard && navigator.clipboard.writeText) {
                  navigator.clipboard.writeText(text);
                }
                copyBtn.classList.add("copied");
                copyBtn.textContent = "✓ Copied";
                setTimeout(function () {
                  copyBtn.classList.remove("copied");
                  copyBtn.innerHTML = originalHtml;
                }, 1400);
              }

              var closeKeyboardModal = event.target.closest("[data-keyboard-modal-close]");
              if (closeKeyboardModal) {
                var modal = document.getElementById("keyboard-shortcuts-modal");
                if (modal) { modal.hidden = true; }
              }
            });

            var pendingKey = "";
            document.addEventListener("keydown", function (event) {
              var tag = event.target.tagName;
              if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
                if (event.key === "Escape") { event.target.blur(); }
                return;
              }

              var modal = document.getElementById("keyboard-shortcuts-modal");
              if (event.key === "?" || (event.key === "/" && event.shiftKey)) {
                event.preventDefault();
                if (modal) { modal.hidden = !modal.hidden; }
                return;
              }

              if (event.key === "Escape") {
                if (modal && !modal.hidden) { modal.hidden = true; return; }
              }

              if (event.key === "/") {
                var search = document.querySelector(".flow-search-input");
                if (search) {
                  event.preventDefault();
                  search.focus();
                  search.select();
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
                if (event.key === "f") { window.location.href = "/dashboard/flow"; }
                if (event.key === "d") { window.location.href = "/dashboard/flow/due"; }
                if (event.key === "w") { window.location.href = "/dashboard/flow/workers"; }
                if (event.key === "l") { window.location.href = "/dashboard/flow/lineage"; }
                if (event.key === "o") { window.location.href = "/dashboard"; }
              }
            });
          }

          onReady(function () {
            document.querySelectorAll("[data-dashboard-instance]").forEach(function (instance) {
              instance.textContent = window.location.host;
              instance.title = window.location.origin;
            });
            clearTransientQueryParams();
            setupFlowValueInspector();
            setupFlowJournalInteractions();
            setupGlobalShortcutsAndCopy();
            applyJournalState(document);
            selectJournalStepFromHash();

            document.addEventListener("submit", function (event) {
              var form = event.target.closest && event.target.closest("[data-dashboard-single-submit]");
              if (!form) { return; }
              if (form.dataset.dashboardSubmitting === "1") {
                event.preventDefault();
                return;
              }
              form.dataset.dashboardSubmitting = "1";
              var buttons = form.querySelectorAll("button[type=submit], input[type=submit]");
              for (var i = 0; i < buttons.length; i += 1) { buttons[i].disabled = true; }
            });

            var root = document.body;
            if (!root || !root.dataset || !root.dataset.dashboardLiveUrl) { return; }

            var url = root.dataset.dashboardLiveUrl;
            var intervalMs = parseInt(root.dataset.dashboardLiveIntervalMs || "2000", 10);
            if (!Number.isFinite(intervalMs) || intervalMs < 500) { intervalMs = 2000; }

            var inFlight = false;
            var failureCount = 0;
            var lastSuccessAt = null;
            var timer = null;
            var status = document.createElement("div");
            status.className = "dashboard-live-status";
            status.setAttribute("data-dashboard-live-status", "connecting");
            status.setAttribute("role", "status");
            status.setAttribute("aria-live", "polite");
            status.innerHTML = '<span class="dashboard-live-dot" aria-hidden="true"></span>' +
              '<span data-dashboard-live-message>Connecting</span>' +
              '<button type="button" data-dashboard-live-retry hidden>Retry</button>';

            function ensureLiveStatusMounted() {
              if (status.isConnected) { return; }
              var statusHost = document.querySelector(".subpage-header") || document.querySelector(".top-bar") || root;
              statusHost.appendChild(status);
            }

            ensureLiveStatusMounted();

            var statusMessage = status.querySelector("[data-dashboard-live-message]");
            var retryButton = status.querySelector("[data-dashboard-live-retry]");

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
              retryButton.hidden = !canRetry;
              retryButton.disabled = inFlight;
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
              var next = window.location.pathname + window.location.search;
              window.location.assign("/dashboard/login?next=" + encodeURIComponent(next));
            }

            function tick() {
              if (document.hidden) { schedule(intervalMs); return; }
              if (inFlight) { return; }
              if (dashboardInteractionPaused()) {
                setLiveStatus("paused", "Updates paused while editing", false);
                schedule(intervalMs);
                return;
              }
              inFlight = true;
              retryButton.disabled = true;

              fetch(url, {
                cache: "no-store",
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (response.status === 401) {
                    setLiveStatus("expired", "Session expired", false);
                    redirectToLogin();
                    throw { dashboardSessionExpired: true };
                  }
                  if (!response.ok) { throw new Error("dashboard live request failed"); }
                  return response.json();
                })
                .then(function (payload) {
                  var componentsPatched = patchComponents(payload.components);
                  ensureLiveStatusMounted();
                  if (componentsPatched) {
                    lastSuccessAt = payload.generated_at_ms || Date.now();
                    root.dataset.dashboardLiveLastUpdateMs = String(lastSuccessAt);
                    root.dataset.dashboardLiveError = "";
                    failureCount = 0;
                    setLiveStatus("live", formatFreshness(), false);
                  } else {
                    setLiveStatus("paused", "Updates paused while editing", false);
                  }
                  schedule(intervalMs);
                })
                .catch(function (error) {
                  if (error && error.dashboardSessionExpired) { return; }
                  failureCount += 1;
                  root.dataset.dashboardLiveError = "1";
                  setLiveStatus("stale", "Stale · " + formatFreshness(), true);
                  schedule(retryDelayMs());
                })
                .finally(function () {
                  inFlight = false;
                  retryButton.disabled = false;
                });
            }

            retryButton.addEventListener("click", function () {
              failureCount = 0;
              setLiveStatus("connecting", "Retrying", false);
              schedule(0);
            });

            document.addEventListener("visibilitychange", function () {
              if (!document.hidden) { schedule(0); }
            });

            tick();
          });
        }());
      </script>
    """
  end

  def render_subpage_header(title) do
    """
    <div class="subpage-header">
      <a class="dashboard-brand" href="/dashboard">FerricStore</a>
      <h1 class="subpage-title">#{escape(title)}</h1>
      <span class="dashboard-instance mono" data-dashboard-instance aria-label="Connected instance"></span>
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

    """
    <details class="dashboard-disclosure"#{open_attr}>
      <summary><span>#{escape(title)}</span><span class="badge badge-idle">#{escape(to_string(badge))}</span></summary>
      <div class="dashboard-disclosure-body">#{content}</div>
    </details>
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
    <nav class="sidebar" aria-label="Dashboard sections">
      #{links}
      #{session}
    </nav>
    """
  end

  defp render_sidebar_session do
    if Acl.protected_mode?() do
      """
      <div class="sidebar-session">
        <span>Protected session</span>
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
       [
         {"keyspace", "/dashboard/keyspace", "Keyspace", :primary},
         {"prefixes", "/dashboard/prefixes", "Prefixes", :sub},
         {"reads", "/dashboard/reads", "Read Path", :sub},
         {"storage", "/dashboard/storage", "Storage", :sub},
         {"commands", "/dashboard/commands", "Command Catalog", :sub}
       ]},
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
    <div id="keyboard-shortcuts-modal" class="keyboard-modal" hidden aria-hidden="true" role="dialog" aria-modal="true" aria-label="Keyboard Shortcuts">
      <div class="keyboard-card">
        <div class="keyboard-header">
          <div class="keyboard-title">Keyboard Shortcuts</div>
          <button type="button" class="dashboard-modal-close" data-keyboard-modal-close aria-label="Close shortcuts dialog">Close</button>
        </div>
        <div class="keyboard-row">
          <span>Search Flow / Partition</span>
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
    </div>
    """
  end

  defp sidebar_item_active?("flow", "flow_detail"), do: true
  defp sidebar_item_active?(key, active), do: key == active
end
