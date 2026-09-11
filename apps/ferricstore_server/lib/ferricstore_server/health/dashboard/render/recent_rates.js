(function () {
  'use strict';
  var keys = ['commands', 'hits', 'misses', 'cold', 'expired', 'evicted'];
  var state = null;

  function valid(sample) {
    return sample && Number.isSafeInteger(sample.at) && sample.at >= 0 && sample.run &&
      /^[1-9][0-9]*$/.test(sample.sampleRate) &&
      keys.every(function (key) { return /^[0-9]+$/.test(sample[key]); });
  }

  function rates(first, last) {
    var result = {};
    var seconds = (last.at - first.at) / 1000;
    keys.forEach(function (key) {
      var delta = BigInt(last[key]) - BigInt(first[key]);
      if (delta < 0n || delta > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('counter discontinuity');
      result[key] = Number(delta) / seconds;
    });
    return result;
  }

  function reduce(previous, sample) {
    if (typeof BigInt !== 'function' || !valid(sample)) return {status: 'unavailable', samples: []};
    var samples = previous && previous.samples ? previous.samples : [];
    var last = samples[samples.length - 1];
    if (!last || sample.run !== last.run || sample.sampleRate !== last.sampleRate ||
        sample.at <= last.at || sample.at - last.at > 15000 ||
        keys.some(function (key) { return BigInt(sample[key]) < BigInt(last[key]); })) {
      return {status: 'warming', samples: [sample]};
    }
    samples = samples.concat([sample]).filter(function (item) { return sample.at - item.at <= 60000; }).slice(-31);
    try {
      return {status: 'ready', samples: samples, elapsedMs: sample.at - samples[0].at, rates: rates(samples[0], sample)};
    } catch (_error) {
      return {status: 'unavailable', samples: []};
    }
  }

  function cell(row, value) {
    var td = document.createElement('td');
    td.textContent = value;
    row.appendChild(td);
  }

  function update() {
    var root = document.querySelector('[data-dashboard-recent-rates]');
    if (!root) { state = null; return; }
    var sample = {at: Number(root.dataset.at), run: root.dataset.run, sampleRate: root.dataset.sampleRate};
    keys.forEach(function (key) { sample[key] = root.dataset[key]; });
    var last = state && state.samples[state.samples.length - 1];
    // Reapplying unchanged HTML is not a second observation.
    if (!last || last.at !== sample.at || last.run !== sample.run) state = reduce(state, sample);
    var ready = state && state.status === 'ready';
    root.querySelector('[data-recent-status]').textContent = ready ?
      'Observed ' + (state.elapsedMs / 1000).toFixed(1) + ' seconds of the last 60 seconds in this tab. Gaps over 15 seconds reset the window.' :
      state && state.status === 'unavailable' ? 'Recent rates unavailable. Waiting for valid counters.' :
      'Waiting for a second sample. Recent window: up to 60 seconds in this tab.';
    root.querySelectorAll('[data-recent-rate]').forEach(function (node) {
      node.textContent = ready ? state.rates[node.dataset.recentRate].toFixed(1) : 'Pending';
      node.classList.toggle('c-muted', !ready);
    });
    var body = root.querySelector('[data-recent-history]');
    if (!ready) return;
    var fragment = document.createDocumentFragment();
    for (var i = state.samples.length - 1; i > 0; i--) {
      var before = state.samples[i - 1], after = state.samples[i];
      var interval = rates(before, after);
      var row = document.createElement('tr');
      cell(row, new Date(after.at).toISOString().replace('T', ' ').replace('Z', ' UTC'));
      cell(row, ((after.at - before.at) / 1000).toFixed(1) + ' s');
      ['commands', 'hits', 'misses', 'cold'].forEach(function (key) { cell(row, interval[key].toFixed(1)); });
      fragment.appendChild(row);
    }
    body.replaceChildren(fragment);
  }

  window.dashboardRecentRates = {reduce: reduce, update: update};
  document.addEventListener('DOMContentLoaded', update);
}());
