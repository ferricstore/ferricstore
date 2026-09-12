---
version: 1
slug: "index-html"
primary_target: "index.html"
related_targets: ["agent-loop/index.html","ai-agent-workflow/index.html","architecture-comparison/index.html","beginner-queue/index.html","benchmark-explainer/index.html","cache-stampede/index.html","canary-rollback/index.html","hash-field-ttl/index.html","hot-cold-storage/index.html","idempotency-determinism/index.html","parallel-fanout/index.html","probabilistic-cache/index.html","rate-limiting-stream/index.html","split-lab/index.html","stream-vs-pubsub/index.html","subscription-dunning/index.html","ticket-reservation/index.html","travel-saga/index.html","workflow-explainer/index.html","zombie-fencing/index.html"]
---

# FerricStore public demo catalog

- Scope: the catalog and all 20 static interactive demo routes.
- Visitor mode: Persuade through direct technical demonstration.
- Audience: people evaluating FerricStore, including non-developers, engineering leaders, and software architects.
- Homepage language: welcome non-developers too. Explain workflows through familiar tasks, use plain-language demo labels, and leave implementation vocabulary to the technical guides and demos. Preserve accuracy about saved progress and steps that may run again.
- Job: understand a mechanism, cause or inspect failure and recovery, verify boundaries, then continue to source or documentation.
- Homepage first visit: understand that FerricStore is a workflow and queue server, distinguish application execution from saved workflow state, then explore the demos or get started locally.
- Homepage primary action: How it works, an in-page introduction before the catalog. Experienced visitors can jump directly to Demos from navigation.
- Demo primary action: run the current experiment within the first viewport.
- Demo clarity: one short explanation, an obvious selected scenario, a matching ready/running/result state, and a plain-language takeaway. Do not repeat the same lesson in the introduction, caption, and result. Keep code and deeper technical explanations available on demand.
- Proof: existing simulations, state histories, measured benchmark evidence, documented technical boundaries, and source/documentation links.
- Constraints: preserve behavior and factual claims; no invented customers, benchmarks, guarantees, comparisons, or enterprise positioning; static GitHub Pages; accessible at 390×844 and 1440×900.
- Chosen direction: Dispatch Strip Board—workflow state is a physical progress strip moving through named rails, with claims, failures, handoffs, and recovery marked in place.
- Memorable moment: a visitor triggers failure and watches the same durable strip move from a crashed owner to a replacement without losing its committed marks.
- Unresolved: none for implementation; mechanism-specific presentations may vary while retaining the shared dispatch grammar.
