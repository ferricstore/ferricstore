# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

The primary audience is engineering leaders and software architects evaluating whether FerricStore is a credible fit for durable workflows, queues, coordination, and related state-management problems. They need to understand the mechanism behind each demonstration and gather enough evidence to make an adoption or deeper-evaluation decision.

Developers learning durable workflow concepts are an important secondary audience, but the catalog should prioritize technical credibility, architectural clarity, and evaluation value over introductory entertainment.

## Product Purpose

FerricStore is an open-source durable workflow and queue server. Applications run ordinary handler code in their own services while FerricStore persists workflow state, makes named states claimable under leases and fencing, and records transitions, retries, signals, and terminal outcomes.

The static demo catalog makes those mechanisms visible through self-contained browser simulations. Success means a visitor can quickly understand what a mechanism does, interact with its failure and recovery behavior, and decide whether FerricStore warrants technical evaluation or adoption.

## Positioning

FerricStore combines durable workflow and queue coordination with a sharded, Raft-backed key-value and data-structure engine. It persists explicit named workflow state without replaying handler code or requiring application code to execute inside the FerricStore server.

Each demo must prove a concrete mechanism rather than relying on generic resilience language. The interface may explain documented boundaries and measured evidence, but it must not invent customers, performance claims, comparisons, guarantees, or capabilities.

## Operating Context

Visitors arrive from GitHub, technical conversations, documentation, or shared demo links. They evaluate the product on desktop and mobile, often with limited time and existing knowledge of workflow engines, queues, databases, caching, streams, or distributed systems.

Their expected path is: recognize the system problem, see the FerricStore mechanism operating, cause or inspect a failure, understand recovery and boundaries, then continue to source code or documentation for deeper evaluation.

## Capabilities and Constraints

- The site is a static GitHub Pages catalog using HTML, CSS, and client-side JavaScript.
- Every existing demo interaction and scenario must remain functional.
- Most demos are simulations with illustrative timings or values; the benchmark explainer contains explicitly sourced historical evidence with workload and hardware boundaries.
- Workflow handlers remain at-least-once. External effects require guarded execution and stable idempotency keys.
- FerricStore does not replay handler code and does not require application code to run inside the server.
- Existing factual content, benchmark numbers, product boundaries, and links must remain accurate.
- The experience must avoid unsupported comparisons, enterprise-only positioning, invented adoption evidence, or promises not present in repository documentation.
- Pages must work at a minimum at 390×844 mobile and 1440×900 desktop sizes, with no clipped workflow content or unintended page-level horizontal scrolling.

## Brand Commitments

The FerricStore name and product truth remain fixed. No existing visual style, palette, typography, icon treatment, or page composition is binding. The visual system may be replaced when doing so improves comprehension, technical trust, and product adoption.

The voice should be technically precise, confident, candid about boundaries, and focused on helping evaluators understand the product rather than using adversarial or exaggerated sales language.

## Evidence on Hand

- The FerricStore OSS README and capability documentation in `/Users/yoavgea/repos/ferricstore`.
- The working static catalog and 20 interactive demos in this repository.
- Documented workflow, queue, data-structure, reliability, architecture, and benchmark examples already present in those pages.
- Existing source, documentation, SDK, and benchmark-reference links.
- No customer logos, testimonials, production adoption claims, or commercial proof were supplied and none may be fabricated.

## Product Principles

- Demonstrate the mechanism before explaining the surrounding theory.
- Let visitors cause, observe, and recover from a concrete failure within seconds.
- Preserve technical truth and surface important boundaries at the moment they matter.
- Structure every page to help an evaluator progress from understanding to evidence to deeper adoption research.
- Keep the shared system coherent while making each mechanism visually and behaviorally distinct.

## Accessibility & Inclusion

The public catalog must support keyboard navigation, visible focus, semantic landmarks, reduced motion, screen-reader-compatible state updates, sufficient contrast, and touch targets suitable for mobile use. Technical explanations must not assume familiarity with FerricStore-specific terminology before defining it.
