---
name: FerricStore Dispatch Strip Board
description: A crisp, mechanical evaluation system that makes durable work, failure, recovery, and evidence inspectable.
colors:
  paper: "#f3f6f4"
  paper-bright: "#fcfefb"
  strip: "#e7ece8"
  ink: "#0d222b"
  ink-soft: "#2e454e"
  muted: "#566b72"
  line: "#aebbb5"
  line-soft: "#d8dfda"
  signal-lime: "#c9e84c"
  signal-lime-hover: "#b8da34"
  signal-ink: "#526b00"
  signal-soft: "#eff7cf"
  success-green: "#19765b"
  warning-yellow: "#8a6500"
  failure-red: "#b33a48"
  evidence-blue: "#205f91"
  surface-white: "#ffffff"
  terminal-deep: "#101b20"
  terminal-text: "#eff4ef"
typography:
  display:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "clamp(56px, 7vw, 92px)"
    fontWeight: 600
    lineHeight: 0.92
    letterSpacing: "-0.035em"
  headline:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "clamp(42px, 4.2vw, 68px)"
    fontWeight: 600
    lineHeight: 0.96
    letterSpacing: "-0.035em"
  section-title:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "clamp(38px, 4vw, 54px)"
    fontWeight: 600
    letterSpacing: "-0.02em"
  panel-title:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "21px"
    fontWeight: 600
    lineHeight: 1.5
    letterSpacing: "-0.02em"
  body:
    fontFamily: '"Atkinson Hyperlegible", ui-sans-serif, system-ui, sans-serif'
    fontSize: "17px"
    fontWeight: 400
    lineHeight: 1.62
    fontFeature: '"tnum" 1'
  supporting:
    fontFamily: '"Atkinson Hyperlegible", ui-sans-serif, system-ui, sans-serif'
    fontSize: "15px"
    fontWeight: 400
    lineHeight: 1.5
  ui-label:
    fontFamily: '"Atkinson Hyperlegible", ui-sans-serif, system-ui, sans-serif'
    fontSize: "15px"
    fontWeight: 700
    lineHeight: 1.5
  metadata:
    fontFamily: '"Atkinson Hyperlegible", ui-sans-serif, system-ui, sans-serif'
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.5
  micro-label:
    fontFamily: '"Atkinson Hyperlegible", ui-sans-serif, system-ui, sans-serif'
    fontSize: "12px"
    fontWeight: 700
    lineHeight: 1.4
    letterSpacing: "0.06em"
  state-label:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "18px"
    fontWeight: 600
    lineHeight: 1.25
  card-title:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "23px"
    fontWeight: 600
    lineHeight: 1.25
  metric:
    fontFamily: '"Barlow Condensed", "Arial Narrow", sans-serif'
    fontSize: "26px"
    fontWeight: 600
    lineHeight: 1.2
  mono:
    fontFamily: '"SFMono-Regular", Consolas, "Liberation Mono", monospace'
    fontWeight: 400
    lineHeight: 1.5
    fontFeature: '"tnum" 1'
rounded:
  square: "0px"
  stamp: "2px"
  compact: "3px"
  control: "4px"
  surface: "6px"
spacing:
  tight: "5px"
  control-gap: "8px"
  cell-y: "10px"
  cell-x: "12px"
  control-x: "14px"
  gutter: "18px"
  section: "22px"
  panel: "34px"
components:
  button-primary:
    backgroundColor: "{colors.signal-lime}"
    textColor: "{colors.ink}"
    typography: "{typography.ui-label}"
    rounded: "{rounded.control}"
    padding: "0 18px"
    height: "50px"
  button-primary-hover:
    backgroundColor: "{colors.signal-lime-hover}"
    textColor: "{colors.ink}"
  button-secondary:
    backgroundColor: "transparent"
    textColor: "{colors.ink}"
    typography: "{typography.ui-label}"
    rounded: "{rounded.control}"
    padding: "0 14px"
    height: "50px"
  setup-choice:
    backgroundColor: "{colors.paper-bright}"
    textColor: "{colors.ink}"
    typography: "{typography.ui-label}"
    rounded: "{rounded.compact}"
    padding: "9px 12px"
    height: "46px"
  setup-choice-selected:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.surface-white}"
  search-field:
    backgroundColor: "{colors.paper-bright}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.compact}"
    padding: "0 12px"
    height: "44px"
  evaluation-nav:
    backgroundColor: "{colors.paper-bright}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    height: "64px"
  mechanism-map:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper-bright}"
    rounded: "{rounded.square}"
  mechanism-cell:
    backgroundColor: "{colors.strip}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "10px 12px"
    height: "72px"
  experiment-surface:
    backgroundColor: "{colors.surface-white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.surface}"
  disclosure-row:
    backgroundColor: "{colors.paper-bright}"
    textColor: "{colors.ink}"
    typography: "{typography.panel-title}"
    rounded: "{rounded.square}"
    padding: "0 18px"
    height: "58px"
  evidence-strip:
    backgroundColor: "{colors.paper-bright}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "16px 18px"
  catalog-row:
    backgroundColor: "transparent"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "18px 8px"
---

# Design System: FerricStore Dispatch Strip Board

## Overview

**Creative North Star: "Dispatch Strip Board"**

Dispatch Strip Board treats durable workflow state like a paper progress strip moving through named rails. The world is crisp, inspectable, and operational: graphite frames hold cool mineral working surfaces, compact stamps identify state, and FerricStore lime marks the action or transition that deserves attention.

The system is built for evaluation rather than spectacle. An architect should recognize a failure, run the mechanism, verify recovery and its boundary, then follow the evidence toward adoption. The first viewport therefore pairs a concise thesis and direct action with a live mechanism; supporting prose, code, comparisons, and evidence follow only after the system has shown its work.

It deliberately rejects the generic gradient demo shell. Personality comes from paper, rails, separators, stamped states, condensed headings, and mechanical control geometry rather than decorative glow or interchangeable cards.

**Key Characteristics:**

- Cool mineral paper over graphite rails.
- Signal lime reserved for the primary action and current state.
- Condensed structural headings paired with hyperlegible proof copy.
- Compact bordered panels, stamped labels, and square mechanical controls.
- Live state movement before supporting prose, code, and evidence.

## Colors

The palette combines cool mineral neutrals with graphite structural ink, one acid-lime product signal, and restrained semantic colors for outcome, warning, failure, and evidence.

### Primary

- **Signal Lime** (`{colors.signal-lime}`): Primary run actions, the current mechanism surface, selection, and active emphasis. It always carries graphite text.
- **Signal Lime Hover** (`{colors.signal-lime-hover}`): Hover state for lime-filled actions.
- **Signal Ink** (`{colors.signal-ink}`): Focus rings, current-state markers, emphasized outcomes, and compact labels that need strong contrast on paper.
- **Signal Soft** (`{colors.signal-soft}`): A restrained current-state wash inside mechanism strips.

### Secondary

- **Recovery Green** (`{colors.success-green}`): Committed and recovered states; it marks work that is durably done.
- **Boundary Yellow** (`{colors.warning-yellow}`): Existing warning states where a mechanism is waiting or approaching a boundary.
- **Failure Red** (`{colors.failure-red}`): Crash and rejected-state semantics, never generic decoration.
- **Evidence Blue** (`{colors.evidence-blue}`): Links from the mechanism toward supporting evidence.

### Neutral

- **Evaluation Paper** (`{colors.paper}`): Page canvas and the faint ruled background.
- **Bright Paper** (`{colors.paper-bright}`): First viewports, navigation, controls, and evidence surfaces.
- **Progress Strip** (`{colors.strip}`): Intro rails, mechanism cells, and hover fills.
- **Rail Ink** (`{colors.ink}`): Primary text, borders, dark rails, selected controls, and structural frames.
- **Soft Ink** (`{colors.ink-soft}`): Explanatory copy that remains prominent but does not compete with labels or headings.
- **Muted Notation** (`{colors.muted}`): Secondary labels, notes, captions, and inactive state numbers.
- **Rule Line** (`{colors.line}`): Primary interior dividers and control borders.
- **Soft Rule Line** (`{colors.line-soft}`): Nested dividers and low-emphasis separation.
- **Surface White** (`{colors.surface-white}`): Live experiment interiors and high-contrast control text.
- **Terminal Deep** (`{colors.terminal-deep}`) and **Terminal Text** (`{colors.terminal-text}`): Code and machine-output surfaces.

### Named Rules

**The Signal Is Earned Rule.** Use signal lime for the primary action and current mechanism state; use signal ink for focus and compact emphasis. Do not wash broad surfaces in either.

**The Paper Before Glow Rule.** The world is cool mineral paper and solid rails; generic gradient demo shells are outside the system.

## Typography

- **Display Font:** Barlow Condensed (with Arial Narrow and sans-serif fallbacks)
- **Body Font:** Atkinson Hyperlegible (with system sans-serif fallbacks)
- **Label/Mono Font:** Atkinson Hyperlegible for interface labels; SFMono-Regular, Consolas, or Liberation Mono for code and machine state

**Character:** Barlow Condensed gives the board its tall, efficient dispatch voice. Atkinson Hyperlegible keeps mechanisms, caveats, and evaluation evidence clear at compact sizes, while tabular numerals and monospaced code preserve machine-readable rhythm.

### Hierarchy

- **Display** (`{typography.display}`): The catalog thesis; use only for the strongest page-level decision statement.
- **Headline** (`{typography.headline}`): Demo titles in the first viewport, constrained to a short measure so the mechanism keeps equal authority.
- **Section Title** (`{typography.section-title}`): Catalog and evidence section headings.
- **Panel Title** (`{typography.panel-title}`): Disclosure labels and compact structural headers.
- **Body** (`{typography.body}`): Introductory explanation and proof copy; the demo summary stays near a 54-character measure and longer accuracy notes stay near 75 characters.
- **Supporting** (`{typography.supporting}`): Catalog descriptions, evidence notes, and other explanatory text that must remain easy to scan.
- **UI Label** (`{typography.ui-label}`): Run controls, setup choices, links, and state-bearing actions.
- **Metadata** (`{typography.metadata}`): Run status, telemetry notes, and compact secondary information.
- **Micro Label** (`{typography.micro-label}`): Route codes, mechanism numbers, stamps, and uppercase operational notation.
- **State Label** (`{typography.state-label}`): Named states inside the shared mechanism strip.
- **Card Title** (`{typography.card-title}`): Demo names in catalog rows.
- **Metric** (`{typography.metric}`): Evidence-strip values and compact measured results.
- **Mono** (`{typography.mono}`): Source, identifiers, fencing tokens, and machine output.

### Named Rules

**The Two-Voice Rule.** Use Barlow Condensed to name structures and Atkinson Hyperlegible to explain and verify them; reserve mono for code and machine state.

## Layout

Desktop demos use a rail-like first viewport: a minimum 280px intro column at roughly 31% of the available width and a flexible live-experiment column, both filling the viewport below the 64px evaluation navigation. The intro carries the thesis, setup choices, run action, verification target, and evidence link; the experiment carries the mechanism strip and the existing interactive stage.

Catalog and evidence bands use a maximum 1320px working width with 20px outer gutters. Longer disclosure content narrows to 1160px. Catalog rows form two columns with a 28px channel; evidence metrics form a four-cell strip. Interior rhythm is compact and repeated: 8px control gaps, 10px by 12px strip cells, 18px evidence padding, and 34px experiment padding.

At 1000px, demo intro share increases and four-column evidence becomes two columns. At 780px, navigation becomes two rows and the first viewport becomes a single ordered flow: thesis, setup, run action, experiment, verification outcome, then evidence link. At 520px, controls stack, catalog and evidence become single-column, and long mechanism rails use explicit horizontal scrolling with scroll-snap hints rather than clipping or shrinking state labels past legibility.

**The Mechanism-First Rule.** In demo first viewports, the thesis, primary action, and live mechanism must appear before supporting prose or code.

## Elevation & Depth

The system is flat by default. One-pixel ink and rule lines, alternating paper tones, dark rails, and nested panels establish depth structurally. The catalog hero apparatus uses one ambient shadow (`0 20px 42px rgba(13, 34, 43, 0.13)`), and the primary run action uses a smaller state shadow (`0 10px 20px rgba(82, 107, 0, 0.14)`). Experiment surfaces, stage headers, catalog rows, disclosures, and ordinary controls remain unshadowed.

### Shadow Vocabulary

- **Apparatus Shadow** (`0 20px 42px rgba(13, 34, 43, 0.13)`): The catalog's dark workflow preview only.
- **Action Shadow** (`0 10px 20px rgba(82, 107, 0, 0.14)`): The primary run control at rest.

### Named Rules

**The Flat-by-Default Rule.** Borders and tonal surfaces carry hierarchy; shadows belong only to the catalog hero apparatus and the primary action.

## Shapes

The form language is square and mechanical with only enough corner relief to avoid visual brittleness. Stamps and state cards use 2px corners, compact filters and inputs use 3px, buttons use 4px, and outer live-experiment surfaces use 6px. Major rails, evidence bands, disclosures, and dividers remain square. Mechanism cells add a clipped hanging marker rather than a rounded badge.

**The Mechanical Corner Rule.** Controls and state markers stay square or gently relieved, never pill-soft unless a route-specific mechanism already requires it.

## Components

### Buttons

- **Shape:** Compact mechanical rectangles with 4px corners and a minimum 44px touch target; the primary run action is 50px high.
- **Primary:** Signal-lime fill, graphite text, 1px ink border, 18px horizontal padding, and bold 15px labeling.
- **Hover / Focus / Active:** Hover shifts to the darker lime step and lifts 1px; active returns to the rail. Keyboard focus is a 3px signal-ink outline with a 3px offset. Motion uses 180ms ease-out and collapses under reduced-motion preferences.
- **Secondary:** Transparent paper with ink text and border; hover inverts to ink with white text.

### Setup Choices

- **Style:** Route modes and scenario choices become a one-column rack of 46px-high paper controls with 3px corners, 1px rule borders, and left-aligned labels.
- **State:** The selected choice inverts to rail ink with white text. Native tab semantics and arrow-key navigation remain visible and usable.

### Cards / Containers

- **Corner Style:** Live experiment shells use the largest system radius at 6px; nested headers and panels return to square separators.
- **Background:** White experiment interiors sit inside bright-paper or strip rails.
- **Shadow Strategy:** No shadow; 1px rule or ink borders and tonal layering carry the hierarchy.
- **Internal Padding:** Experiment framing uses a responsive 18–34px inset; strip and evidence cells remain denser.

### Inputs / Fields

- **Style:** Search uses a bright-paper fill, 1px ink border, 3px corners, and a 44px touch target.
- **Focus:** The shared 3px signal-ink focus outline sits outside the field without replacing its border.

### Navigation

- **Style:** The demo evaluation bar is sticky and 64px high on desktop, with brand, route code/title, and adoption links divided by vertical rules. The current route code is stamped in ink on bright paper.
- **Responsive:** At 780px it becomes a two-row grid, moving the route name beneath the brand and links; lower-priority links progressively hide while the path back to all demos remains.

### Mechanism Map

The signature component is a dark framed progress strip with a compact caption rail and one cell per named state. Each cell shows an ordinal and condensed uppercase state name. The current cell uses a soft-lime fill and signal-ink hanging marker; completed cells shift to recovery paper and green markers. JavaScript advances the existing strip from live route signals so the summary and the underlying experiment describe the same run.

**The Same-Strip Rule.** Updates advance the existing strip with current and done stamps rather than replacing it with a disconnected success graphic.

### Evidence Strip

Metrics are arranged as one bordered four-cell band with 18px interior padding and rule dividers. It collapses to two columns at 1000px and one column at 520px. Values use condensed 26px type; labels and boundaries remain muted and explicit.

### Disclosures

Supporting code, comparisons, sources, accuracy notes, and technical boundaries use native `details` rows. The 58px summary is condensed and left-aligned, with a small signal-ink Open/Close state at the edge; opened content sits on bright paper behind a soft rule.

### Catalog Rows

Catalog demos are rows rather than floating cards: a dark code stamp, a concise title and description, and a signal-ink open cue separated by top rules. Hover changes the paper tone but does not lift the row. Filters and search sit in a border-block control rail above the list.

## Do's and Don'ts

### Do:

- **Do** put the runnable mechanism and its current state in the first viewport.
- **Do** keep failure, recovery, and the evaluation boundary attached to the same strip or evidence flow.
- **Do** use paper tones, one-pixel rules, stamps, and condensed labels to create structure before adding elevation.
- **Do** preserve visible focus, 44px minimum targets, reduced motion, and horizontal rail scrolling on narrow screens.
- **Do** use route-specific diagrams inside the shared Dispatch framing when they make the mechanism clearer.

### Don't:

- **Don't** return to a generic gradient hero or an interchangeable floating-card demo shell.
- **Don't** use signal lime as a broad decorative wash or use semantic status colors without state meaning.
- **Don't** hide the live experiment behind introductory prose, code, or a separate detail page.
- **Don't** imply exactly-once handler execution, unsupported guarantees, or adoption evidence through celebratory visual treatment.
- **Don't** round every surface into pills or add shadows where a rail, rule, or paper layer already establishes hierarchy.
