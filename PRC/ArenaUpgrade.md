# ArenaUpgrade.md — Complete Arena Redesign Specification

**From Benchmark Runner to Conversion Engine** | February 15, 2026

---

## Executive Summary

The Arena is Sharc's most visible asset and its worst liability. It runs 16 benchmarks across three engines in-browser with live timing — technically impressive. But it opens with "Browser Database Showdown," shows sequential slides of numbers, and ends with a 16/0 scoreboard that says "Sharc Dominates." A skeptical engineer sees a rigged demo. A decision-maker sees nothing they understand. A mobile visitor sees a broken layout.

This document specifies every change required to transform the Arena from a benchmark runner into a conversion engine. It covers the seven-page narrative structure, every new component, every file that must change, the data-driven refactor that eliminates 365 lines of hardcoded slide data, the query pipeline benchmarks that are currently absent, and the mobile layout that is currently non-functional.

Every specification includes the exact file path, the current state, the target state, and the implementation approach. No ambiguity. No hand-waving.

---

## Table of Contents

1. [What Is Wrong Today](#1-what-is-wrong-today)
2. [Target Architecture: Seven Pages](#2-target-architecture-seven-pages)
3. [Data Layer Refactor: Kill SlideData.cs](#3-data-layer-refactor-kill-slidedatacs)
4. [New Components](#4-new-components)
5. [Existing Component Upgrades](#5-existing-component-upgrades)
6. [Query Pipeline Benchmarks](#6-query-pipeline-benchmarks)
7. [Trust & Encryption Demo](#7-trust--encryption-demo)
8. [Mobile & Responsive Overhaul](#8-mobile--responsive-overhaul)
9. [Shareable Results & Analytics](#9-shareable-results--analytics)
10. [CSS Architecture](#10-css-architecture)
11. [File-by-File Change Manifest](#11-file-by-file-change-manifest)
12. [Implementation Phases](#12-implementation-phases)
13. [Testing & Verification](#13-testing--verification)

---

## 1. What Is Wrong Today

### 1.1 The Arena Opens with a Scoreboard, Not a Story

The current `Arena.razor` renders `ArenaHeader` → 16 sequential `SlideCard` components → `Scoreboard` → CTA. There is no narrative arc. A visitor arrives and immediately sees "Browser Database Showdown" and a grid of technical benchmarks. They must decide to click "Run All Benchmarks" with zero understanding of why they should care.

**Evidence:** The header badge says "🦈 SHARC BENCHMARK ARENA" and the title says "Browser Database Showdown." This frames Sharc as a performance curiosity, not a solution to a problem.

### 1.2 The 16/16 Scorecard Is Misleading

The Arena benchmarks test only core engine operations (CreateReader API). The new query pipeline (Query API) loses 9 of 14 benchmarks to SQLite. The Scoreboard component unconditionally declares the winner "Dominates" without acknowledging the scope of what was tested. The WHERE filter slide already has a methodology note saying "SQLite wins," but the scoreboard ignores this because it only checks the lowest raw value across all engines, and the WHERE filter's Sharc result (1.229ms) is lower than IndexedDB (N/A, not supported), making Sharc appear the winner in the three-engine comparison even when SQLite is faster.

**Evidence:** `Scoreboard.razor` calculates wins by finding the lowest `Value` per slide across all engines where `!r.NotSupported`. The WHERE filter has Sharc at 1.229ms and SQLite at 0.554ms — SQLite wins. But the scoreboard counts it correctly because it compares all supported engines. However, the overall "16/16" framing in documentation and the BENCHMARKS.md Arena table showing "Score: Sharc 16 / SQLite 0 / IndexedDB 0" contradicts the README's query pipeline table showing Sharc losing 9/14. The two data sets are never presented together.

### 1.3 Allocation Data Is Hidden

Every `EngineBaseResult` has an `Allocation` field. The `EngineResult.razor` component renders it as a tiny `engine-alloc` line: `font-size: 0.55rem; color: rgba(255, 255, 255, 0.2)` — nearly invisible. Allocation is where Sharc's core engine shines (0 B on primitives, 648 B on sustained scans) and where the query pipeline struggles (414 KB for SELECT *, 1.5 MB for UNION). Hiding this data makes both the wins and losses invisible.

### 1.4 No Context Problem Page

The Arena never explains *why* fast database reads matter for AI agents. The README's "Why Sharc Exists" section mentions "62-133x token reduction," but the Arena contains zero mention of tokens, context windows, or AI agents. The visitor who understands AI context problems is never reached. The visitor who doesn't understand isn't educated.

### 1.5 No Interactive "Try It"

The Arena is purely passive. A visitor cannot drag a `.db` file, cannot type a query, cannot explore a schema. Every benchmark runs identically for every visitor. There is no discovery moment — no point where the visitor thinks "what if I try *my* data?"

### 1.6 Mobile Is Broken

`Arena.razor` has `_isMobile = false; _showSidebar = true;` hardcoded in `UpdateLayout()`. There is no JS interop to detect viewport width. The single `@media (max-width: 640px)` rule in `app.css` hides the sidebar but does nothing else. The 3-column engine result grid (`grid-template-columns: repeat(3, 1fr)`) does not collapse on mobile. Touch targets are too small. The loading screen works, but everything after it doesn't.

### 1.7 No Conversion Paths

The CTA section at the bottom offers "Star on GitHub" and "Connect on LinkedIn." There is no path for a developer ("Install: dotnet add package Sharc"), no path for a decision-maker ("Download the ROI brief"), no path for enterprise ("Contact for consulting").

### 1.8 SlideData.cs Is a Maintenance Nightmare

365 lines of hardcoded C# object initializers define every benchmark. Adding a new benchmark requires modifying this file, understanding the nested object structure, and recompiling. The file includes phantom engines ("surrealdb", "arangodb") with fabricated reference data that never runs live. These phantom entries pad the visual layout but represent no actual measurement.

---

## 2. Target Architecture: Seven Pages

The Arena becomes a seven-page narrative application with the benchmark runner as Page 3 (not Page 1).

### Page Flow

```
[Page 1: Hook] → [Page 2: Context Problem] → [Page 3: Benchmark Arena] → 
[Page 4: Query Pipeline] → [Page 5: Trust Demo] → [Page 6: Honest Comparison] → 
[Page 7: Call to Action]
```

### Page 1: The Hook

**Purpose:** Communicate value proposition in 10 seconds.

**Layout:**
- Headline: "Read SQLite at Memory Speed"
- Subheadline: "Pure C#. Zero dependencies. Sub-250KB. Built for AI agents."
- Three value cards (horizontal on desktop, stacked on mobile):
  - Speed: "61× faster seeks" — with a single animated timing bar
  - Size: "250 KB total" — with a comparative dot chart (Sharc dot vs SQLite dot vs IndexedDB dot)
  - Trust: "Cryptographic audit trail" — with an animated hash chain icon
- Two CTAs: "Run the Benchmarks" (scrolls to Page 3), "Install Now" (code block)
- Subtle background: the existing `arena-bg-gradient` and `arena-bg-grid`

**File:** New `Components/HookPage.razor`

**Data dependency:** None. Static content.

### Page 2: The Context Problem

**Purpose:** Explain why this matters for AI agents. Make the visitor feel the pain.

**Layout:**
- Headline: "AI Agents Are Drowning in Context"
- Two-column comparison:
  - Left ("The Old Way"): Animated diagram showing 500K tokens flowing into an LLM, with 99% grayed out as irrelevant, result quality "64% accuracy"
  - Right ("The Sharc Way"): Animated diagram showing 2K tokens flowing, all highlighted as relevant, result quality "83% accuracy"
- Key stat callout: "Smart 200K retrieval beats brute-force 1M" — cited from Google research
- Three scenario cards: Healthcare (62× token reduction), Engineering (133× reduction), Manufacturing (£225K savings)
- Transition: "Don't take our word for it — measure it yourself" → scroll to Page 3

**File:** New `Components/ContextProblemPage.razor`

**Data dependency:** Static content. Scenario cards from case study data.

### Page 3: The Benchmark Arena (Redesigned)

**Purpose:** Live, honest, interactive benchmark comparison.

**Changes from current state:**

The current 16 sequential slides become 4 themed sections:

**Section A — Point Operations:** Engine Load, Schema Read, Point Lookup, Batch Lookups
**Section B — Scan Operations:** Sequential Scan, Type Decode, NULL Detection, WHERE Filter, GC Pressure
**Section C — Graph Operations:** Node Scan, Edge Scan, Node Seek, 2-Hop BFS
**Section D — Trust & Security:** Encrypted Read, Trust Ledger, Memory Footprint, Primitives

Each section has a header card explaining *why these operations matter*:
- Point Ops header: "When an AI agent needs one fact, it needs it in microseconds."
- Scan Ops header: "Context retrieval requires scanning thousands of rows without GC pauses."
- Graph Ops header: "Relationship traversal connects facts into knowledge."
- Trust header: "Provenance turns AI recommendations from advisory to actionable."

Each benchmark card gains:
- "Why It Matters" tooltip (new field on SlideDefinition)
- Allocation bar alongside the timing bar (currently hidden text)
- Speedup multiplier badge on the winner (e.g., "61×")
- Honest loss annotation: when Sharc loses, show the loss explicitly

**Scoreboard changes:**
- Rename from "Final Standings" to "Core Engine Results"
- Add subtitle: "These benchmarks test the CreateReader API (direct B-tree access). See Query Pipeline for SQL roundtrip results."
- Show wins/total with allocation wins alongside timing wins

**Files modified:** `Arena.razor`, `SlideCard.razor`, `Scoreboard.razor`, `EngineResult.razor`
**Files created:** `Components/SectionHeader.razor`, `Components/AllocationBar.razor`, `Components/SpeedupBadge.razor`

### Page 4: Query Pipeline Comparison

**Purpose:** Show the new SQL query capabilities with honest benchmarks.

**Layout:**
- Headline: "SQL Query Pipeline: Parse → Compile → Execute → Read"
- Subtitle: "Full roundtrip benchmarks — Sharc Query API vs SQLite"
- Two-column result table showing all 14 query categories from the README
- Each row: query text, Sharc timing, SQLite timing, speedup/loss ratio, memory
- Color coding: green rows where Sharc wins, amber where parity, red where SQLite wins
- Summary card: "Sharc wins on raw scans and UNION ALL (in-process advantage). SQLite wins on complex filtered/sorted queries (40-year C optimizer). Use CreateReader for point lookups, Query for analytics."
- Interactive: visitor can click any row to expand the full methodology

**File:** New `Components/QueryPipelinePage.razor`
**Service:** New `Services/QueryPipelineEngine.cs` — runs live query benchmarks using `SharcDatabase.Query()` vs SQLite command execution
**Data:** New JSON file `wwwroot/data/query-benchmarks.json` with all 14 categories

### Page 5: Interactive Trust Demo

**Purpose:** Demystify the trust layer with a live, hands-on demonstration.

**Layout:**
- Headline: "See Trust in Action"
- Interactive four-step flow:
  1. **Register Agent:** Visitor types an agent name, clicks "Register." Shows ECDSA key generation.
  2. **Append Entry:** Visitor types a payload, clicks "Sign & Append." Shows the ledger entry being created with hash chain.
  3. **Verify Chain:** Click "Verify Integrity." Shows green checkmarks cascading down the chain.
  4. **Tamper & Detect:** Click "Tamper with Entry #2." Shows the modification, then re-verify shows the break point in red.
- The entire flow runs in the browser using Sharc.Crypto and Sharc Trust layer (already available in WASM)
- Below the interactive demo: code snippets showing how to do each step programmatically

**File:** New `Components/TrustDemoPage.razor`
**Service:** New `Services/TrustDemoEngine.cs` — wraps LedgerManager, AgentRegistry, AuditManager for interactive use

### Page 6: The Honest Comparison

**Purpose:** Build trust by explicitly acknowledging where SQLite is better.

**Layout:**
- Two columns:
  - Left: "Where Sharc Excels" (green theme) — B-tree seeks, graph traversal, edge deployment, encryption, trust/audit, token reduction, zero dependencies
  - Right: "Where SQLite Excels" (blue theme) — JOINs, complex aggregates, UPDATE/DELETE, FTS5, R*Tree, write-heavy workloads, 40-year ecosystem
- Bottom statement: "Sharc is a complement to SQLite, not a replacement."
- Links to: WHEN_NOT_TO_USE.md, MIGRATION.md, COMPARISONS.md

**File:** New `Components/HonestComparisonPage.razor`

**Data dependency:** Static content.

### Page 7: Call to Action

**Purpose:** Convert interest into action.

**Layout:**
Three paths, each with a distinct visual card:

**Developer Path:**
```
Install: dotnet add package Sharc
→ Getting Started Guide (5 min)
→ Cookbook (18 recipes)
→ GitHub repo
```

**Decision Maker Path:**
```
Download: ROI Brief (PDF)
→ Case Studies
→ Comparison Matrices
→ Architecture Overview
```

**Enterprise / Government Path:**
```
Contact: consulting@sharc.dev
→ Sharc for Governments brief
→ Professional Services overview
→ Data sovereignty guarantee
```

**File:** New `Components/CtaPage.razor`

---

## 3. Data Layer Refactor: Kill SlideData.cs

### 3.1 Current Problem

`SlideData.cs` (365 lines) hardcodes every engine, category, preset, density tier, and benchmark slide as C# object initializers. Adding a benchmark requires recompiling the WASM application. The file also includes phantom engines (SurrealDB, ArangoDB) with fabricated reference numbers.

### 3.2 Target: JSON-Driven Configuration

Create `wwwroot/data/` directory with declarative JSON files:

```
wwwroot/data/
├── engines.json          # Engine definitions
├── categories.json       # Benchmark categories
├── presets.json           # Workload presets
├── core-benchmarks.json  # Section A slides
├── scan-benchmarks.json  # Section B slides
├── graph-benchmarks.json # Section C slides
├── trust-benchmarks.json # Section D slides
├── query-benchmarks.json # Page 4 query pipeline data
└── comparison.json       # Page 6 comparison data
```

### 3.3 JSON Schema for Benchmarks

```json
{
  "$schema": "benchmark-slide",
  "slides": [
    {
      "id": "point-lookup",
      "title": "Memory Seek (Direct Access)",
      "subtitle": "Pinpoint retrieval of a single fact by Primary Key",
      "icon": "🎯",
      "unit": "ns",
      "categoryId": "core",
      "sectionId": "point-ops",
      "whyItMatters": "When an AI agent needs to verify a single fact, it cannot afford 24μs of overhead. Sub-microsecond seeks mean the agent's retrieval step becomes negligible in the total response latency.",
      "methodology": "Retrieving a specific memory by its global ID. Sharc descends the B-tree directly in memory. BenchmarkDotNet DefaultJob, 15 iterations, 8 warmups.",
      "scaleMode": "fixed",
      "defaultDensity": "md",
      "densityTiers": "fixed",
      "baseResults": {
        "sharc": {
          "value": 3444,
          "allocation": "8,320 B",
          "note": "7.1× — direct pointer seek"
        },
        "sqlite": {
          "value": 24347,
          "allocation": "728 B",
          "note": "VDBE VM overhead"
        },
        "indexeddb": {
          "value": 85000,
          "allocation": "320 B",
          "note": "Async IDB request"
        }
      }
    }
  ]
}
```

### 3.4 JSON Schema for Query Pipeline

```json
{
  "$schema": "query-pipeline",
  "description": "Full SQL roundtrip: parse → compile → execute → read all rows",
  "config": {
    "rowsPerTable": 2500,
    "overlapStart": 2001,
    "tables": ["users_a", "users_b"]
  },
  "benchmarks": [
    {
      "id": "simple-select-all",
      "category": "simple",
      "query": "SELECT * FROM users_a",
      "sharc": { "timeUs": 713, "allocKb": 414, "note": "Full materialization" },
      "sqlite": { "timeUs": 747, "allocBytes": 688 },
      "ratio": 1.05,
      "winner": "sharc"
    },
    {
      "id": "medium-order-limit",
      "category": "medium",
      "query": "SELECT id, name, score FROM users_a WHERE age >= 25 AND active = 1 ORDER BY score DESC LIMIT 100",
      "sharc": { "timeUs": 3298, "allocKb": 54 },
      "sqlite": { "timeUs": 326, "allocKb": 5.5 },
      "ratio": 0.1,
      "winner": "sqlite",
      "explanation": "SQLite's VDBE optimizer handles WHERE + ORDER BY + LIMIT as a single pass with index-assisted sort. Sharc materializes all matching rows, sorts in managed memory, then truncates."
    }
  ]
}
```

### 3.5 Loader Service

**File:** New `Services/BenchmarkDataLoader.cs`

```csharp
public sealed class BenchmarkDataLoader
{
    private readonly HttpClient _http;
    
    public async Task<IReadOnlyList<SlideDefinition>> LoadSectionAsync(string sectionFile)
    {
        var json = await _http.GetStringAsync($"data/{sectionFile}");
        return JsonSerializer.Deserialize<BenchmarkFile>(json)!.Slides
            .Select(MapToSlideDefinition)
            .ToList();
    }
    
    public async Task<IReadOnlyList<QueryBenchmark>> LoadQueryBenchmarksAsync()
    {
        var json = await _http.GetStringAsync("data/query-benchmarks.json");
        return JsonSerializer.Deserialize<QueryPipelineFile>(json)!.Benchmarks;
    }
}
```

### 3.6 Migration Path

1. Create JSON files from current SlideData.cs values (mechanical extraction)
2. Add `BenchmarkDataLoader` service
3. Update `Arena.razor` to load from JSON instead of `SlideData.CreateSlides()`
4. Remove phantom engines (SurrealDB, ArangoDB) — these never ran live and fabricated data erodes trust
5. Delete `SlideData.cs`
6. Keep `DependencyGraph.cs` (it captures real execution dependencies)

### 3.7 Phantom Engine Decision

**Remove SurrealDB and ArangoDB from all benchmark slides.** They appear as "reference data only" with fabricated numbers from external documentation. The Arena's value proposition is *live, measured benchmarks*. Phantom engines with estimated numbers undermine this. If a user sees "SurrealDB: 487μs" next to "Sharc: 4.0μs" and then discovers the SurrealDB number was never measured in-browser, the entire Arena's credibility collapses.

Keep only three engines: **Sharc, SQLite, IndexedDB** — all measured live, with consistent timing methodology, documented in the footer.

---

## 4. New Components

### 4.1 `Components/HookPage.razor`

**Responsibility:** Render the landing page with headline, value pillars, and primary CTAs.

**Parameters:** None. Static content.

**Internal structure:**
```
<section class="hook-page">
  <div class="hook-headline">Read SQLite at Memory Speed</div>
  <div class="hook-subheadline">Pure C#. Zero dependencies. Sub-250KB. Built for AI agents.</div>
  <div class="hook-pillars">
    <ValuePillar Icon="⚡" Stat="61×" Label="faster seeks" />
    <ValuePillar Icon="📦" Stat="250 KB" Label="total footprint" />
    <ValuePillar Icon="🔐" Stat="ECDSA" Label="agent attestation" />
  </div>
  <div class="hook-ctas">
    <button @onclick="ScrollToBenchmarks">Run the Benchmarks</button>
    <div class="hook-install">dotnet add package Sharc</div>
  </div>
</section>
```

**Estimated size:** ~60 lines.

### 4.2 `Components/ValuePillar.razor`

**Responsibility:** Render a single value proposition card with icon, stat, and label.

**Parameters:** `string Icon`, `string Stat`, `string Label`, optional `string Detail`.

**Estimated size:** ~20 lines.

### 4.3 `Components/ContextProblemPage.razor`

**Responsibility:** Render the AI context problem with old-way/new-way comparison.

**Internal structure:**
```
<section class="context-page">
  <h2>AI Agents Are Drowning in Context</h2>
  <div class="context-comparison">
    <ContextColumn 
      Label="The Old Way" Tokens="500K" Relevance="1%" Accuracy="64%" 
      Theme="red" />
    <ContextColumn 
      Label="The Sharc Way" Tokens="2K" Relevance="95%" Accuracy="83%" 
      Theme="green" />
  </div>
  <div class="context-scenarios">
    <ScenarioCard Title="Healthcare" Metric="62×" Detail="token reduction" />
    <ScenarioCard Title="Engineering" Metric="133×" Detail="token reduction" />
    <ScenarioCard Title="Manufacturing" Metric="£225K" Detail="annual savings" />
  </div>
</section>
```

**Estimated size:** ~80 lines.

### 4.4 `Components/ContextColumn.razor`

**Responsibility:** Animated token flow visualization.

**Parameters:** `string Label`, `string Tokens`, `string Relevance`, `string Accuracy`, `string Theme`.

**Animation:** CSS keyframes that show tokens flowing into an LLM box. Red theme grays out 99% of tokens. Green theme highlights all tokens.

**Estimated size:** ~40 lines + CSS.

### 4.5 `Components/ScenarioCard.razor`

**Responsibility:** Render a case study summary card.

**Parameters:** `string Title`, `string Metric`, `string Detail`, optional `string LinkUrl`.

**Estimated size:** ~15 lines.

### 4.6 `Components/SectionHeader.razor`

**Responsibility:** Render a themed header for each benchmark section within Page 3.

**Parameters:** `string SectionId`, `string Title`, `string Description`, `string Color`.

**Internal structure:**
```
<div class="section-header" style="border-color: @Color">
  <h3 class="section-title">@Title</h3>
  <p class="section-description">@Description</p>
</div>
```

**Estimated size:** ~15 lines.

### 4.7 `Components/AllocationBar.razor`

**Responsibility:** Render a visual bar representing memory allocation alongside the timing bar.

**Parameters:** `string Allocation`, `long AllocBytes` (parsed from string), `long MaxAllocBytes`, `string Color`.

**Design:** Horizontal bar below the timing bar, using a muted version of the engine color. Labels show allocation in human-readable format.

**Estimated size:** ~30 lines.

### 4.8 `Components/SpeedupBadge.razor`

**Responsibility:** Render a "61×" badge on the winner or a "0.1×" loss badge on losers.

**Parameters:** `double Ratio`, `bool IsWinner`.

**Logic:** If ratio >= 1.1, show green "Nx faster." If ratio <= 0.9, show amber "Nx slower." If between, show "~parity."

**Estimated size:** ~20 lines.

### 4.9 `Components/WhyItMattersTooltip.razor`

**Responsibility:** Render an expandable tooltip explaining why a benchmark matters.

**Parameters:** `string Text`, `bool Expanded`.

**Interaction:** Click to expand/collapse. On mobile, tap. On desktop, hover shows preview, click expands.

**Estimated size:** ~25 lines.

### 4.10 `Components/QueryPipelinePage.razor`

**Responsibility:** Render the full query pipeline comparison table.

**Internal structure:**
```
<section class="query-page">
  <h2>SQL Query Pipeline</h2>
  <p class="query-subtitle">Full roundtrip: parse → compile → execute → read all rows</p>
  <div class="query-table">
    @foreach (var benchmark in _queryBenchmarks)
    {
      <QueryRow 
        Query="@benchmark.Query"
        SharcTime="@benchmark.Sharc.TimeUs"
        SqliteTime="@benchmark.Sqlite.TimeUs"
        Ratio="@benchmark.Ratio"
        Winner="@benchmark.Winner"
        SharcAlloc="@benchmark.Sharc.AllocKb"
        SqliteAlloc="@benchmark.Sqlite.AllocKb" />
    }
  </div>
  <div class="query-summary">
    <SummaryCard 
      SharcWins="@_sharcWins" SqliteWins="@_sqliteWins" 
      TotalBenchmarks="@_queryBenchmarks.Count" />
  </div>
</section>
```

**Data:** Loaded from `data/query-benchmarks.json` via `BenchmarkDataLoader`.

**Estimated size:** ~120 lines.

### 4.11 `Components/QueryRow.razor`

**Responsibility:** Render a single query benchmark row with timing, allocation, and ratio.

**Parameters:** `string Query`, `double SharcTime`, `double SqliteTime`, `double Ratio`, `string Winner`, `double SharcAlloc`, `double SqliteAlloc`.

**Interaction:** Click to expand methodology/explanation.

**Estimated size:** ~50 lines.

### 4.12 `Components/TrustDemoPage.razor`

**Responsibility:** Interactive four-step trust demonstration.

**State management:** Local component state tracking: registered agents (list), ledger entries (list), verification status (bool), tamper target (int?).

**Service dependency:** `TrustDemoEngine` (new).

**Internal flow:**
1. User types agent name → calls `TrustDemoEngine.RegisterAgent(name)` → shows public key
2. User types payload → calls `TrustDemoEngine.AppendEntry(agentId, payload)` → shows hash chain entry
3. User clicks Verify → calls `TrustDemoEngine.VerifyChain()` → cascading green animation
4. User clicks Tamper → calls `TrustDemoEngine.TamperEntry(index)` → modifies payload → re-verify shows red break

**Estimated size:** ~200 lines (most complex new component).

### 4.13 `Components/HonestComparisonPage.razor`

**Responsibility:** Two-column comparison of Sharc strengths vs SQLite strengths.

**Parameters:** None. Data from `comparison.json`.

**Estimated size:** ~60 lines.

### 4.14 `Components/CtaPage.razor`

**Responsibility:** Three-path conversion with developer, decision-maker, and enterprise cards.

**Parameters:** None. Static content.

**Estimated size:** ~80 lines.

### 4.15 `Components/PageNav.razor`

**Responsibility:** Fixed navigation bar showing the seven pages with scroll-to behavior.

**Parameters:** `int ActivePage`.

**Design:** Horizontal pill bar at top (desktop) or bottom (mobile). Shows: Hook · Context · Benchmarks · Queries · Trust · Compare · Get Started. Active page highlighted.

**Estimated size:** ~40 lines.

---

## 5. Existing Component Upgrades

### 5.1 `Arena.razor` — Complete Restructure

**Current:** Single page rendering Header → Slides → Scoreboard → CTA.

**Target:** Seven-page container with scroll-based navigation.

**Structure:**
```razor
@page "/"

<PageNav ActivePage="@_activePage" OnNavigate="@ScrollToPage" />

<div class="arena-container">
  <HookPage />
  <ContextProblemPage />
  
  <section id="benchmarks" class="arena-benchmarks">
    <ArenaHeader ... />   @* Stripped-down: no "Showdown" framing *@
    @foreach (var section in _sections)
    {
      <SectionHeader ... />
      @foreach (var slide in section.Slides)
      {
        <SlideCard ... />
      }
    }
    <Scoreboard ... />
  </section>
  
  <QueryPipelinePage />
  <TrustDemoPage />
  <HonestComparisonPage />
  <CtaPage />
</div>
```

**Key changes:**
- Add `IntersectionObserver` JS interop to track which page is visible (sets `_activePage`)
- Load slide data from JSON via `BenchmarkDataLoader` in `OnInitializedAsync`
- Group slides by `sectionId` for themed sections
- Add JS interop for `ScrollToPage(pageId)` smooth scrolling
- Remove `_isMobile = false` hardcode — detect via JS interop

**Estimated delta:** ~+80 lines, ~-20 lines removed.

### 5.2 `ArenaHeader.razor` — Reframe

**Current title:** "Browser Database Showdown"
**Target title:** "Core Engine Benchmarks"

**Current badge:** "🦈 SHARC BENCHMARK ARENA"
**Target badge:** "🦈 SHARC ARENA — LIVE BENCHMARKS"

**Current subtitle:** "@Slides.Count benchmarks · @Engines.Count engines · Sequential · ~@EstimateSeconds()s"
**Target subtitle:** "Core engine performance measured live in your browser. @Slides.Count benchmarks across @Engines.Count engines."

**Remove:** The "Workload Profile" preset section clutters the header. Move presets to a collapsible settings panel.

**Add:** Scope indicator: "Testing: CreateReader API (direct B-tree access)"

### 5.3 `SlideCard.razor` — Enrich

**Add parameters:**
- `string? WhyItMatters` — renders `WhyItMattersTooltip`
- `bool ShowAllocationBars` (default true) — renders `AllocationBar` per engine
- `bool ShowSpeedupBadge` (default true) — renders `SpeedupBadge` on winner
- `string? SectionId` — for section grouping

**Remove:** Phantom engine handling (SurrealDB/ArangoDB entries will be gone from data).

**Allocation display upgrade:**
Currently: `<div class="engine-alloc">Alloc: @(Result.Allocation ?? "—")</div>` (near-invisible)
Target: Full `AllocationBar` component with visual width proportional to allocation size, rendered below the timing bar.

**Estimated delta:** ~+30 lines.

### 5.4 `EngineResult.razor` — Upgrade

**Add:**
- `AllocationBar` rendering (new visual bar)
- `SpeedupBadge` on winner (e.g., "61×")
- Loss annotation on losers: when Sharc loses, show "SQLite: 0.55ms ✓" with checkmark

**Modify formatting:**
- `engine-alloc` CSS: increase from 0.55rem/0.2 opacity to 0.65rem/0.5 opacity
- Add allocation bar track below timing bar track

**Estimated delta:** ~+25 lines.

### 5.5 `Scoreboard.razor` — Honest Reframe

**Change title:** "Final Standings" → "Core Engine Results"

**Add subtitle:** "These benchmarks test the CreateReader API (direct B-tree access). Query pipeline results shown below."

**Add allocation summary row:** For each engine, show total allocation across all benchmarks.

**Add link:** "See Query Pipeline Results →" (scrolls to Page 4).

**Remove:** "🏆 @winnerEngine.Name Dominates" — replace with "@winnerEngine.Name: @wins wins / @total benchmarks (core engine)"

**Estimated delta:** ~+15 lines.

### 5.6 `SideNav.razor` — Section Grouping

**Add:** Section headers in the nav that group slides by category.

**Current:** Flat list of 16 slide items.
**Target:** Grouped list: "Point Ops" (4 items), "Scan Ops" (5 items), "Graph Ops" (4 items), "Trust & Meta" (3 items).

**Estimated delta:** ~+20 lines.

---

## 6. Query Pipeline Benchmarks

### 6.1 New Service: `Services/QueryPipelineEngine.cs`

**Responsibility:** Run live query pipeline benchmarks in-browser.

**Benchmarks to run:**
1. `SELECT * FROM users_a` (2,500 rows)
2. `SELECT id, name, age FROM users_a WHERE age > 30`
3. `SELECT id, name, score FROM users_a WHERE age >= 25 AND active = 1 ORDER BY score DESC LIMIT 100`
4. `SELECT dept, COUNT(*) AS cnt, AVG(score) AS avg_score FROM users_a GROUP BY dept`
5. `SELECT id, name, dept FROM users_a UNION ALL SELECT id, name, dept FROM users_b`
6. `SELECT id, name, dept FROM users_a UNION SELECT id, name, dept FROM users_b`
7. `SELECT id, name, dept FROM users_a INTERSECT SELECT id, name, dept FROM users_b`
8. `SELECT id, name, dept FROM users_a EXCEPT SELECT id, name, dept FROM users_b`
9. `SELECT id, name, score FROM users_a UNION ALL SELECT id, name, score FROM users_b ORDER BY score DESC LIMIT 50`
10. `SELECT id, name FROM users_a UNION ALL SELECT id, name FROM users_b UNION ALL SELECT id, name FROM users_a` (3-way)
11. `WITH active AS (SELECT id, name, score FROM users_a WHERE active = 1) SELECT id, name, score FROM active WHERE score > 50`
12. `WITH eng AS (SELECT id, name, score FROM users_a WHERE dept = 'eng') SELECT id, name, score FROM eng UNION ALL SELECT id, name, score FROM users_b`
13. Parameterized: `SELECT id, name FROM users_a WHERE age > $min_age AND score < $max_score`

**Implementation pattern:** Same as `SharcEngine.cs` — Stopwatch timing, GC allocation tracking, warmup iterations.

**Data generation:** Reuse `DataGenerator` with two tables (users_a, users_b) of 2,500 rows each with 500 overlap (same as `QueryRoundtripBenchmarks.cs`).

**SQLite comparison:** Create and execute equivalent `SqliteCommand` for each query.

**Estimated size:** ~400 lines.

### 6.2 Data File: `wwwroot/data/query-benchmarks.json`

Contains all 13 benchmark definitions with reference data from the BenchmarkDotNet results in BENCHMARKS.md. Used for fallback when live benchmarks are disabled.

### 6.3 Memory Comparison Table

The query pipeline page includes a dedicated memory comparison table (from README):

| Query Type | Sharc | SQLite | Notes |
|:---|---:|---:|:---|
| `SELECT *` (2.5K rows) | 414 KB | 688 B | Sharc materializes all column values |
| `UNION/INTERSECT/EXCEPT` | 1.5-1.8 MB | 744 B | SQLite does set ops in native C |
| `GROUP BY + COUNT + AVG` | 169 KB | 920 B | Streaming hash aggregator, O(G) memory |
| `Cote` | 281 KB | 31 KB | Cote rows materialized then re-scanned |

This table is rendered with explicit visual emphasis — not hidden in small text.

---

## 7. Trust & Encryption Demo

### 7.1 New Service: `Services/TrustDemoEngine.cs`

**Responsibility:** Provide a simplified API over the trust layer for the interactive demo.

**Methods:**
```csharp
public record DemoAgent(string Id, string Name, string PublicKeyHex);
public record DemoLedgerEntry(long Id, string AgentId, string Payload, string HashHex, string PrevHashHex);

public DemoAgent RegisterAgent(string name);
public DemoLedgerEntry AppendEntry(string agentId, string payload);
public (bool Valid, int? BreakAtIndex) VerifyChain();
public void TamperEntry(int index, string newPayload);
public IReadOnlyList<DemoLedgerEntry> GetEntries();
```

**Implementation:** Creates an in-memory SharcDatabase with `_sharc_ledger` and `_sharc_agents` tables. Uses existing `AgentRegistry`, `LedgerManager`, and `SharcSigner`.

**Estimated size:** ~150 lines.

### 7.2 Encryption Demo Enhancement

The current "Encrypted Read" slide shows timing only. Enhance:
- Show the Argon2id key derivation step separately
- Show the AES-256-GCM page decryption step
- Display the encrypted bytes vs decrypted bytes (hex view, first 64 bytes)
- Show that the data is identical after decrypt

This enriches the existing `SharcEngine.RunEncryption()` method with step-by-step results.

---

## 8. Mobile & Responsive Overhaul

### 8.1 Viewport Detection

**Current:** `_isMobile = false; _showSidebar = true;` hardcoded.

**Fix:** Add JS interop for viewport detection.

**File:** New `wwwroot/js/viewport.js`
```javascript
window.sharcArena = {
    getViewportWidth: () => window.innerWidth,
    onResize: (dotNetRef) => {
        window.addEventListener('resize', () => {
            dotNetRef.invokeMethodAsync('OnViewportChanged', window.innerWidth);
        });
    }
};
```

**Blazor integration:** In `Arena.razor`:
```csharp
[Inject] private IJSRuntime JS { get; set; } = default!;
private DotNetObjectReference<Arena>? _selfRef;

protected override async Task OnAfterRenderAsync(bool firstRender)
{
    if (firstRender)
    {
        var width = await JS.InvokeAsync<int>("sharcArena.getViewportWidth");
        _isMobile = width < 768;
        _showSidebar = width >= 1024;
        _selfRef = DotNetObjectReference.Create(this);
        await JS.InvokeVoidAsync("sharcArena.onResize", _selfRef);
        StateHasChanged();
    }
}

[JSInvokable]
public void OnViewportChanged(int width)
{
    _isMobile = width < 768;
    _showSidebar = width >= 1024;
    StateHasChanged();
}
```

### 8.2 Responsive Breakpoints

**Current:** Single `@media (max-width: 640px)` rule that hides sidebar.

**Target breakpoints:**
```css
/* Mobile: < 768px */
@media (max-width: 767px) { ... }

/* Tablet: 768px–1023px */
@media (min-width: 768px) and (max-width: 1023px) { ... }

/* Desktop: ≥ 1024px */
@media (min-width: 1024px) { ... }
```

### 8.3 Mobile Layout Rules

**Engine result grid:**
- Desktop: `grid-template-columns: repeat(3, 1fr)` (current, works)
- Tablet: `grid-template-columns: repeat(3, 1fr)` (same, with smaller cards)
- Mobile: `grid-template-columns: 1fr` (single column, cards stacked)

**Timing bars:**
- Desktop: Horizontal (current, works)
- Mobile: Horizontal but full-width

**Touch targets:**
- All buttons: minimum 44×44px touch target
- Density control buttons: increase from current ~24px to 44px height on mobile
- Preset buttons: increase padding

**Page navigation:**
- Desktop: Top horizontal pill bar
- Mobile: Bottom fixed tab bar (iOS/Android convention)

**Hook page:**
- Desktop: Three pillars horizontal
- Mobile: Three pillars vertical stacked

**Query pipeline table:**
- Desktop: Full table layout
- Mobile: Card layout (one card per benchmark, stacked)

**Honest comparison:**
- Desktop: Two columns side-by-side
- Mobile: Two columns stacked vertically

### 8.4 Font Size Adjustments

Several current sizes are too small for mobile:

| Element | Current | Mobile Target |
|:---|:---|:---|
| `.engine-alloc` | 0.55rem | 0.7rem |
| `.engine-note` | 0.52rem | 0.65rem |
| `.arena-legend-text` | 0.52rem | 0.65rem |
| `.slide-number` | 0.48rem | 0.6rem |
| `.scoreboard-total` | 0.52rem | 0.65rem |
| `.preset-desc` | 0.55rem | 0.65rem |

---

## 9. Shareable Results & Analytics

### 9.1 Shareable Result Links

After benchmarks complete, generate a URL with encoded results:

```
https://revred.github.io/Sharc.Open/#results=eyJlbmdpbmVMb2FkIjp7InNoYXJjIjo0LjAsInNxbGl0ZSI6MTQyMDAwfSwic2NoZW1hUmVhZCI6ey4uLn19
```

**Implementation:**
1. After all benchmarks complete, serialize results to compact JSON
2. Base64-encode
3. Set URL hash via JS interop
4. "Share Results" button copies URL to clipboard
5. On page load, check for `#results=` hash, decode, and display static results

**File:** New `Services/ResultShareService.cs` (~50 lines)
**JS:** Add `sharcArena.setHash(hash)` and `sharcArena.getHash()` to viewport.js

### 9.2 Result Export

Add "Download as JSON" and "Download as CSV" buttons to the scoreboard:

```csharp
private async Task ExportJson()
{
    var json = JsonSerializer.Serialize(_results, new JsonSerializerOptions { WriteIndented = true });
    await JS.InvokeVoidAsync("sharcArena.downloadFile", "sharc-benchmark-results.json", json);
}
```

### 9.3 Basic Analytics

Track (client-side, no external service):
- Page views per section (via IntersectionObserver)
- "Run All" click count
- Average time on page
- Benchmark completion rate
- Device type (mobile/tablet/desktop)

Store in `localStorage` (for the user's own dashboard) and optionally POST to a GitHub-hosted analytics endpoint.

---

## 10. CSS Architecture

### 10.1 CSS Custom Properties

The current CSS uses raw color values throughout. Consolidate:

```css
:root {
    /* Existing (keep) */
    --sharc: #10B981;
    --text-muted: rgba(255, 255, 255, 0.4);
    --text-dim: rgba(255, 255, 255, 0.15);
    
    /* New */
    --sqlite: #3B82F6;
    --indexeddb: #F59E0B;
    --bg-primary: #0A0A0F;
    --bg-card: rgba(255, 255, 255, 0.02);
    --bg-card-hover: rgba(255, 255, 255, 0.04);
    --border-subtle: rgba(255, 255, 255, 0.06);
    --border-active: rgba(16, 185, 129, 0.3);
    --win-bg: rgba(16, 185, 129, 0.08);
    --loss-bg: rgba(239, 68, 68, 0.05);
    --parity-bg: rgba(251, 191, 36, 0.05);
    
    /* Typography */
    --font-sans: 'DM Sans', sans-serif;
    --font-mono: 'JetBrains Mono', monospace;
    --text-xs: 0.55rem;
    --text-sm: 0.65rem;
    --text-base: 0.75rem;
    --text-lg: 0.9rem;
    --text-xl: 1.1rem;
    --text-2xl: 1.4rem;
    --text-3xl: 1.8rem;
    
    /* Spacing */
    --space-xs: 4px;
    --space-sm: 8px;
    --space-md: 16px;
    --space-lg: 24px;
    --space-xl: 40px;
}
```

### 10.2 New CSS Sections

Add to `app.css` (or split into multiple files):

```
/* ── Page Navigation ─── */
/* ── Hook Page ───────── */
/* ── Context Problem ─── */
/* ── Section Headers ─── */
/* ── Allocation Bars ─── */
/* ── Speedup Badges ──── */
/* ── Query Pipeline ──── */
/* ── Trust Demo ──────── */
/* ── Honest Comparison ─ */
/* ── CTA Page ────────── */
/* ── Mobile Overrides ── */
```

### 10.3 Animation Budget

Current animations: `pulse` (loading), `loading` (loading bar), `blink` (live dot), `bar-fill` (result bars).

New animations (keep total under 6):
- `fadeUp` — page sections appearing on scroll
- `chainVerify` — cascading green checkmarks in trust demo
- `tokenFlow` — tokens flowing into LLM in context problem page

All animations respect `prefers-reduced-motion`.

---

## 11. File-by-File Change Manifest

### New Files (21 files)

| File | Size Est. | Purpose |
|:---|:---|:---|
| `Components/HookPage.razor` | 60 lines | Landing page |
| `Components/ValuePillar.razor` | 20 lines | Value prop card |
| `Components/ContextProblemPage.razor` | 80 lines | AI context problem |
| `Components/ContextColumn.razor` | 40 lines | Token flow visualization |
| `Components/ScenarioCard.razor` | 15 lines | Case study summary |
| `Components/SectionHeader.razor` | 15 lines | Benchmark section divider |
| `Components/AllocationBar.razor` | 30 lines | Memory allocation visual |
| `Components/SpeedupBadge.razor` | 20 lines | Multiplier badge |
| `Components/WhyItMattersTooltip.razor` | 25 lines | Expandable explanation |
| `Components/QueryPipelinePage.razor` | 120 lines | SQL benchmark table |
| `Components/QueryRow.razor` | 50 lines | Single query result |
| `Components/TrustDemoPage.razor` | 200 lines | Interactive trust demo |
| `Components/HonestComparisonPage.razor` | 60 lines | Where each excels |
| `Components/CtaPage.razor` | 80 lines | Conversion paths |
| `Components/PageNav.razor` | 40 lines | Page navigation |
| `Services/BenchmarkDataLoader.cs` | 80 lines | JSON data loader |
| `Services/QueryPipelineEngine.cs` | 400 lines | Live query benchmarks |
| `Services/TrustDemoEngine.cs` | 150 lines | Trust demo backend |
| `Services/ResultShareService.cs` | 50 lines | Shareable URLs |
| `wwwroot/js/viewport.js` | 30 lines | Viewport detection |
| `wwwroot/data/*.json` | 7 files | Declarative benchmark data |

**Total new: ~1,545 lines of Blazor/C# + ~7 JSON files**

### Modified Files (11 files)

| File | Current Lines | Change | Key Modifications |
|:---|:---|:---|:---|
| `Pages/Arena.razor` | 180 | Major restructure | Seven-page layout, JSON loading, JS interop |
| `Components/ArenaHeader.razor` | 85 | Moderate | Remove "Showdown," add scope indicator |
| `Components/SlideCard.razor` | 95 | Moderate | Add WhyItMatters, AllocationBar, SpeedupBadge |
| `Components/EngineResult.razor` | 105 | Moderate | Add AllocationBar, SpeedupBadge, loss annotation |
| `Components/Scoreboard.razor` | 65 | Moderate | Reframe title, add allocation row, add query link |
| `Components/SideNav.razor` | 40 | Minor | Section grouping |
| `wwwroot/css/app.css` | 880 | Major | New sections, responsive breakpoints, variables |
| `wwwroot/index.html` | 40 | Minor | Add viewport.js script tag, update OG meta |
| `Program.cs` | 24 | Minor | Register new services |
| `Layout/MainLayout.razor` | 4 | Minor | Add PageNav |
| `Sharc.Arena.Wasm.csproj` | ~50 | Minor | Add System.Text.Json if not present |

### Deleted Files (1 file)

| File | Lines Removed | Reason |
|:---|:---|:---|
| `Data/SlideData.cs` | 365 | Replaced by JSON data files |

---

## 12. Implementation Phases

### Phase 1: Data-Driven Foundation (2 days)

**Goal:** Replace SlideData.cs with JSON, remove phantom engines, add viewport detection.

**Tasks:**
1. Create `wwwroot/data/` with JSON files extracted from current SlideData.cs
2. Create `BenchmarkDataLoader` service
3. Remove SurrealDB and ArangoDB from all data
4. Update `Arena.razor` to load from JSON
5. Delete `SlideData.cs`
6. Add `viewport.js` and wire viewport detection
7. Fix `_isMobile` detection
8. Add responsive breakpoints to CSS

**Verification:** Arena renders identically to current state (minus phantom engines) on desktop. Mobile viewport detected correctly.

### Phase 2: Story Layer (2 days)

**Goal:** Add Pages 1, 2, 6, and 7. Arena gains narrative flow.

**Tasks:**
1. Create `HookPage`, `ValuePillar` components
2. Create `ContextProblemPage`, `ContextColumn`, `ScenarioCard` components
3. Create `HonestComparisonPage` component (data from comparison.json)
4. Create `CtaPage` component
5. Create `PageNav` component
6. Restructure `Arena.razor` as seven-page container
7. Add IntersectionObserver JS interop for page tracking
8. Add scroll-to-page JS interop
9. Add CSS for all new page sections

**Verification:** Seven pages render. Navigation works. Scroll tracking works. Mobile layout stacks correctly.

### Phase 3: Benchmark Enrichment (2 days)

**Goal:** Upgrade existing benchmark presentation with sections, allocation bars, speedup badges, and tooltips.

**Tasks:**
1. Create `SectionHeader`, `AllocationBar`, `SpeedupBadge`, `WhyItMattersTooltip` components
2. Add `whyItMatters` field to all benchmark JSON entries
3. Update `SlideCard` to render section-grouped slides with enriched data
4. Update `EngineResult` with allocation bars and speedup badges
5. Update `Scoreboard` with honest reframe and allocation summary
6. Update `SideNav` with section grouping
7. Update `ArenaHeader` messaging

**Verification:** Benchmark section runs identically with enriched presentation. Allocation visible. Speedup badges accurate. Scoreboard honest.

### Phase 4: Query Pipeline Page (2 days)

**Goal:** Add Page 4 with live query pipeline benchmarks.

**Tasks:**
1. Create `QueryPipelineEngine` service
2. Create `QueryPipelinePage` and `QueryRow` components
3. Create `query-benchmarks.json` with reference data
4. Wire live benchmark execution for all 13 query categories
5. Add memory comparison table
6. Add expand/collapse for methodology per row
7. Add CSS for query table and mobile card layout

**Verification:** Query benchmarks run live. Results match BenchmarkDotNet numbers (±20% for WASM). Losses displayed honestly. Memory table visible.

### Phase 5: Trust Demo + Polish (2 days)

**Goal:** Add Page 5, shareable results, and final mobile polish.

**Tasks:**
1. Create `TrustDemoEngine` service
2. Create `TrustDemoPage` with four-step interactive flow
3. Add shareable result links (`ResultShareService`)
4. Add result export (JSON/CSV download)
5. Mobile layout testing and fixes across all seven pages
6. Touch target verification (44px minimum)
7. Font size adjustments for mobile
8. Animation cleanup (respect prefers-reduced-motion)
9. Final CSS polish

**Verification:** Trust demo works end-to-end. Shareable links encode/decode correctly. Mobile layout functional on iOS Safari and Chrome Android. All touch targets 44px+.

---

## 13. Testing & Verification

### 13.1 Functional Tests

For each phase, verify:
- [ ] Arena loads without errors in browser console
- [ ] All benchmark slides execute and display results
- [ ] Scoreboard calculates wins correctly
- [ ] Navigation between all seven pages works
- [ ] JSON data loads correctly (no 404s)
- [ ] CancellationToken stops execution mid-run
- [ ] "Live Hardware" toggle switches between live and reference engines

### 13.2 Visual Tests

- [ ] Desktop (1920×1080): All seven pages render, sidebar visible, 3-column engine grid
- [ ] Tablet (768×1024): All pages render, sidebar hidden, 3-column grid, readable text
- [ ] Mobile (375×812): All pages stack, single-column grid, bottom nav, 44px touch targets
- [ ] Hook page: Headline visible above fold on all viewports
- [ ] Query pipeline: Table readable on desktop, cards on mobile
- [ ] Trust demo: All four steps interactive on all viewports

### 13.3 Performance Tests

- [ ] Initial load < 3 seconds (WASM download included)
- [ ] First contentful paint < 1 second (loading screen)
- [ ] Page navigation < 100ms
- [ ] Benchmark execution does not block UI (Task.Yield after each slide)
- [ ] JSON data files total < 50 KB
- [ ] No memory leaks after repeated benchmark runs (verify via browser DevTools)

### 13.4 Content Tests

- [ ] No mention of "16/16" or "Dominates" in Arena
- [ ] WHERE filter slide shows SQLite winning
- [ ] Query pipeline shows Sharc losing 9/14
- [ ] Honest comparison page acknowledges all SQLite strengths
- [ ] All methodology notes reference BenchmarkDotNet configuration
- [ ] No phantom engine data (SurrealDB/ArangoDB) anywhere

### 13.5 Accessibility Tests

- [ ] All interactive elements keyboard-navigable
- [ ] Color contrast ratio ≥ 4.5:1 for text (check faint allocation text)
- [ ] `prefers-reduced-motion` respected for all animations
- [ ] Screen reader: page structure uses semantic HTML (section, h2, h3, table)
- [ ] Focus indicators visible on all interactive elements

---

## Appendix A: Benchmark Number Reconciliation

The Arena currently shows numbers that differ from the README and BENCHMARKS.md. Before implementing, reconcile to a single source of truth:

| Benchmark | Arena SlideData | README Core Table | README Query Table | BENCHMARKS.md |
|:---|:---|:---|:---|:---|
| Point Lookup | 3,444 ns | 392 ns | — | 848 ns |
| Batch Lookups | 5,237 ns | 1,940 ns | — | 3,326 ns |
| Sequential Scan | 2.59 ms | 1.54 ms | — | 1.54 ms |
| Schema Read | 2.97 μs | — | — | 4.69 μs |

**Resolution:** The Arena runs in-browser (WASM) while BENCHMARKS.md numbers come from native BenchmarkDotNet. The WASM overhead explains the consistent ~2× gap. Both are valid measurements for their context. The Arena should note: "Browser WASM — native performance is 2-3× faster. See BENCHMARKS.md for BenchmarkDotNet results."

## Appendix B: Dependency Inventory

New dependencies required: **None.** All functionality uses existing Blazor WASM, System.Text.Json (already referenced), and Sharc packages already in the project. No new NuGet packages. No new CDN scripts. No external analytics services.

## Appendix C: Phantom Engine Reference Data (To Remove)

The following entries exist in SlideData.cs and must not carry forward to JSON:

- `surrealdb`: 16 entries across 16 slides, all fabricated reference data, never ran live
- `arangodb`: 16 entries across 16 slides, all fabricated reference data, never ran live

Total: 32 phantom data points. Remove all. The Arena tells the truth or it tells nothing.

---

*This document specifies the complete transformation. Every file is named. Every component is parameterized. Every phase has verification criteria. Build it.*
