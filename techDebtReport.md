# Sharc.Open Technical Debt Report

Date: 2026-03-01
Scope: Full technical debt audit of `Sharc.Open`
Branch: `fix/arena-pages-url`

## Debt Definition Used
A finding is marked as technical debt if it increases one or more of:
- Future change cost (maintainability debt)
- Defect probability/severity (quality debt)
- Runtime reliability confidence (operational debt)
- Build/release friction (delivery debt)
- Security risk exposure (security debt)

## Executive Summary
Sharc.Open has strong baseline engineering hygiene (clean tree, CI, no TODO/FIXME backlog in core trees), but carries concentrated debt in:
1. security-sensitive path containment logic,
2. sync-over-async blocking in production code paths,
3. MCP error-contract quality,
4. solution/CI coverage drift for two projects,
5. monolithic core classes that raise change risk.

## Findings (Severity Ordered)

### 1) High — Path containment check in LocalArcLocator is prefix-based
- Why this is debt:
  - `StartsWith(baseDir)` is a brittle containment check for filesystem boundaries.
  - This is a security-critical boundary (path traversal defense).
- Evidence:
  - `src/Sharc.Arc/Locators/LocalArcLocator.cs:48`
- Note:
  - Existing tests cover traversal patterns like `../../` but not sibling-prefix edge cases.
  - `tests/Sharc.Arc.Tests/Locators/LocalArcLocatorTests.cs:149`
- Recommended action:
  - Replace prefix check with canonical path + separator-safe boundary verification.
  - Add regression tests for sibling-prefix escapes (for example, `C:\baseX` not accepted for `C:\base`).

### 2) High — Sync-over-async blocking in active code paths
- Why this is debt:
  - Blocking on async (`GetAwaiter().GetResult()`) risks deadlock/thread starvation and prevents robust cancellation behavior.
- Evidence:
  - `src/Sharc.Arc/Locators/HttpArcLocator.cs:85`
  - `tools/Sharc.Repo/Cli/UpdateCommand.cs:51`
  - `tools/Sharc.Repo/Cli/UpdateCommand.cs:66`
- Recommended action:
  - Move these paths to async end-to-end APIs/commands.
  - Preserve sync wrappers only at strict boundaries with timeout/cancellation guards.

### 3) High — MCP tools return raw exception messages as plain strings
- Why this is debt:
  - Weak machine-readable contract and potential accidental leakage of sensitive/internal details in tool output.
- Evidence:
  - `tools/Sharc.Repo/Mcp/RepoContextTool.cs:38,71,124,143,168`
  - Same pattern in `RepoAnnotateTool.cs`, `RepoDecisionTool.cs`, `RepoKnowledgeTool.cs`, `RepoQueryTool.cs`.
- Recommended action:
  - Return structured error payloads (`code`, `message`, `details?`, `retryable`) instead of raw `ex.Message` strings.
  - Introduce centralized exception-to-tool-error mapper.

### 4) Medium — Solution/CI coverage drift (2 csproj outside `Sharc.sln`)
- Why this is debt:
  - Projects can rot when excluded from standard CI build/test graph.
- Evidence:
  - Total csproj: 42; in solution: 40.
  - Missing from solution: `tests/Sharc.TestRunner/Sharc.TestRunner.csproj`, `tools/Sharc.Debug/Sharc.Debug.csproj`.
  - CI build uses solution: `.github/workflows/ci.yml:87-93`.
- Recommended action:
  - Either add both projects to solution and CI, or explicitly mark/track them as non-gated.

### 5) Medium — Tools suppress warning-as-error policy
- Why this is debt:
  - Tools are production-facing (CLI/MCP). Allowing warnings in these projects leads to silent debt accumulation.
- Evidence:
  - `tools/Sharc.Repo/Sharc.Repo.csproj:10`
  - `tools/Sharc.Index/Sharc.Index.csproj:10`
  - `tools/Sharc.Context/Sharc.Context.csproj:10`
  - `tools/Sharc.Archive/Sharc.Archive.csproj:10`
- Recommended action:
  - Re-enable warnings-as-errors per tool incrementally (wave migration) with targeted suppressions only where justified.

### 6) Medium — Integration tests reference WASM app directly
- Why this is debt:
  - Pulling UI/WASM project into integration test graph increases build complexity and fragility.
- Evidence:
  - `tests/Sharc.IntegrationTests/Sharc.IntegrationTests.csproj:16`
- Recommended action:
  - Split integration test layers: core engine integration vs UI/WASM integration.
  - Keep default integration lane independent of WASM unless explicitly needed.

### 7) Medium — Monolithic hotspot classes in critical runtime path
- Why this is debt:
  - Large files increase review burden and blast radius of changes.
  - Harder to maintain strict invariants across parsing/IO/query code.
- Evidence:
  - `src/Sharc/SharcDataReader.cs` (~1773 lines)
  - `src/Sharc/SharcDatabase.cs` (~1321 lines)
  - `src/Sharc/SharcWriter.cs` (~1086 lines)
  - `src/Sharc.Query/Sharq/SharqParser.cs` (~902 lines)
- Recommended action:
  - Extract focused components (reader primitives, query planning segments, mutation services) behind internal interfaces.
  - Add class-size/complexity budget checks in PR review checklist.

### 8) Low — Preview dependency for MCP package
- Why this is debt:
  - Preview versions can change behavior/API unexpectedly.
- Evidence:
  - `Directory.Packages.props:28` (`ModelContextProtocol 0.8.0-preview.1`)
- Recommended action:
  - Pin and track upgrade cadence; isolate MCP adapter layer to reduce churn impact.

### 9) Low — Tool references benchmark project
- Why this is debt:
  - Tooling project dependency on benchmark project increases coupling and potentially build overhead.
- Evidence:
  - `tools/Sharc.Debug/Sharc.Debug.csproj:15` references `bench/Sharc.Comparisons/Sharc.Comparisons.csproj`.
- Recommended action:
  - Decouple via shared utility library if needed; avoid tool->bench project dependency.

## Metrics Snapshot
- Repository files (`rg --files`): 1040
- C# source footprint:
  - `src`: 361 files, 50,525 LOC
  - `tests`: 333 files, 61,001 LOC
  - `tools`: 58 files, 6,921 LOC
  - `bench`: 48 files, 8,576 LOC
- Catch blocks:
  - `src`: 62
  - `tools`: 43
  - `tests`: 140
- Sync-over-async patterns detected: 3
- TODO/FIXME/HACK markers in src/tests/tools/bench: 0

## Positive Signals (Keep)
- Clean working tree at audit time.
- CI present with test and performance gate lanes (`.github/workflows/ci.yml`).
- Benchmark workflow separated (`.github/workflows/benchmarks.yml`).
- Repo hygiene includes `TestResults/` and `.sharc/` in `.gitignore`.
- Consistent MIT header usage across open-source codebase.

## Priority Action Plan (Pragmatic)
1. Security hardening wave:
   - Fix local path boundary check.
   - Add sibling-prefix traversal regression tests.
2. Async correctness wave:
   - Remove blocking calls in `HttpArcLocator` and `UpdateCommand`.
3. MCP contract wave:
   - Standardize structured error schema for all repo MCP tools.
4. Build governance wave:
   - Decide and enforce policy for non-solution projects (`Sharc.TestRunner`, `Sharc.Debug`).
5. Maintainability wave:
   - Start decomposition of `SharcDataReader` and `SharcDatabase` with snapshot parity tests.

## Evidence Trail (Key Files)
- `src/Sharc.Arc/Locators/LocalArcLocator.cs`
- `src/Sharc.Arc/Locators/HttpArcLocator.cs`
- `tools/Sharc.Repo/Cli/UpdateCommand.cs`
- `tools/Sharc.Repo/Mcp/RepoContextTool.cs`
- `tools/Sharc.Debug/Sharc.Debug.csproj`
- `tests/Sharc.IntegrationTests/Sharc.IntegrationTests.csproj`
- `.github/workflows/ci.yml`
- `Directory.Build.props`
- `Directory.Packages.props`
