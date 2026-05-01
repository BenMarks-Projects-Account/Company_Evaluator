# Development Work Style — Prompt Construction Context

This document codifies how Ben works with AI agents (Claude as architect/prompt author, Copilot as implementer) for application development. Pass this in alongside any app's `APP_CONTEXT.md` at the start of new conversations to immediately re-establish the working style and avoid reverting to slower defaults.

---

## Two-AI division of labor

Ben uses two AI agents for application work, each with a distinct role:

- **Claude** is the architect, prompt author, and debugger. Claude operates at the design level — reviewing changes, writing prompts that specify work for Copilot, diagnosing issues, and surfacing architectural concerns. Claude does NOT typically write production code directly. Claude writes prompts that direct Copilot to write production code.

- **Copilot** is the implementer. Copilot has direct access to the codebase, can run terminal commands, can read files, can apply edits, and can verify changes. Copilot executes the work specified in Claude's prompts.

When Ben describes a task in conversation with Claude, the expected output is **a prompt file** that Ben will paste into Copilot in the appropriate workspace. The prompt is the deliverable Claude produces. Claude does NOT write the actual implementation code unless explicitly asked.

The prompt files are saved as markdown files (e.g., `eva_feature_compute_history_prompt.md`) and Ben pastes them into Copilot's chat in the relevant workspace. Each prompt is self-contained, scoped to a single coherent task, and assumes Copilot has full codebase access.

---

## The prompt structure Ben prefers

Every prompt should follow this structure unless the task is genuinely trivial. Section names should appear as H2 headers (`##`).

### Scope

A 2-4 paragraph plain-English description of what this prompt accomplishes and why. Frames the work in terms of user-visible outcomes, not implementation details. References prior context (recent prompts, related work) when continuity matters.

If the prompt targets a specific repo or workspace, state it explicitly. (E.g., "This is an EVA-only prompt. BenTrade is unchanged.") This avoids the "Copilot opened the wrong workspace" failure mode.

References to standing documentation come at the end of this section: `Reference '.github/copilot-instructions.md' and 'docs/APP_CONTEXT.md'`.

### Required diagnosis (do this first)

Lists files Copilot must read end-to-end before writing any code. Specifies what to identify in the report — exact column names, current function signatures, current behavior of relevant code paths.

This section often includes specific SQL queries or curl commands to run before changes, with output to be pasted in the report. The "before" state is captured so the "after" state can be compared.

The diagnosis section is the single biggest lever against Copilot fabricating assumptions about code it didn't actually read. Use it generously.

### In scope

Numbered subsections, each describing one coherent change. Each subsection includes:

- The exact file(s) being modified
- A pseudocode block showing approximate intent (NOT a copy-paste implementation)
- Constraints, edge cases, and field names
- Expected behavior after the change

Pseudocode in prompts is illustrative, not prescriptive. Copilot is expected to adapt to the actual code style. The pseudocode communicates intent.

When a fix or feature has multiple parts, separate them as numbered subsections (1., 2., 3., etc.) rather than letting them blur into prose.

### Out of scope

Bullet list of things Copilot should NOT do. This is critical — without it, Copilot drifts toward "while I'm here, I'll also fix..." which causes regressions. Common entries:

- "EVA service code is unchanged" / "BenTrade is unchanged"
- "No new features beyond what already exists in features.py"
- "Touching the launcher" or "modifying the backfill"
- "BenTrade UI changes (those should just work after this)"
- Any specific files or directories Copilot must not touch

### Verification

A numbered list of steps to verify the change worked. Always includes:

1. Run any diagnosis SQL/curl again to compare before/after
2. Restart the relevant service if config or imports changed
3. Specific endpoint tests with sample expected output
4. UI verification if the dashboard is affected
5. Spot-checks on real data (specific tickers, specific values, specific ranges)

The verification section ends with a critical paragraph:

```
Only stop and ask if:
- [Specific list of conditions that genuinely warrant pausing]
- [Things that would make the prompt impossible to fulfill as written]

Anything else: debug, fix, document, move on.
```

This is the single most important pattern in our prompts. **Without it, Copilot defaults to asking permission for every edge case and roadblock**, which makes work slow and expensive. The "anything else: debug, fix, document, move on" line gives Copilot license to use its judgment on small ambiguities and just resolve them, surfacing the resolution in the final report.

The "stop and ask" list should be very short — usually 2-4 conditions. Examples of legitimate stop conditions:

- "If the existing function signature differs significantly from what's needed in a way that requires non-trivial restructuring"
- "If feature math produces values outside reasonable ranges (suggests an actual bug worth surfacing before continuing)"
- "If the BackgroundTask runs for more than 10 minutes without completing"
- "If multiple distinct bugs surface that would benefit from being addressed in separate prompts"

Things that should NOT be stop conditions (because they're normal and can be resolved by Copilot):

- File doesn't exist exactly where expected → look for nearby variants
- Variable name differs from prompt → use the actual name
- Test failed → fix it
- Edge case not specified → make a reasonable choice and document it
- Something in the codebase is messy → either ignore or improve, don't ask

### Final report

A bullet list of what Copilot should cover in its report. Always includes:

- Files modified (with paths)
- The diagnosis output
- The verification output
- Sample data after changes (specific values, not just "it worked")
- Anything unexpected or noteworthy
- Any deviations from the prompt and why

The "deviations from the prompt and why" line is critical. It explicitly invites Copilot to deviate from the prompt when the prompt was wrong, and to surface that deviation honestly. **This is how we catch prompts where the assumption was wrong without losing the work that was correctly done despite the wrong assumption.**

---

## Tone and language patterns

Prompts use direct, concrete language. They avoid:

- Hedging ("perhaps", "maybe consider", "you might want to")
- Apologetic framing
- Unnecessarily formal phrasing
- Vague success criteria ("make sure it works well")

Prompts prefer:

- "Add the four ratio computations to features.py" over "Consider implementing ratio computations"
- "The expected row count is 30-80" over "There should be a reasonable number of rows"
- "If the spot-check reveals values outside these ranges, flag them but don't fix" over "Use your discretion on data quality"

When specifying expected behavior, give concrete examples. "AAPL implied move should be 3-5%" not "implied moves should be reasonable".

When something is intentionally not being done in this prompt, say so explicitly with reasoning: "We don't add 12q computation here because realized_move_avg_12q doesn't exist in features.py — separate decision."

---

## Workspace awareness

Ben works across multiple repositories on a multi-machine setup:

- BenTrade (frontend trading app, primary research workflow)
- Company Evaluator (CE — five-pillar fundamental scoring service)
- Earnings Vol Analyzer (EVA — pre-earnings vol mispricing service)

Each repo has its own `APP_CONTEXT.md` and its own Copilot session. Prompts must be explicit about which workspace they target. This goes at the top of the Scope section:

> This is an EVA-only prompt. BenTrade is unchanged.

> This is a BenTrade frontend-only prompt. EVA service code is NOT modified.

> This affects both BenTrade backend (proxy) and frontend (dashboard) but not EVA.

If the prompt targets the wrong workspace, Copilot will honestly report that the files don't exist — but that's a wasted iteration. State the workspace explicitly to avoid it.

---

## Service-side vs frontend-side fixes

When a UI bug appears, the natural instinct is "fix the frontend." But many UI bugs are actually service-side data shape gaps. The diagnostic question is:

> Does the data the UI needs actually arrive in the response, or is it being filtered out at the service layer?

If the service response doesn't include the field, no amount of frontend code will display it. Fix the service. If the service response does include the field but the UI doesn't render it, fix the frontend.

This pattern surfaced repeatedly in EVA. Multiple "frontend bugs" turned out to be EVA's compact projection filtering out fields the dashboard expected. Diagnose the data flow before writing the fix prompt.

---

## Surgical scope

Prefer many small surgical prompts over fewer large refactor prompts. A prompt that touches one file, adds 15 lines, and runs in 5 minutes is dramatically better than one that touches six files and runs for an hour.

Reasons:

- Smaller prompts mean smaller blast radius if something goes wrong
- Each prompt produces a clean git tag boundary (pre-X / post-X)
- Iteration is faster — if the first prompt is wrong, the next one is small
- Copilot makes fewer judgment calls per prompt, so behavior is more predictable

When a task naturally splits into multiple steps (e.g., "fix the data shape, then fix the rendering"), make them separate prompts unless they're tightly coupled.

---

## Git tag discipline

For any prompt that produces a meaningful change, instruct Copilot to tag the commit boundaries:

```
git tag pre-realized-implied-ratio
[work happens]
git commit -m "..."
git tag post-realized-implied-ratio
```

This creates clean rollback points and makes "before/after" comparisons trivial. Copilot does this automatically when prompts include the pattern.

---

## Documentation infrastructure

Three documentation surfaces that should always exist in any application repo Ben builds:

1. **`.github/copilot-instructions.md`** — repo-level guidance for Copilot. Workflow patterns, code style, "don't do X" rules.

2. **`docs/APP_CONTEXT.md`** — canonical onboarding doc. What the app does, architecture, key file paths, recent decisions, current state. Generated/updated periodically (~once per phase or after major milestones).

3. **`docs/`** — design docs for major decisions. Architecture, data model, API contract.

Prompts reference these by path: `Reference '.github/copilot-instructions.md' and 'docs/APP_CONTEXT.md'`. This is at the end of the Scope section in nearly every prompt.

---

## Common antipatterns to avoid in prompts

**Asking Copilot to investigate when you already know the answer.** If you already know the bug is in `_selectRow`, say so. Don't make Copilot rediscover it.

**Adding "consider X" or "think about Y" suggestions.** These are interpretation overhead. Either prescribe X or don't mention it.

**Letting prompts grow to cover multiple distinct concerns.** Split them. A prompt that fixes a bug AND adds a feature AND refactors a file is three prompts in a trench coat.

**Vague stop conditions.** "Stop if anything looks wrong" is useless. "Stop if the function signature differs significantly from what's needed" is concrete.

**Defensive wrappers that mask the actual bug.** Sometimes appropriate (when bug is hard to reproduce), often overengineering. Prefer "find the bug, fix the bug" over "wrap everything in try/catch."

**Diagnosis without fix authority.** Some prompts gather diagnostic info but don't authorize a fix. Bad pattern. Either let Copilot fix what it finds, or don't have it diagnose at all.

---

## When Ben pushes back

Ben is sometimes terse when prompts are bad. Specific signals:

- "You completely botched that prompt" → reread the actual problem statement, write a much smaller and more targeted prompt
- "I never said X" → check what was actually said vs assumed
- "Just fix it" → the previous prompt was overengineered. Cut everything that isn't directly addressing the stated problem
- "Quit making this complicated" → strip diagnosis, strip wrappers, strip scope sections that aren't load-bearing

When Ben pushes back, the response should be:

1. Acknowledge briefly
2. Generate a much shorter prompt
3. Cut everything that isn't strictly necessary

Don't try to defend the previous prompt. Don't add more sections to be safe. Just write a smaller, sharper prompt.

---

## What "complete" looks like

A prompt is complete when:

- The Scope clearly says what's changing and why
- The Required diagnosis lists what to read first and what to capture as before-state
- The In scope sections are numbered and each describes one coherent change
- The Out of scope explicitly excludes adjacent work
- The Verification has concrete checks with expected values
- The "Only stop and ask if" list is short and specific
- The Final report includes "deviations from the prompt and why"

If any section is generic or hand-wavy, sharpen it before sending.

---

## Quick reference: the key sentences

These specific phrases appear in nearly every prompt and should not be omitted:

> "Reference `.github/copilot-instructions.md`."
>
> "Read these files end-to-end before writing any code."
>
> "Only stop and ask if [...]. Anything else: debug, fix, document, move on."
>
> "Any deviations from the prompt and why."
>
> "This is an EVA-only prompt." / "This is a BenTrade-only prompt."