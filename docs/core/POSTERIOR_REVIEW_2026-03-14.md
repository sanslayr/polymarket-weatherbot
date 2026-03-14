# Posterior Review - 2026-03-14

## Scope

This review focuses on the current `posterior -> analysis snapshot -> report` chain, with extra attention on:

- branch-conditioned range evolution
- ENS surface integration
- report reasoning quality
- runtime/cache behavior that affects `/look`

## Current Strengths

- The final displayed range is now sourced from `weather_posterior.range_hint`, rather than being re-capped a second time in snapshot.
- ENS has been narrowed to `surface-first` usage, which is more honest than pretending ENS currently provides reliable member-level vertical-structure guidance for every case.
- `posterior_feature_vector` now carries matched-branch, member-evolution, and history-alignment state in a way that can support later learning artifacts.
- Report reasoning is much closer to a logic chain than before, and false-positive `再摸前高` wording has been tightened.

## Findings

### High: Numeric posterior still over-trusts broad `second_peak_potential`

The report-layer wording around false `再摸前高` has been tightened, and `second_peak_retest` branch labeling now requires stronger evidence. But core posterior scoring still applies large numeric adjustments directly from broad `second_peak_potential`.

Current examples:

- `weather_posterior_core.py`
  - center adjustment still adds warm bias from `second_peak_potential`
  - `new_high_score` gets a large boost from `second_peak_potential`
  - `lock_score` gets a large penalty from `second_peak_potential`

This means a case can already be protected from false second-peak wording, yet still retain a numerically too-warm upper tail or too-low lock probability because the broad shape prior is still being used as if it were active rebreak evidence.

Recommended change:

- Stop using `second_peak_potential` directly as a strong posterior driver in near-window / in-window cases.
- Replace it with a narrower gate based on:
  - `should_discuss_second_peak`
  - `rebound_mode`
  - `future_candidate_role == secondary_peak_candidate`
  - observed progress not being a fresh high still rising
- Treat `second_peak_potential` as a weak station/shape prior, not as direct live rebreak evidence.

### High: Posterior truth and report truth are still not fully aligned

The report can now suppress misleading second-peak wording even when broader posterior shape fields still imply elevated rebreak risk. That is better than bad wording, but it also means:

- the displayed reasoning
- the posterior numeric shape
- and the learning target interpretation

are not always fully aligned.

This is especially relevant for future posterior learning, because the system can learn the wrong lesson if the numeric posterior is still shaped by broad priors while the report has already corrected to the live-observation interpretation.

Recommended change:

- Promote more of the report-side live gating back into posterior numeric logic.
- Treat the report layer as a renderer, not as the place where semantic correction happens.

### Medium: Key gating logic is duplicated across posterior/report layers

The same kind of “fresh observed high and still rising” suppression now exists in multiple places:

- posterior branch gating
- report reasoning gating
- report focus gating

This duplication is understandable for safety, but it creates drift risk. A future change can fix one layer and leave the other two behind.

Recommended change:

- Move these gates into a shared helper module, for example a small `posterior_live_gates.py`.
- Reuse one source of truth for:
  - fresh-high-still-rising
  - second-peak-retest-ready
  - near-window rebreak eligibility

### Medium: `/look` can still become the fallback builder for ENS surface

The cache/worker path is much better than before, but current behavior still allows `/look` to trigger live ENS surface construction when cache coverage is incomplete.

That has two downsides:

- user-facing latency becomes unpredictable
- the same expensive work can be repeated in the foreground

Recommended change:

- Add explicit `ensemble_source=cache_hit|live_build|fallback_none` perf logging to `/look`.
- Add a stricter foreground timeout for live ENS build.
- If live build exceeds that budget, degrade cleanly to a non-ENS posterior path instead of blocking the request.

### Medium: Surface-path reasoning is still a bit too numeric in some branches

The recent cleanup removed the worst “spreadsheet sentence” cases, but the logic still allows too many quantitative fragments to survive in nearby lines:

- surface mismatch summaries
- future 1-3h path summaries
- branch-resolution condition lines

This is not a correctness bug, but it hurts signal density.

Recommended change:

- Enforce a report rule of:
  - one main directional judgment
  - one decisive confirmation/failure condition
- Keep extra dewpoint/wind/pressure details only when they are truly the main branch discriminator.

### Medium: ENS surface history is directionally right, but still coarse for learning

Matching members against observed history is the right move, but the current historical alignment is still mostly a compact score layer:

- history alignment score
- temp MAE
- trend bias

This is useful, but not yet enough for a real learned posterior update loop.

Recommended change:

- Persist time-indexed history-match features per member, not just aggregates.
- Keep:
  - temperature error path
  - wind direction/speed agreement path
  - pressure tendency agreement path
  - branch switches over time

That will make later learning much easier.

## Recommended Next Moves

### 1. Clean up second-peak numeric influence

Priority: immediate

- Reduce or remove direct `second_peak_potential` boosts in posterior center / new-high / lock scoring.
- Replace them with `second_peak_retest_ready` or similarly narrow live gates.

### 2. Make report logic a thin renderer

Priority: immediate

- Pull duplicated live gating into shared helpers.
- Leave report responsible for phrasing, not for semantic correction.

### 3. Make ENS surface cache-first by construction

Priority: short term

- Batch by valid time and reuse one file for many stations.
- Log cache-hit vs live-build explicitly.
- Fail open faster on foreground requests.

### 4. Prepare posterior learning on branch-conditioned outcomes

Priority: short to medium term

Learning should target:

- center delta under matched branch context
- spread shrink under observation progress
- upper-tail allowance under real follow-through evidence
- lock/new-high calibration by station family and branch family

The runtime interfaces are much closer now, but the next real unlock is to log branch-conditioned realized outcomes in a more time-resolved way.

## Longer-Term Direction

The current system is moving in the right philosophical direction:

- not “trust the deterministic peak”
- not “trust raw ensemble blindly”
- but “update a conditional distribution as observations reveal which path the day is actually taking”

To keep pushing that forward, the north star should be:

- `surface-member path matching`
- `observation-conditioned branch weighting`
- `station-family calibration`
- `posterior numeric logic and report logic sharing the same live gates`

That would move the skill from “heuristic posterior with good report control” toward “genuinely observation-conditioned posterior reasoning”.
