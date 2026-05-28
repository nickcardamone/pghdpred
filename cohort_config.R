# ══════════════════════════════════════════════════════════════════════════════
# cohort_config.R
# Study 1 — PGHD Pre-Event Surveillance Cohort
# Centralised cohort derivation parameters
#
# SOURCE this script at the top of the consort-pipeline chunk:
#   source("cohort_config.R")
#
# All downstream code should reference these named objects rather than
# hardcoded dates so that a single edit here propagates everywhere.
# ══════════════════════════════════════════════════════════════════════════════

# ── Outcome window ─────────────────────────────────────────────────────────────
# Number of days after the observation week end-date within which a composite
# event (ED visit, inpatient admission, or death) counts as a positive outcome.
max_horizon <- 30L                    # days;  change to 14L for 14-day horizon sensitivity

# ── Uniform follow-up window ──────────────────────────────────────────────────
# Every enrolled patient receives exactly this many days of potential follow-up.
# All patients in the cohort therefore have the same maximum observation window,
# so differences in follow-up length reflect events / censoring, not enrolment timing.
#
# Design rationale:
#   data_end_raw       = last date with observed PGHD data
#   last_observable    = latest week-end date on which a max_horizon-day outcome
#                        window is fully observable  (= data_end_raw - max_horizon)
#   followup_cutoff    = latest first-upload date that allows a patient to complete
#                        a full max_follow_up_days window before last_observable
#                        (= last_observable - max_follow_up_days)
#
# With max_follow_up_days = 364 (52 weeks) and data through ~May 2026:
#   followup_cutoff ≈ May 2026 - 30 - 364 ≈ March 2025  → captures enrollees
#   through early 2025 and ensures every patient has ≥52 weeks of potential
#   observation (or is censored at first composite event, whichever comes first).
#
# Adjust max_follow_up_days to shift the tradeoff between cohort size and
# minimum observable follow-up:
#   52 weeks (364 d) → followup_cutoff ≈ March 2025  [current]
#   40 weeks (280 d) → followup_cutoff ≈ July 2025
#   30 weeks (210 d) → followup_cutoff ≈ Sep 2025
max_follow_up_days <- 52L * 7L        # 364 days = 52 weeks

# ── Incident-enrolment grace period ───────────────────────────────────────────
# Data extract begins January 1, 2022. inception_cutoff anchors data_start_date
# in the narrative text and is used as the lower-bound label only; the operational
# CONSORT filter is the followup_cutoff upper bound (first upload ≤ followup_cutoff).
inception_cutoff <- as.Date("2022-01-01")

# ── Baseline window ────────────────────────────────────────────────────────────
# Number of event-free weeks required before the first observation week.
# Features derived from this period anchor the within-person normalization
# (e.g. k_steps_7d_pct_from_baseline).
baseline_weeks <- 6L
min_total_weeks <- baseline_weeks + 1L   # baseline + ≥1 observation week

# ── Step 2 data-quality threshold ─────────────────────────────────────────────
# Maximum number of missing days allowed in a 7-day rolling window.
# miss_days_threshold = 3  →  require ≥4 days with step data per week.
miss_days_threshold <- 3L

# ══════════════════════════════════════════════════════════════════════════════
# DERIVED DATES  (computed from the parameters above + data_end_raw)
# These are set here as defaults using today's date.  In the pipeline they will
# be recomputed after pghd_daily_raw is loaded so they reflect actual data.
# ══════════════════════════════════════════════════════════════════════════════
.today            <- Sys.Date()
data_end_raw      <- .today                          # overwritten in pipeline
last_observable   <- data_end_raw  - max_horizon
followup_cutoff   <- last_observable - max_follow_up_days

# Human-readable labels (overwritten in pipeline once real data_end_raw is known)
data_start_date        <- format(inception_cutoff,  "%B %d, %Y")
data_end_date          <- format(data_end_raw,      "%B %d, %Y")
followup_cutoff_label  <- format(followup_cutoff,   "%B %d, %Y")

message(sprintf(
  "cohort_config.R loaded\n  inception_cutoff   : %s\n  followup_cutoff    : %s (data end = %s)\n  max_follow_up_days : %d days (%d weeks)\n  max_horizon        : %d days\n  baseline_weeks     : %d\n  miss_days_threshold: %d",
  inception_cutoff,
  followup_cutoff_label,
  data_end_date,
  max_follow_up_days, max_follow_up_days %/% 7L,
  max_horizon,
  baseline_weeks,
  miss_days_threshold
))
