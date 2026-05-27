### Nick Cardamone
### OCC_PGHDPred
### 2B.3 Health care utilization 
### Date created: 4/29/2025
### Last updated: 5/15/2026

# -----------------------------------------------------------------------------
# Purpose: Extract inpatient/outpatient visit features (ED, urgent care, specialty visits, hospitalizations, LOS, etc.)
# as predictors if before first upload date and as outcomes if within the analysis window (first upload to last upload date plus 30 days).
# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Setup: packages & helpers
# ---------------------------------------------------------------------------
suppressPackageStartupMessages({
  library(DBI)         # DB interface
  library(dbplyr)      # dplyr backend for databases
  library(dplyr)       # data manipulation
  library(data.table)  # fast tabular operations (if needed)
  library(matrixStats) # row/column summary stats
  library(stringr)     # string operations
  library(arrow)       # read/write parquet files
  library(tidyverse)   # includes ggplot2, tidyr, purrr, etc.
  library(lubridate)   # manipulate date features
  library(janitor)     # cleaning names and tables
  library(readxl)      # read Excel input
  library(openxlsx)    # write Excel output
  library(future)      # parallel processing primitives
  library(tictoc)      # timing code blocks
  library(parquetize)  # helper for parquet operations
  library(table1)      # quick table summaries
}
)

# Negative "not in" operator for convenience
`%!in%` <- function(x, y) !('%in%'(x, y))

# ---------------------------------------------------------------------------
# Database connections
# ---------------------------------------------------------------------------
# NOTE: these use Windows ODBC connection strings. Keep credentials and host details out of source control and prefer environment-based config in real projects (e.g. Sys.getenv()).

con <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  timeout = 10
)

cdwwork <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  timeout = 10,
  database = "CDWWork"
)

db_pghpred <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  timeout = 10,
  database = "OCC_PGHDPred"
)

VINCI_IVC_CDS <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  timeout = 10,
  database = "VINCI_IVC_CDS"
)


# ---------------------------------------------------------------------------
# 1) Read cohort and map IDs
# ---------------------------------------------------------------------------

setwd(
  "C://Users//VHAPHICardaN//OneDrive - Department of Veterans Affairs//Desktop//Projects//OPS_Bressman-PGHDPred//pghdpred_deliverable"
)

cohort <- open_dataset('data//pghd_final_full_visits_ids.parquet') %>%
  collect() %>%
  na.omit()
setDT(cohort)
cohort[, `:=`(
  date_first            = as.Date(date_first),
  date_last             = as.Date(date_last),
  obs_end               = as.Date(obs_end),
  one_years_prior_date  = as.Date(one_years_prior_date),
  two_years_prior_date  = as.Date(two_years_prior_date),
  five_years_prior_date = as.Date(five_years_prior_date),
  last_plus_7           = as.Date(last_plus_7),
  last_plus_30          = as.Date(last_plus_30)
)]

# Global pull window (scalars) — single broad range covering all lookback
# windows and observation periods for all patients.
pull_start <- as.Date("2022-01-01")
pull_end   <- max(cohort$obs_end, na.rm = TRUE)
cat(sprintf("Pull window: %s -> %s  (%s patients)\n",
            pull_start, pull_end, nrow(cohort)))

# Map PatientICN -> PatientSID (cohort ICNs pushed into SQL as a temp table)
xw_icn_sid <- tbl(cdwwork, in_schema('SPatient', 'SPatient')) %>%
  select(PatientSID, PatientICN) %>%
  inner_join(as.data.frame(cohort[, .(PatientICN)]),
             by = "PatientICN", copy = TRUE) %>%
  distinct()

# OMOP PatientICN -> PERSON_ID crosswalk (CMS OMOP source)
omop_xw_util <- tbl(cdwwork, in_schema('OMOPV5Map', 'SPatient_PERSON')) %>%
  inner_join(as.data.frame(cohort[, .(PatientICN)]),
             by = "PatientICN", copy = TRUE) %>%
  select(PatientICN, PERSON_ID) %>%
  distinct()

# ---------------------------------------------------------------------------
# 2) ED and Urgent Care — 5 active sources
#    Date filter: pull_start -> pull_end (scalar, identical for all patients)
#    ed_sc130 = 1 : VA direct visit (stop code 130 on Outpat.Visit)
#    ed_sc130 = 0 : Fee-basis / CMS OMOP source
# ---------------------------------------------------------------------------

# ---- Dimension tables ----
dim_ed_stopcode <- tbl(cdwwork, in_schema('Dim', 'StopCode')) %>%
  filter(StopCode == 130L) %>%
  select(StopCodeSID)

dim_urgent_stopcode <- tbl(cdwwork, in_schema('Dim', 'StopCode')) %>%
  filter(StopCode == 131L) %>%
  select(StopCodeSID)

dim_fee_er <- tbl(cdwwork, in_schema('Dim', 'FeePurposeOfVisit')) %>%
  filter(AustinCode %in% c('32', '33')) %>%
  select(FeePurposeOfVisitSID)

ed_cms_concept_ids <- c(9203L, 262L, 581385L)

# ---- Source 1: VA direct ED — primary stop code 130 (ed_sc130 = 1) ----
# Using Outpat.Visit (visit-level) to avoid duplicate rows from Outpat.Workload.
tic("ED src1 (primary SC130)")
ed_src1 <- tbl(cdwwork, in_schema('Outpat', 'Visit')) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  inner_join(dim_ed_stopcode, by = c("PrimaryStopCodeSID" = "StopCodeSID")) %>%
  filter(VisitDateTime >= !!as.character(pull_start),
         VisitDateTime <= !!as.character(pull_end)) %>%
  transmute(PatientICN, ed_date = as.Date(VisitDateTime), ed_sc130 = 1L) %>%
  distinct() %>%
  collect()
setDT(ed_src1); toc()

# ---- Source 2: VA direct ED — secondary stop code 130 (ed_sc130 = 1) ----
tic("ED src2 (secondary SC130)")
ed_src2 <- tbl(cdwwork, in_schema('Outpat', 'Visit')) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  inner_join(dim_ed_stopcode, by = c("SecondaryStopCodeSID" = "StopCodeSID")) %>%
  filter(VisitDateTime >= !!as.character(pull_start),
         VisitDateTime <= !!as.character(pull_end)) %>%
  transmute(PatientICN, ed_date = as.Date(VisitDateTime), ed_sc130 = 1L) %>%
  distinct() %>%
  collect()
setDT(ed_src2); toc()

# ---- Source 3: Fee-basis ER — FeeInpatInvoice (Austin codes 32/33) ----
tic("ED src3 (FeeInpatInvoice)")
ed_src3 <- tbl(cdwwork, in_schema('Fee', 'FeeInpatInvoice')) %>%
  inner_join(dim_fee_er, by = "FeePurposeOfVisitSID") %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  filter(TreatmentFromDateTime >= !!as.character(pull_start),
         TreatmentFromDateTime <= !!as.character(pull_end)) %>%
  transmute(PatientICN, ed_date = as.Date(TreatmentFromDateTime), ed_sc130 = 0L) %>%
  distinct() %>%
  collect()
setDT(ed_src3); toc()

# ---- Source 4: Fee-basis ER — FeeInitialTreatment + FeeServiceProvided ----
tic("ED src4 (FeeInitialTreatment)")
ed_src4 <- tbl(cdwwork, in_schema('Fee', 'FeeInitialTreatment')) %>%
  inner_join(
    tbl(cdwwork, in_schema('Fee', 'FeeServiceProvided')) %>%
      inner_join(dim_fee_er, by = "FeePurposeOfVisitSID") %>%
      select(FeeInitialTreatmentSID),
    by = "FeeInitialTreatmentSID"
  ) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  filter(InitialTreatmentDateTime >= !!as.character(pull_start),
         InitialTreatmentDateTime <= !!as.character(pull_end)) %>%
  transmute(PatientICN, ed_date = as.Date(InitialTreatmentDateTime), ed_sc130 = 0L) %>%
  distinct() %>%
  collect()
setDT(ed_src4); toc()

# ---- Source 7: CMS OMOP — ER visit concept IDs 9203 / 262 / 581385 ----
tic("ED src7 (CMS OMOP)")
ed_src7 <- tbl(cdwwork, in_schema('OMOPV5', 'VISIT_OCCURRENCE')) %>%
  filter(VISIT_CONCEPT_ID %in% !!ed_cms_concept_ids) %>%
  inner_join(omop_xw_util, by = "PERSON_ID") %>%
  filter(VISIT_START_DATE >= !!as.character(pull_start),
         VISIT_START_DATE <= !!as.character(pull_end)) %>%
  transmute(PatientICN, ed_date = as.Date(VISIT_START_DATE), ed_sc130 = 0L) %>%
  distinct() %>%
  collect()
setDT(ed_src7); toc()

# ---- Union: one row per PatientICN x date; ed_sc130=1 if any VA direct ----
ed_all <- rbindlist(list(ed_src1, ed_src2, ed_src3, ed_src4, ed_src7))[
  , .(ed = 1L, ed_sc130 = max(ed_sc130, na.rm = TRUE)),
  by = .(PatientICN, date = ed_date)
]

# ---- Urgent Care: primary stop code 131 ----
tic("Urgent Care (SC131)")
urgent_all <- tbl(cdwwork, in_schema('Outpat', 'Visit')) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  inner_join(dim_urgent_stopcode, by = c("PrimaryStopCodeSID" = "StopCodeSID")) %>%
  filter(VisitDateTime >= !!as.character(pull_start),
         VisitDateTime <= !!as.character(pull_end)) %>%
  transmute(PatientICN, date = as.Date(VisitDateTime), urgent = 1L) %>%
  distinct() %>%
  collect()
setDT(urgent_all); toc()

cat(sprintf("  ED events: %s rows  |  Urgent: %s rows\n",
            format(nrow(ed_all),    big.mark = ","),
            format(nrow(urgent_all), big.mark = ",")))

# ---------------------------------------------------------------------------
# 3) Inpatient records — CDW + CMS, validity filter, cross-source dedup,
#    + transfer / observation rollup
#
# CAN paper rule: include medical, surgical, specialty, and mental health.
# Exclude pure long-term / non-acute facilities (Nursing_Home, Domiciliary,
# PRRTP, Blind_Rehabilitation).
# Observation stays: proxy = LOS 0 days + non-psychiatric/non-ICU specialty.
# Observation -> full-admission conversions are absorbed by the consecutive-
# day rollup below (CAN paper methodology).
# ---------------------------------------------------------------------------

# Long-term / non-acute specialty groups — excluded from index event eligibility
non_acute_grps <- c("Nursing_Home", "Domiciliary", "PRRTP", "Blind_Rehabilitation")

# ---- 3a) CDW pull ----
tic("Inpatient CDW pull")
inpat_raw <- tbl(cdwwork, in_schema('Inpat', 'Inpatient')) %>%
  select(
    InpatientSID, PatientSID, Sta3n,
    AdmitDiagnosis, AdmitDateTime, DischargeDateTime,
    DischargeSpecialtySID, Discharge45WardLocationSID,
    AdmitWardLocationSID, ProviderSID
  ) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  left_join(
    tbl(cdwwork, in_schema('Dim', 'Specialty')) %>%
      select(Specialty, SpecialtySID, SpecialtyIEN, MedicalService, BedSectionSID, PTFCode),
    by = c("DischargeSpecialtySID" = "SpecialtySID")
  ) %>%
  left_join(
    tbl(cdwwork, in_schema('Dim', 'BedSection')) %>%
      select(BedSectionSID, BedSectionCode, BedSectionAbbreviation, BedSectionIEN),
    by = "BedSectionSID"
  ) %>%
  filter(DischargeDateTime >= !!as.character(pull_start),
         DischargeDateTime <= !!as.character(pull_end)) %>%
  collect()
setDT(inpat_raw); toc()

# HERC specialty mapping (https://vaww.herc.research.va.gov/include/page.asp?id=inpatient)
inpat_raw[, inpt_grp := case_when(
  SpecialtyIEN %in% c('21', '36') ~ 'Blind_Rehabilitation',
  SpecialtyIEN %in% c('24', '30', '31', '34', '83') |
    PTFCode %in% c('1E', '1F', '1H', '1J') |
    (suppressWarnings(as.integer(SpecialtyIEN)) >= 1 &
       suppressWarnings(as.integer(SpecialtyIEN)) <= 11) |
    grepl('^1[4-9]$', SpecialtyIEN) | is.na(SpecialtyIEN) ~ 'Acute_Medicine',
  SpecialtyIEN %in% c('20', '35', '41', '82') |
    PTFCode %in% c('1D', '1N') ~ 'Rehabilitation',
  SpecialtyIEN %in% c('22', '23') ~ 'Spinal_Cord_Injury',
  SpecialtyIEN %in% c('65', '78', '97') |
    PTFCode %in% c('1G') |
    (suppressWarnings(as.integer(SpecialtyIEN)) >= 48 &
       suppressWarnings(as.integer(SpecialtyIEN)) <= 62) ~ 'Surgery',
  SpecialtyIEN %in% c('25', '26', '28', '29', '33', '38', '39', '70', '71', '75',
                      '76', '77', '79', '89') |
    PTFCode %in% c('1K', '1L') | grepl('^9[1-4]$', SpecialtyIEN) ~ 'Psychiatry',
  SpecialtyIEN %in% c('27', '72', '73', '74', '84', '90', '1M') ~ 'Substance_Abuse',
  SpecialtyIEN %in% c('32', '40') ~ 'Intermediate_Medicine',
  SpecialtyIEN %in% c('37') | grepl('^8[5-8]$', SpecialtyIEN) ~ 'Domiciliary',
  SpecialtyIEN %in% c('64', '80', '81', '95', '96') |
    PTFCode %in% c('1A', '1B', '1C') |
    grepl('^4[2-7]$', SpecialtyIEN) | grepl('^6[6-9]$', SpecialtyIEN) ~ 'Nursing_Home',
  SpecialtyIEN %in% c('38', '39') | grepl('^2[5-9]$', SpecialtyIEN) ~ 'PRRTP',
  SpecialtyIEN %in% c('12', '13', '63') ~ 'ICU',
  TRUE ~ 'Unidentified'
)]

inpat_cdw <- inpat_raw[
  !is.na(AdmitDateTime) & !is.na(DischargeDateTime),
  .(PatientICN,
    admit_date     = as.Date(AdmitDateTime),
    discharge_date = as.Date(DischargeDateTime),
    inpt_grp,
    SOURCE = "CDW")
]

# Flag probable observation stays: LOS = 0 AND not an acute specialty.
# These rows are kept; the rollup below absorbs them if contiguous with a
# full admission (CAN paper observation -> full-admission logic).
inpat_cdw[, los := as.integer(discharge_date - admit_date)]
inpat_cdw[, inpat_obs_proxy := as.integer(
  los == 0L & !inpt_grp %in% c("Psychiatry", "ICU", "Surgery", "Acute_Medicine")
)]

# ---- 3b) CMS source: VINCI_IVC_CDS claim headers ----
tic("Inpatient CMS (IVC_CDS)")
IVC_CDS_raw <- tbl(VINCI_IVC_CDS, in_schema('IVC_CDS', 'CDS_Claim_Header')) %>%
  filter(IsCurrent == "Y",
         Admission_Date >= !!as.character(pull_start),
         Discharge_Date  <= !!as.character(pull_end)) %>%
  inner_join(as.data.frame(cohort[, .(Patient_ICN = PatientICN)]),
             by = "Patient_ICN", copy = TRUE) %>%
  collect()
setDT(IVC_CDS_raw); toc()

inpat_cms <- IVC_CDS_raw[
  !is.na(Admission_Date) & !is.na(Discharge_Date),
  .(PatientICN      = Patient_ICN,
    admit_date      = as.Date(Admission_Date),
    discharge_date  = as.Date(Discharge_Date),
    inpt_grp        = NA_character_,
    SOURCE          = "CMS",
    los             = as.integer(as.Date(Discharge_Date) - as.Date(Admission_Date)),
    inpat_obs_proxy = 0L)   # CMS: no same-day special handling; rely on rollup
][discharge_date >= admit_date]  # drop impossible dates

# ---- 3c) Cross-source deduplication (CDW wins on date-range overlap) ----
setkey(inpat_cdw, PatientICN)
setkey(inpat_cms, PatientICN)

cdw_intervals <- inpat_cdw[, .(PatientICN, cdw_admit = admit_date, cdw_discharge = discharge_date)]
cms_overlaps  <- inpat_cms[cdw_intervals, on = "PatientICN", allow.cartesian = TRUE][
  admit_date <= cdw_discharge & cdw_admit <= discharge_date,
  .(PatientICN, admit_date)
]
inpat_cms_dedup <- inpat_cms[!cms_overlaps, on = .(PatientICN, admit_date)]

inpat_combined <- rbindlist(list(inpat_cdw, inpat_cms_dedup), fill = TRUE)

# ---- 3d) Transfer + observation rollup ----
# A new episode begins when admit_date > prior discharge_date + 1 day.
# Observation stays (LOS=0) immediately before a full admission are absorbed
# into the episode — their admit_date becomes the episode start, and the full
# admission's discharge_date becomes the episode end.
setorder(inpat_combined, PatientICN, admit_date)
inpat_combined[, discharge_int := as.integer(discharge_date)]
inpat_combined[, admit_int     := as.integer(admit_date)]
inpat_combined[,
               new_episode := as.integer(is.na(data.table::shift(discharge_int)) | admit_int > data.table::shift(discharge_int) + 1L),
               by = PatientICN
]
inpat_combined[, c("discharge_int", "admit_int") := NULL]
inpat_combined[, episode_id := cumsum(new_episode), by = PatientICN]

inpat_episodes <- inpat_combined[,
                                 .(episode_admit_date    = min(as.integer(admit_date)),
                                   episode_discharge_date = max(as.integer(discharge_date)),
                                   # Prefer CDW specialty (non-NA inpt_grp) if present in episode
                                   inpt_grp  = {g <- na.omit(inpt_grp); if (length(g)) g[1L] else NA_character_},
                                   obs_only  = as.integer(all(inpat_obs_proxy == 1L))  # 1 if entire episode is obs-proxy
                                 ),
                                 by = .(PatientICN, episode_id)
]
# Rebuild Date class from integer days-since-epoch — aggregation strips the class
inpat_episodes[, episode_admit_date     := as.Date(episode_admit_date,     origin = "1970-01-01")]
inpat_episodes[, episode_discharge_date := as.Date(episode_discharge_date, origin = "1970-01-01")]
inpat_episodes[is.na(inpt_grp), inpt_grp := "Unidentified"]

# ---- 3e) Build inpat_daily (one row per rolled-up episode) ----
# inpat_acute_any : eligible for index events (excludes non-acute and obs-only)
# inpat_any       : all episodes including non-acute (backward compat with 3D)
# NOTE: avoid renaming Date columns inside [, .(newname = col)] — data.table
# strips the Date class during that rename. Use setnames() instead.
inpat_daily <- inpat_episodes[,
                              .(PatientICN,
                                episode_admit_date,
                                episode_discharge_date,
                                inpat_obs_only = obs_only,
                                inpat_any      = 1L,
                                inpat_acute_any     = as.integer(!inpt_grp %in% non_acute_grps & obs_only == 0L),
                                inpat_med_surg      = as.integer(inpt_grp %in% c("Acute_Medicine", "Surgery")),
                                inpat_mental_health = as.integer(inpt_grp == "Psychiatry"),
                                inpat_nursing_home  = as.integer(inpt_grp == "Nursing_Home"),
                                inpat_other         = as.integer(
                                  !inpt_grp %in% c("Acute_Medicine", "Surgery", "Nursing_Home", "Psychiatry") &
                                    obs_only == 0L
                                )
                              )
]
setnames(inpat_daily,
         c("episode_admit_date", "episode_discharge_date"),
         c("date",               "discharge_date"))

cat(sprintf("  Inpatient episodes (CDW+CMS deduped + rollup): %s\n",
            format(nrow(inpat_daily), big.mark = ",")))
cat(sprintf("  Obs-only episodes flagged: %s\n", sum(inpat_daily$inpat_obs_only)))

# ---------------------------------------------------------------------------
# 4) Wide binary indicator grid — one row per PatientICN x date
#    Merges ED / urgent care / inpatient into a single event table.
#    Binary 0/1 columns; discharge_date and inpat_los carried for IP rows.
# ---------------------------------------------------------------------------

# Merge ED and urgent care into one table
ed_urg_dt <- merge(
  ed_all,
  urgent_all[, .(PatientICN, date, urgent)],
  by  = c("PatientICN", "date"),
  all = TRUE
)
ed_urg_dt[is.na(ed),       ed       := 0L]
ed_urg_dt[is.na(ed_sc130), ed_sc130 := 0L]
ed_urg_dt[is.na(urgent),   urgent   := 0L]

# Normalize join-key types immediately before the inpatient merge.
# This is the most reliable point to defend against class loss from earlier
# data.table aggregations or column selection.
ed_urg_dt[, date := as.Date(date, origin = "1970-01-01")]
inpat_daily[, date := as.Date(date, origin = "1970-01-01")]
inpat_daily[, discharge_date := as.Date(discharge_date, origin = "1970-01-01")]

# Merge with inpatient events
hcu_events_daily <- merge(
  ed_urg_dt,
  inpat_daily[, .(PatientICN, date, discharge_date, inpat_obs_only,
                  inpat_any, inpat_acute_any,
                  inpat_med_surg, inpat_mental_health,
                  inpat_nursing_home, inpat_other)],
  by  = c("PatientICN", "date"),
  all = TRUE
)

for (col in c("inpat_any", "inpat_acute_any", "inpat_obs_only",
              "inpat_med_surg", "inpat_mental_health",
              "inpat_nursing_home", "inpat_other")) {
  hcu_events_daily[is.na(get(col)), (col) := 0L]
}
hcu_events_daily[, inpat_los := as.integer(discharge_date - date)]

setcolorder(hcu_events_daily,
            c("PatientICN", "date",
              "ed", "ed_sc130", "urgent",
              "inpat_any", "inpat_acute_any", "inpat_obs_only",
              "inpat_med_surg", "inpat_mental_health",
              "inpat_nursing_home", "inpat_other",
              "discharge_date", "inpat_los"))

write_parquet(hcu_events_daily, "data//hcu_events_daily.parquet")
cat(sprintf("  hcu_events_daily: %s rows saved.\n",
            format(nrow(hcu_events_daily), big.mark = ",")))

# ---- Backward-compatible parquet files (consumed by 3D) ----
# ed_urgent_daily: schema unchanged; ed_sc130 is an additive column
write_parquet(
  hcu_events_daily[(ed == 1L | urgent == 1L),
                   .(PatientICN, date, ed, ed_sc130, urgent)],
  "data\\ed_urgent_daily.parquet"
)

# inpat_daily: schema unchanged for 3D compatibility
write_parquet(
  inpat_daily[, .(PatientICN, date, discharge_date,
                  inpat_any, inpat_med_surg, inpat_mental_health,
                  inpat_nursing_home, inpat_other)],
  "data\\inpat_daily.parquet"
)

# ---------------------------------------------------------------------------
# 5) Preliminary index event identification
#    First qualifying ED or acute-IP contact on or after each patient's
#    date_first (PGHD first-upload date).  No 30-day observation filter here —
#    that eligibility gate is applied in 3D.  When ED and IP occur on the same
#    day, inpatient is preferred (it carries a discharge_date for the post-
#    discharge window).
# ---------------------------------------------------------------------------
cat("\nIdentifying preliminary index events...\n")

ix_candidates <- merge(
  hcu_events_daily[
    (ed == 1L | inpat_acute_any == 1L) & inpat_obs_only == 0L,
    .(PatientICN, date, ed, inpat_acute_any, discharge_date)
  ],
  cohort[, .(PatientICN, date_first)],
  by = "PatientICN"
)[date >= date_first]

# Prefer inpatient over ED on the same day; then take the earliest date
setorder(ix_candidates, PatientICN, date, -inpat_acute_any)
index_event_prelim <- ix_candidates[, .SD[1L], by = PatientICN][,
                                                                .(PatientICN,
                                                                  index_date           = date,
                                                                  index_ed             = ed,
                                                                  index_inpat          = inpat_acute_any,
                                                                  index_discharge_date = fifelse(inpat_acute_any == 1L, discharge_date, date)
                                                                )
]

cat(sprintf("  Patients with a preliminary index event: %s / %s\n",
            nrow(index_event_prelim), nrow(cohort)))
cat("  Index event type (ED=1, Inpat=1, both=1 on same-day tie):\n")
print(table(ED = index_event_prelim$index_ed,
            Inpat = index_event_prelim$index_inpat))

write_parquet(index_event_prelim, "data\\index_event_prelim.parquet")

# ---------------------------------------------------------------------------
# 6) CAN-score prior features — 365-day lookback from preliminary index_date
#    Features are computed relative to the admission date, not date_first,
#    consistent with the published CAN score methodology.
# ---------------------------------------------------------------------------
cat("\nComputing CAN prior features (365d lookback from index_date)...\n")

can_window <- merge(
  hcu_events_daily,
  index_event_prelim[, .(PatientICN, index_date)],
  by = "PatientICN"
)[date >= (index_date - 365L) & date < index_date]

can_features_prior <- can_window[,
                                 .(can_ed_n              = sum(ed,              na.rm = TRUE),
                                   can_ed_any            = as.integer(sum(ed,   na.rm = TRUE) > 0L),
                                   can_ed_sc130_n        = sum(ed_sc130,          na.rm = TRUE),
                                   can_urgent_n          = sum(urgent,             na.rm = TRUE),
                                   can_urgent_any        = as.integer(sum(urgent,  na.rm = TRUE) > 0L),
                                   can_inpat_n           = sum(inpat_acute_any,    na.rm = TRUE),
                                   can_inpat_any         = as.integer(sum(inpat_acute_any, na.rm = TRUE) > 0L),
                                   can_inpat_los_total   = sum(as.numeric(inpat_los), na.rm = TRUE),
                                   can_inpat_mh_any      = as.integer(sum(inpat_mental_health, na.rm = TRUE) > 0L),
                                   can_inpat_medsurg_any = as.integer(sum(inpat_med_surg,      na.rm = TRUE) > 0L)
                                 ),
                                 by = PatientICN
]

# Ensure all cohort patients appear (0 for those with no prior HCU events)
can_features_prior <- merge(
  cohort[, .(PatientICN)],
  can_features_prior,
  by    = "PatientICN",
  all.x = TRUE
)
for (col in setdiff(names(can_features_prior), "PatientICN")) {
  can_features_prior[is.na(get(col)), (col) := 0L]
}

write_parquet(can_features_prior, "data\\can_features_prior.parquet")
cat(sprintf("  can_features_prior: %s patients, %s features.\n",
            nrow(can_features_prior), ncol(can_features_prior) - 1L))

# ---------------------------------------------------------------------------
# 7) Appointment no-show — broad pull, per-patient window filtered in R
#    Collecting the full appointment table for cohort patients over pull_start
#    -> pull_end avoids a per-row date-filter that SQL cannot push-down as a
#    sargable predicate.  The per-patient windows are applied after collect().
# ---------------------------------------------------------------------------
no_show_codes <- c("NA", "N")

tic("Appointment no-show pull")
appt_raw <- tbl(cdwwork, in_schema('Appt', 'Appointment')) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  filter(
    AppointmentDateTime >= !!as.character(pull_start),
    AppointmentDateTime <= !!as.character(pull_end),
    AppointmentStatus   %in% !!no_show_codes
  ) %>%
  transmute(PatientICN, appt_date = as.Date(AppointmentDateTime)) %>%
  distinct() %>%
  collect()
setDT(appt_raw); toc()

# CAN no-show: [index_date - 365, index_date)
no_show_can <- merge(
  appt_raw,
  index_event_prelim[, .(PatientICN, index_date)],
  by = "PatientICN"
)[appt_date >= (index_date - 365L) & appt_date < index_date,
  .(can_appt_noshow_n   = .N,
    can_appt_noshow_any = 1L),
  by = PatientICN
]

can_features_prior <- merge(can_features_prior, no_show_can, by = "PatientICN", all.x = TRUE)
can_features_prior[is.na(can_appt_noshow_n),   can_appt_noshow_n   := 0L]
can_features_prior[is.na(can_appt_noshow_any), can_appt_noshow_any := 0L]

write_parquet(can_features_prior, "data\\can_features_prior.parquet")

# Backward compat: no-show anchored to date_first (consumed by 3D via hc_util_prior)
no_show_prior_compat <- merge(
  appt_raw,
  cohort[, .(PatientICN, one_years_prior_date, date_first)],
  by = "PatientICN"
)[appt_date >= one_years_prior_date & appt_date < date_first,
  .(py_appt_no_show = 1L),
  by = PatientICN
]
write_parquet(no_show_prior_compat, "data\\no_show_prior.parquet")

# ---------------------------------------------------------------------------
# 8) VINCI CDS (IVC_CDS) — CAN features + backward-compat prior summary
#    IVC_CDS_raw was collected in Section 3b above.
# ---------------------------------------------------------------------------
IVC_CDS_events <- IVC_CDS_raw[
  !is.na(Admission_Date),
  .(PatientICN = Patient_ICN,
    date       = as.Date(Admission_Date),
    los        = as.numeric(as.Date(Discharge_Date) - as.Date(Admission_Date)))
][los >= 0]

# CAN features: CMS admissions in 365 days before index_date
can_cms <- merge(
  IVC_CDS_events,
  index_event_prelim[, .(PatientICN, index_date)],
  by = "PatientICN"
)[date >= (index_date - 365L) & date < index_date,
  .(can_cms_inpat_n    = .N,
    can_cms_inpat_any  = 1L,
    can_cms_los_total  = sum(los, na.rm = TRUE)),
  by = PatientICN
]

can_features_prior <- merge(can_features_prior, can_cms, by = "PatientICN", all.x = TRUE)
for (col in c("can_cms_inpat_n", "can_cms_inpat_any", "can_cms_los_total")) {
  can_features_prior[is.na(get(col)), (col) := 0L]
}
write_parquet(can_features_prior, "data\\can_features_prior.parquet")

# Backward compat: IVC_CDS anchored to two_years_prior_date / date_first (for 3D)
IVC_CDS_prior <- merge(
  IVC_CDS_events,
  cohort[, .(PatientICN, two_years_prior_date, date_first)],
  by = "PatientICN"
)[date >= two_years_prior_date & date < date_first,
  .(py2_IVC     = 1L,
    py2_IVC_n   = .N,
    py2_IVC_los = if_else(sum(los, na.rm = TRUE) > 730, 730, sum(los, na.rm = TRUE))),
  by = PatientICN
]
write_parquet(IVC_CDS_prior, "data\\IVC_CDS_prior.parquet")

# ---------------------------------------------------------------------------
# 9) Assemble backward-compatible hc_util_prior (anchored to date_first)
#    Consumed by 3D to build cohort_static_preadmission.parquet.
#    NOTE: For the CAN model itself, use can_features_prior (anchored to
#    index_date) which better reflects the CAN score methodology.
# ---------------------------------------------------------------------------
cat("\nAssembling backward-compatible hc_util_prior (anchored to date_first)...\n")

# ED / urgent care: any event before date_first
ed_urgent_prior <- merge(
  hcu_events_daily[(ed == 1L | urgent == 1L), .(PatientICN, date, ed, urgent)],
  cohort[, .(PatientICN, date_first)],
  by = "PatientICN"
)[date < date_first,
  .(py_ed     = as.integer(sum(ed,     na.rm = TRUE) > 0L),
    py_urgent = as.integer(sum(urgent, na.rm = TRUE) > 0L)),
  by = PatientICN
]

# Inpatient: 2-year prior window
inpat_prior <- merge(
  inpat_daily,
  cohort[, .(PatientICN, two_years_prior_date, date_first)],
  by = "PatientICN"
)[date >= two_years_prior_date & date < date_first,
  .(py2_inpat_any            = 1L,
    py2_inpat_med_surg       = as.integer(sum(inpat_med_surg,      na.rm = TRUE) > 0L),
    py2_inpat_mental_health  = as.integer(sum(inpat_mental_health, na.rm = TRUE) > 0L),
    py2_inpat_nursing_home   = as.integer(sum(inpat_nursing_home,  na.rm = TRUE) > 0L),
    py2_inpat_other          = as.integer(sum(inpat_other,         na.rm = TRUE) > 0L)),
  by = PatientICN
]
write_parquet(inpat_prior, "data\\inpat_prior.parquet")

hc_util_prior <- merge(cohort[, .(PatientICN)], ed_urgent_prior,        by = "PatientICN", all.x = TRUE)
hc_util_prior <- merge(hc_util_prior,            inpat_prior,            by = "PatientICN", all.x = TRUE)
hc_util_prior <- merge(hc_util_prior,            IVC_CDS_prior,          by = "PatientICN", all.x = TRUE)
hc_util_prior <- merge(hc_util_prior,            no_show_prior_compat,   by = "PatientICN", all.x = TRUE)

for (col in setdiff(names(hc_util_prior), "PatientICN")) {
  hc_util_prior[is.na(get(col)), (col) := 0L]
}
write_parquet(hc_util_prior, "data\\hc_util_prior.parquet")

cat("\n=== OUTPUT SUMMARY ===\n")
cat(sprintf("  hcu_events_daily    : %s rows  (PatientICN x date wide grid)\n",
            format(nrow(hcu_events_daily),  big.mark = ",")))
cat(sprintf("  index_event_prelim  : %s patients with a qualifying index event\n",
            format(nrow(index_event_prelim), big.mark = ",")))
cat(sprintf("  can_features_prior  : %s patients, %s features (anchored to index_date)\n",
            format(nrow(can_features_prior), big.mark = ","),
            ncol(can_features_prior) - 1L))
cat(sprintf("  hc_util_prior (compat): %s patients, %s features (anchored to date_first)\n",
            format(nrow(hc_util_prior),      big.mark = ","),
            ncol(hc_util_prior) - 1L))
cat("  Backward-compat files: ed_urgent_daily, inpat_daily, inpat_prior,\n")
cat("                         IVC_CDS_prior, no_show_prior  (consumed by 3D)\n")
