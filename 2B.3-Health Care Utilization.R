### Nick Cardamone
### OCC_PGHDPred
### 2B.3 Health care utilization 
### Date created: 4/29/2025
### Last updated: 10/24/2025

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

# Set working directory
setwd("C:\\Users\\VHAPHICardaN\\OneDrive - Department of Veterans Affairs\\Desktop\\Projects\\OPS_Bressman-PGHDPred\\")

# Cohort: contains PatientICN and upload window variables (e.g. date_first)
cohort <- open_dataset('parquet\\pghd_final_full_visits_ids.parquet') %>%
  collect()

# Map PatientICN -> PatientSID using the CDW SPatient table so we can join to
# CDW tables that use PatientSID. Keep distinct to avoid duplicate joins.
xw_icn_sid <- tbl(cdwwork, in_schema('SPatient', 'SPatient')) %>%
  select(PatientSID, PatientICN) %>%
  inner_join(cohort, by = "PatientICN", copy = TRUE) %>%
  distinct()

#  ---------------------------------------------------------------------------
# 2) Outpatient workload table: ED and Urgent Care
# ---------------------------------------------------------------------------
# Define stop codes of interest (ED = 130, Urgent Care = 131)
urged <- tbl(cdwwork, in_schema('Dim', 'StopCode')) %>%
  filter(StopCode %in% c(130, 131))

# Pull outpatient workload for cohort members in the upload window and We create date from the VisitDateTime and keep only relevant columns.
ed_urgent_daily <- tbl(cdwwork, in_schema('Outpat', 'Workload')) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  inner_join(urged, by = c("PrimaryStopCodeSID" = "StopCodeSID")) %>%
  filter(VisitDateTime >= one_years_prior_date & VisitDateTime <= last_plus_30) %>% 
  transmute(
    PatientICN,
    date = as.Date(VisitDateTime),
    ed = if_else(StopCode == 130, 1, 0),      # ED visit flag
    urgent = if_else(StopCode == 131, 1, 0),   # Urgent care flag
  ) %>% 
  distinct() %>%
  collect()

# Save to parquet
ed_urgent_daily %>% write_parquet("parquet\\ed_urgent_daily.parquet")

# Utilization in year prior:
ed_urgent_prior = ed_urgent_daily %>% 
  inner_join(cohort %>% select(PatientICN, date_first), by = "PatientICN") %>%
  filter(date < date_first) %>%
  group_by(PatientICN) %>%
  summarize(
    PatientICN,
    py_ed = max(ed, na.rm = T), # Any ED visit flag
    py_urgent = max(urgent, na.rm = T),   # Any Urgent care flag
  )

# Save to parquet
ed_urgent_prior %>% write_parquet("parquet\\ed_urgent_prior.parquet")


# ---------------------------------------------------------------------------
# 3) Inpatient records and grouping
# ---------------------------------------------------------------------------
# Pull inpatient admissions in the analysis window and enrich with specialty and bed section metadata. Bring into local environment for further mutation/aggregation.
inpat <- tbl(cdwwork, in_schema('Inpat', 'Inpatient')) %>%
  select(
    InpatientSID,
    PatientSID,
    Sta3n, #site
    AdmitDiagnosis,
    AdmitDateTime,
    DischargeDateTime,
    DischargeSpecialtySID,
    Discharge45WardLocationSID,
    AdmitWardLocationSID,
    ProviderSID
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
  filter(DischargeDateTime >= two_years_prior_date & DischargeDateTime <= last_plus_30) %>% 
  collect()

# Map specialty codes into high-level inpatient groups used for indicator flags based on Health Economics Research Center table:
# https://vaww.herc.research.va.gov/include/page.asp?id=inpatient
inpat_grouping <- inpat %>%
  mutate(
    inpt_grp = case_when(
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
      SpecialtyIEN %in% c('25', '26', '28', '29', '33', '38', '39', '70', '71', '75', '76', '77', '79', '89') |
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
    )
  )

# Create binary indicators for med-surg, mental health, and nursing home stays used in CAN score.
inpat_daily <- inpat_grouping %>%
  transmute(
    PatientICN,
    date = as.Date(AdmitDateTime),
    inpat_any = 1,
    inpat_med_surg = if_else(inpt_grp %in% c("Acute_Medicine", "Surgery"), 1, 0),
    inpat_mental_health = if_else(inpt_grp %in% c("Psychiatry"), 1, 0),
    inpat_nursing_home = if_else(inpt_grp %in% c("Nursing_Home"), 1, 0),
    inpat_other = if_else(inpt_grp %!in% c("Acute_Medicine", "Surgery", "Nursing_Home", "Psychiatry"), 1, 0),
  )

# Save to parquet;
inpat_daily %>% write_parquet("parquet\\inpat_daily.parquet")

# Within2: logical flag whether admission started in two_years_prior_date window
# Inpatient hospitalization in year prior:

inpat_prior = inpat_daily %>% 
  inner_join(cohort %>% select(PatientICN, date_first), by = "PatientICN") %>%
  filter(date < date_first) %>%
  group_by(PatientICN) %>%
  summarize(
    PatientICN,
    py2_inpat_any = 1,
    py2_inpat_med_surg = max(inpat_med_surg),
    py2_inpat_mental_health = max(inpat_mental_health),
    py2_inpat_nursing_home = max(inpat_nursing_home),
    py2_inpat_other = max(inpat_other),
  )

# Persist claim dataset to local parquet. Path is user-specific
inpat_prior %>% write_parquet("parquet\\inpat_prior.parquet")

# ---------------------------------------------------------------------------
# 4) VINCI CDS claims: inpatient claim headers
# --------------------------------------------------------------------------- 
# Pull claims from VINCI IVC_CDS table; restrict to current claims (IsCurrent == 'Y')
claim_form <- data.frame(IsCurrent = 'Y')

IVC_CDS <- tbl(VINCI_IVC_CDS, in_schema('IVC_CDS', 'CDS_Claim_Header')) %>%
  filter(IsCurrent == "Y" & Admission_Date > "2020-01-01") %>%
  inner_join(cohort, by = c("Patient_ICN" = "PatientICN"), copy = TRUE) %>%
  filter(Admission_Date >= two_years_prior_date & Discharge_Date <= last_plus_30) %>%
  collect()

# Compute length-of-stay (los) per admission and aggregate per PatientICN
IVC_CDS_prior <- IVC_CDS %>%
  transmute(
    PatientICN = Patient_ICN,
    date = as.Date(Admission_Date),
    los = as.numeric(as.Date(Discharge_Date) - as.Date(Admission_Date))
  ) %>%
  distinct() %>%
  dplyr::group_by(PatientICN) %>%
  dplyr::summarize(
    py2_IVC = 1,
    py2_IVC_n = n(),
    py2_IVC_los = sum(los),
  )

# Persist claim dataset to local parquet. Path is user-specific
IVC_CDS_prior %>% write_parquet("parquet\\IVC_CDS_prior.parquet")

# ---------------------------------------------------------------------------
# 5) Appointment no-show indicator (prior year)
#  ---------------------------------------------------------------------------

# Known appointment status codes interpreted as no-shows. Adjust to CDW coding
# for your environment.
no_show_codes <- c("NA", "N")

# Flag patients with at least one appointment status in no_show_codes during
# the year prior to date_first. Collect to local memory for joining.
no_show_prior <- tbl(cdwwork, in_schema('Appt', 'Appointment')) %>%
  inner_join(xw_icn_sid, by = "PatientSID") %>%
  filter(
    AppointmentDateTime >= one_years_prior_date,
    AppointmentDateTime < as.Date(date_first),
    AppointmentStatus %in% !!no_show_codes
  ) %>%
  select(PatientICN) %>%
  distinct() %>%
  transmute(PatientICN, 
            py_appt_no_show = 1) %>%
  collect()

# Persist claim dataset to local parquet. Path is user-specific
no_show_prior %>% write_parquet("parquet\\no_show_prior.parquet")

# ---------------------------------------------------------------------------
# 6) Assemble cohort-level features and fill missing values
# ---------------------------------------------------------------------------

ed_urgent_prior <- open_dataset("parquet\\ed_urgent_prior.parquet") %>% distinct() %>% collect()
inpat_prior <- open_dataset("parquet\\inpat_prior.parquet") %>% distinct() %>% collect()
IVC_CDS_prior <- open_dataset("parquet\\IVC_CDS_prior.parquet")  %>% distinct() %>% collect()
no_show_prior <- open_dataset("parquet\\no_show_prior.parquet")  %>% distinct() %>% collect()

# Health care utilization in 1-2 years prior to first data upload date:
hc_util_prior <- cohort %>% 
  select(PatientICN) %>%
  left_join(ed_urgent_prior, by = "PatientICN") %>%
  left_join(inpat_prior, by = "PatientICN") %>%
  left_join(IVC_CDS_prior, by = "PatientICN") %>%
  left_join(no_show_prior, by = "PatientICN")

# Replace NAs for indicator/per-count variables with 0.

hc_util_prior[is.na(hc_util_prior)] <- 0

# Check table prior healthcare utilization variables: table1(~py_ed + py_urgent + py2_inpat_any + py2_inpat_med_surg + py2_inpat_nursing_home + py2_inpat_other + py_appt_no_show, data = hc_util_prior %>% mutate(across(where(is.numeric), as.factor)))

hc_util_prior %>% write_parquet("parquet\\hc_util_prior.parquet")
