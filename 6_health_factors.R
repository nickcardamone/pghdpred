### Nick Cardamone
### OCC_PGHDPred
### 6. Health factors - Tobacco use and Military Sexual Trauma
### Date created: 10/22/2025
### Last updated: 5/15/2026

# Features of interest for modeling:
# 1. Tobacco Use Status (Most Recent, Prior to upload date) - Categorical
# 2. Military Sexual Trauma (Ever) - Categorical

suppressPackageStartupMessages({
library(DBI) # Working with data in databases
library(dbplyr) # Working with data in databases
library(dplyr)
library(stringr) # string var manipulation
library(arrow) # parquet files 
library(tidyverse) # helper functions
library(lubridate)
library(readxl)
library(odbc)
})

'%!in%' <- function(x,y)!('%in%'(x,y))

# ---------------------------------------------------------------------------
# Database connections
# ---------------------------------------------------------------------------

# Connect to CDWWork database
cdwwork <- dbConnect(odbc::odbc(), 
                     .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                     timeout = 10,
                     database = "CDWWork")

# Connect to OCC_PGHDPred database
db_pghpred <- dbConnect(odbc::odbc(), 
                        .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                        timeout = 10,
                        database = "OCC_PGHDPred")

# ---------------------------------------------------------------------------
# 1) Set working directory and load cohort
# ---------------------------------------------------------------------------

setwd(
  "C://Users//VHAPHICardaN//OneDrive - Department of Veterans Affairs//Desktop//Projects//OPS_Bressman-PGHDPred//pghdpred_deliverable"
)

# Cohort: contains PatientICN and upload window variables (e.g. date_first)
cohort = open_dataset('data\\pghd_final_full_visits_ids.parquet') %>% collect() %>% na.omit()

# ---------------------------------------------------------------------------
# M2 anchor: load index events for patients with qualifying ED/IP event
# ---------------------------------------------------------------------------
index_events_m2 <- open_dataset('data\\index_event_prelim.parquet') %>%
  collect() %>%
  filter(!is.na(index_date)) %>%
  select(PatientICN, index_date) %>%
  mutate(index_date = as.Date(index_date))

cohort_m2 <- cohort %>%
  filter(as.Date(date_first) >= as.Date("2023-06-01")) %>%
  inner_join(index_events_m2, by = "PatientICN")

global_pull_end <- max(cohort_m2$index_date, na.rm = TRUE)
cat("M2 cohort:", nrow(cohort_m2), "patients; pull end:", as.character(global_pull_end), "\n")

# Build PatientICN -> PatientSID crosswalk directly from CDWWork
cw <- tbl(cdwwork, in_schema('SPatient', 'SPatient')) %>%
  select(PatientICN, PatientSID) %>%
  inner_join(cohort %>% select(PatientICN), by = "PatientICN", copy = TRUE) %>%
  distinct() %>%
  collect() %>%
  inner_join(cohort %>% select(PatientICN, date_first, five_years_prior_date),
             by = "PatientICN")

# ---------------------------------------------------------------------------
# 2) Tobacco Use Status - Most Recent (Prior to upload date)
# ---------------------------------------------------------------------------

# Load smoking lookup table with groupings
Groupings <- readxl::read_xlsx("xw/Health_Factor_Smoking_Lookup_table.xlsx") %>% 
  transmute(HealthFactorType = HEALTHFACTORTYPE, SmokingFactor)

# Get HealthFactorType dimension table
hf_dim <- tbl(cdwwork, in_schema('Dim', 'HealthFactorType')) %>% 
  select(HealthFactorTypeSID, HealthFactorType)

# Extract all smoking health factors — broad scalar pull; R-side anchor filter applied below
tobacco_hf <- tbl(cdwwork, in_schema('HF', 'HealthFactor')) %>%
  inner_join(hf_dim, by = "HealthFactorTypeSID") %>%
  inner_join(cw, by = "PatientSID", copy = TRUE) %>%
  filter(HealthFactorDateTime >= '2018-01-01' & HealthFactorDateTime <= !!as.character(global_pull_end)) %>%
  distinct() %>%
  inner_join(Groupings, by = "HealthFactorType", copy = TRUE) %>%
  select(PatientICN, PatientSID, Sta3n, HealthFactorTypeSID, HealthFactorType,
         HealthFactorDateTime, SmokingFactor, date_first) %>%
  collect()

# M1: most recent tobacco status prior to date_first
most_recent_tobacco <- tobacco_hf %>%
  filter(as.Date(HealthFactorDateTime) < as.Date(date_first)) %>%
  group_by(PatientICN) %>%
  arrange(desc(HealthFactorDateTime)) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(PatientICN, tobacco_status = SmokingFactor,
         tobacco_date = HealthFactorDateTime,
         tobacco_type = HealthFactorType) %>%
  mutate(tobacco_status = if_else(tobacco_status == "unknown", "UNKNOWN", tobacco_status))

# M2: most recent tobacco status prior to index_date
most_recent_tobacco_m2 <- tobacco_hf %>%
  inner_join(cohort_m2 %>% select(PatientICN, index_date), by = "PatientICN") %>%
  filter(as.Date(HealthFactorDateTime) < as.Date(index_date)) %>%
  group_by(PatientICN) %>%
  arrange(desc(HealthFactorDateTime)) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(PatientICN, tobacco_status = SmokingFactor,
         tobacco_date = HealthFactorDateTime,
         tobacco_type = HealthFactorType)  %>%
  mutate(tobacco_status = if_else(tobacco_status == "unknown", "UNKNOWN", tobacco_status))

cat("Patients with tobacco use status:", nrow(most_recent_tobacco), "\n")
cat("\nTobacco Status Distribution:\n")
print(table(most_recent_tobacco$tobacco_status))

# Save tobacco data
write_parquet(tobacco_hf, 'data/tobacco_health_factors_all.parquet')
write_parquet(most_recent_tobacco, 'data/tobacco_most_recent.parquet')

# ---------------------------------------------------------------------------
# 3) Military Sexual Trauma (MST) - Ever
# ---------------------------------------------------------------------------

# Extract MST indicator from PatSub_MilitarySexualTrauma table
# Priority order: Y, N, Declined, Unknown
mst_data <- tbl(cdwwork, in_schema('PatSub', 'MilitarySexualTrauma')) %>%
  inner_join(cw, by = "PatientSID", copy = TRUE) %>%
  select(PatientICN, PatientSID, Sta3n, MilitarySexualTraumaIndicator, 
         MSTChangeStatusDate) %>%
  collect()


# Create priority ranking for MST status
# Priority: "Yes" > "No" > "Declined" > "Unknown" > NULL
mst_summary <- mst_data %>%
  dplyr::mutate(mst_status = if_else(MilitarySexualTraumaIndicator == "Yes, Screened reports MST", 1, 0)
  ) %>%
  dplyr::group_by(PatientICN) %>%
  dplyr::summarize(mst_status = max(mst_status, na.rm = T))

cat("Patients with MST data:", nrow(mst_summary), "\n")
cat("\nMST Status Distribution:\n")
print(table(mst_summary$mst_status))

# Save MST data
write_parquet(mst_data, 'data/mst_all_records.parquet')
write_parquet(mst_summary, 'data/mst_ever.parquet')

# ---------------------------------------------------------------------------
# 4) Create final combined feature dataset
# ---------------------------------------------------------------------------

# Combine tobacco (M1) and MST features with cohort (M1 file)
final_health_factors <- cohort %>%
  select(PatientICN, date_first) %>%
  left_join(most_recent_tobacco, by = "PatientICN") %>%
  left_join(mst_summary, by = "PatientICN")

# Save M1 combined features
write_parquet(final_health_factors, 'data/final_health_factors_tobacco_mst.parquet')

# M2 combined features: tobacco anchored to index_date; MST is lifetime (unchanged)
final_health_factors_m2 <- cohort_m2 %>%
  select(PatientICN) %>%
  left_join(most_recent_tobacco_m2, by = "PatientICN") %>%
  left_join(mst_summary, by = "PatientICN")

write_parquet(final_health_factors_m2, 'data/final_health_factors_tobacco_mst_m2.parquet')
cat("Saved final_health_factors_tobacco_mst_m2.parquet:", nrow(final_health_factors_m2), "rows\n")

final_health_factors <- open_dataset('data/final_health_factors_tobacco_mst.parquet') %>% collect()

