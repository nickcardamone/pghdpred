### Nick Cardamone
### OCC_PGHDPred
### 2E.2 Health factors - Tobacco use and Military Sexual Trauma
### Date created: 10/22/2025
### Last updated: 10/24/2025

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

setwd("C:\\Users\\VHAPHICardaN\\OneDrive - Department of Veterans Affairs\\Desktop\\Projects\\OPS_Bressman-PGHDPred\\")

# Cohort: contains PatientICN and upload window variables (e.g. date_first)
cohort <- open_dataset('parquet\\pghd_final_full_visits_ids.parquet') %>%
  collect()

# Load crosswalk for PatientICN -> PatientSID mapping
cw <- open_dataset("parquet/cw_sid.parquet") %>% 
  collect() %>% 
  inner_join(cohort, by = "PatientICN") %>% 
  select(PatientICN, PatientSID, date_first, five_years_prior_date) %>% 
  distinct()

# ---------------------------------------------------------------------------
# 2) Tobacco Use Status - Most Recent (Prior to upload date)
# ---------------------------------------------------------------------------

# Load smoking lookup table with groupings
Groupings <- readxl::read_xlsx("04_Data_Connection/Health_Factor_Smoking_Lookup_table.xlsx") %>% 
  transmute(HealthFactorType = HEALTHFACTORTYPE, SmokingFactor)

# Get HealthFactorType dimension table
hf_dim <- tbl(cdwwork, in_schema('Dim', 'HealthFactorType')) %>% 
  select(HealthFactorTypeSID, HealthFactorType)

# Extract all smoking health factors within two years prior to upload date
tobacco_hf <- tbl(cdwwork, in_schema('HF', 'HealthFactor')) %>%
  inner_join(hf_dim, by = "HealthFactorTypeSID") %>% 
  inner_join(cw, by = "PatientSID", copy = TRUE) %>% 
  filter(HealthFactorDateTime >= five_years_prior_date & HealthFactorDateTime <= date_first) %>%
  distinct() %>% 
  inner_join(Groupings, by = "HealthFactorType", copy = TRUE) %>% 
  select(PatientICN, PatientSID, Sta3n, HealthFactorTypeSID, HealthFactorType, 
         HealthFactorDateTime, SmokingFactor) %>% 
  collect()

# Get most recent tobacco use status per patient (prior to upload date)
most_recent_tobacco <- tobacco_hf %>%
  group_by(PatientICN) %>%
  arrange(desc(HealthFactorDateTime)) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(PatientICN, tobacco_status = SmokingFactor, 
         tobacco_date = HealthFactorDateTime, 
         tobacco_type = HealthFactorType)

cat("Patients with tobacco use status:", nrow(most_recent_tobacco), "\n")
cat("\nTobacco Status Distribution:\n")
print(table(most_recent_tobacco$tobacco_status))

# Save tobacco data
write_parquet(tobacco_hf, 'parquet/tobacco_health_factors_all.parquet')
write_parquet(most_recent_tobacco, 'parquet/tobacco_most_recent.parquet')

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
write_parquet(mst_data, 'parquet/mst_all_records.parquet')
write_parquet(mst_summary, 'parquet/mst_ever.parquet')

# ---------------------------------------------------------------------------
# 4) Create final combined feature dataset
# ---------------------------------------------------------------------------

# Combine tobacco and MST features with cohort
final_health_factors <- cohort %>%
  select(PatientICN, date_first) %>%
  left_join(most_recent_tobacco, by = "PatientICN") %>%
  left_join(mst_summary, by = "PatientICN")

# Save combined features
write_parquet(final_health_factors, 'parquet/final_health_factors_tobacco_mst.parquet')

