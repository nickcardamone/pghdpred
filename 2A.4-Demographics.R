### Nick Cardamone
### 2A. Socio-demographics Gender, Race, Ethnicity (OMOP CDW), Marital Status (SPatient) and Rurality (SPatient Address), VA Enrollment Group. 
### OCC_PGHDPred
### Date created: 4/28/2025
### Last updated: 10/23/2025

suppressPackageStartupMessages({
  library(DBI) # Working with data in databases
  library(dbplyr) # Working with data in databases
  library(dplyr)
  library(data.table)
  library(comorbidity) #processing elixhauser data
  library(matrixStats)
  library(stringr) # string var manipulation
  library(arrow) #parquet files
  library(tidyverse) # helper functions
  library(lubridate)
  library(janitor)
  library(readxl)
  library(openxlsx)
  library(future)
  library(tictoc)
  library(parquetize)
  library(table1)
})

'%!in%' <- function(x, y)
  ! ('%in%'(x, y))


con <- dbConnect(odbc::odbc(),
                 .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
                 timeout = 10)
# Connect to specific databases using ODBC
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

# ---------------------------------------------------------------------------
# 1) Load PGHD cohort: 
cohort = open_dataset('parquet\\pghd_final_full_visits_ids.parquet') %>% collect()
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 2) OMOP Concepts:
# ---------------------------------------------------------------------------
omop_concept <- tbl(cdwwork, in_schema('OMOPV5', 'CONCEPT')) %>%
  select(CONCEPT_ID, CONCEPT_NAME)

# ---------------------------------------------------------------------------
# 3) OMOP Person (CDW): Gender, Race, Ethnicity
# ---------------------------------------------------------------------------
omop_xw <- tbl(cdwwork, in_schema('OMOPV5Map', 'SPatient_PERSON')) %>%
  inner_join(cohort, by = "PatientICN", copy = TRUE) %>%
  select(PatientICN, PERSON_ID, date_first) %>% distinct()

omop_person_cdw <- tbl(cdwwork, in_schema('OMOPV5', 'PERSON')) %>%
  inner_join(omop_xw, by = "PERSON_ID") %>%
  left_join(
    omop_concept %>% transmute(CONCEPT_ID, Race = CONCEPT_NAME),
    by = c("RACE_CONCEPT_ID" = "CONCEPT_ID")
  ) %>%
  left_join(
    omop_concept %>% transmute(CONCEPT_ID, Gender = CONCEPT_NAME),
    by = c("GENDER_CONCEPT_ID" = "CONCEPT_ID")
  ) %>%
  left_join(
    omop_concept %>% transmute(CONCEPT_ID, Ethnicity = CONCEPT_NAME),
    by = c("ETHNICITY_CONCEPT_ID" = "CONCEPT_ID")
  ) %>%
  transmute(
    PERSON_ID,
    BirthDate = BIRTH_DATETIME,
    Race = if_else(Race == "No matching concept", NA_character_, Race),
    Gender = if_else(Gender == "No matching concept", NA_character_, Gender),
    Ethnicity = if_else(Ethnicity == "No matching concept", NA_character_, Ethnicity)
  ) %>%
  distinct()

# ---------------------------------------------------------------------------
# 4) Marital status:
# Map cohort to PatientSID for joining to appointment source
# ---------------------------------------------------------------------------

cohort_sid <- tbl(cdwwork, in_schema('SPatient', 'SPatient')) %>%
  inner_join(cohort, by = "PatientICN", copy = TRUE) %>%
  transmute(PatientICN, PatientSID, date_first, MaritalStatus_date = as.Date(PatientEnteredDateTime), MaritalStatus)

marital_spatient <- cohort_sid  %>% 
  mutate(MaritalStatus2 = if_else(MaritalStatus=="MARRIED", 1, 0),
         MaritalStatus2 = if_else(MaritalStatus %in% c("*Missing*", "UNKNOWN", "*Unknown at this time*", "SINGLE *DO NOT USE*"), NA, MaritalStatus2)) %>% 
  transmute(PatientICN,
            date_first,
            MaritalStatus_date,
            Married = MaritalStatus2,
            MaritalStatus = if_else(MaritalStatus == "WIDOW/WIDOWER", "WIDOWED", MaritalStatus),
            MaritalStatus = if_else(MaritalStatus == "SINGLE", "NEVER MARRIED", MaritalStatus)) %>%
  collect()

marital_spatient <- marital_spatient %>%
  mutate(days_from = ymd(MaritalStatus_date) - ymd(date_first))

marital_outpat_visit <- tbl(cdwwork, in_schema('Outpat', 'Visit')) %>% 
  transmute(PatientSID, 
            MaritalStatus_date = as.Date(VisitDateTime),
            PatientMaritalStatus) %>% 
  filter(PatientMaritalStatus %in% c('W', 'M', 'D', 'S', 'N')) %>%
  distinct() %>%
  inner_join(cohort_sid %>% select(PatientICN, PatientSID, date_first) %>% distinct(), "PatientSID", copy = T) %>% 
  transmute(PatientICN,
            date_first,
            MaritalStatus_date,
            Married = if_else(PatientMaritalStatus == 'M', 1, 0),
            MaritalStatus = if_else(PatientMaritalStatus == "W", "WIDOWED", PatientMaritalStatus),
            MaritalStatus = if_else(MaritalStatus == "M", "MARRIED", MaritalStatus),
            MaritalStatus = if_else(MaritalStatus == "N", "NEVER MARRIED", MaritalStatus),
            MaritalStatus = if_else(MaritalStatus == "S", "SEPARATED", MaritalStatus),
            MaritalStatus = if_else(MaritalStatus == "D", "DIVORCED", MaritalStatus)) %>% 
  collect() 

marital_outpat_visit <- marital_outpat_visit %>%
  mutate(days_from = ymd(MaritalStatus_date) - ymd(date_first))

# Join marital status info from SPatient and Outpat.Visit tables:
marital_status <- rbind.data.frame(marital_spatient, marital_outpat_visit) %>% 
  filter(MaritalStatus %in% c("MARRIED", "DIVORCED", "SEPARATED", "NEVER MARRIED", "WIDOWED")) %>%
  distinct()

# Most recent marital status prior to first upload date
marital_status_before <- marital_status %>%
  filter(days_from <= 0) %>%
  arrange(PatientICN, abs(days_from)) %>%
  group_by(PatientICN) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  transmute(PatientICN, marital_l = MaritalStatus)

# Marital status within 3 years
marital_status_any <- marital_status %>%
  arrange(PatientICN, abs(days_from)) %>%
  group_by(PatientICN) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  transmute(PatientICN, marital_a = MaritalStatus)

marital_status_final <- cohort %>%
  left_join(marital_status_before, by = "PatientICN") %>%
  left_join(marital_status_any, by = "PatientICN") %>% 
  transmute(PatientICN,
            MaritalStatus = coalesce(marital_l, marital_a),
            Married = if_else(MaritalStatus == "MARRIED", 1, 0)) %>%
  distinct()

# Check data: table1(~MaritalStatus+Married, data = marital_status_final)

# ---------------------------------------------------------------------------
# 5) Rurality from SPatient_SPatientAddress (GISURH)
#   Use cw_sid to map PatientSID -> PatientICN for cohort
#   Choose the address with the most recent known date prior to first upload date:
#   If none exists take  
#   Ordinal Number 13 is self-reported address (gold standard).
# ---------------------------------------------------------------------------
pat_add <- tbl(cdwwork, in_schema('SPatient', 'SPatientAddress')) %>%
  filter(OrdinalNumber == 13,
         !is.na(StreetAddress1),
         is.na(BadAddressIndicator)) %>%
  select(
    PatientSID,
    GISURH,
    AddressChangeDateTime,
    GISAddressUpdatedDate,
    GISPatientAddressLatitude
  )

pa_raw <- pat_add %>%
  inner_join(cohort_sid, by = 'PatientSID') %>%
  transmute(
    PatientICN,
    GISURH,
    LastKnownAddressDate = coalesce(AddressChangeDateTime, GISAddressUpdatedDate),
    hasLat = !is.na(GISPatientAddressLatitude),
    date_first
  ) %>%
  collect() %>%
  filter(!is.na(GISURH), GISURH != "", GISURH != " ")

# Latest address at time of data upload per PatientICN
pa_latest_before <- pa_raw %>%
  filter(LastKnownAddressDate <= date_first) %>% 
  arrange(PatientICN, desc(LastKnownAddressDate), desc(hasLat)) %>%
  group_by(PatientICN) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  transmute(PatientICN, GISURH_l = GISURH)

# Any address closest to time of data upload per PatientICN
pa_any <- pa_raw %>%
  mutate(days_from = ymd(as.Date(LastKnownAddressDate)) - ymd(date_first)) %>%
  arrange(PatientICN, abs(days_from), desc(hasLat)) %>%
  group_by(PatientICN) %>%
  slice_head(n = 1) %>%
  ungroup()  %>%
  transmute(PatientICN, GISURH_any = GISURH)

# Join a coalesce rurality indicators
rurality = cohort %>% select(PatientICN) %>%
  left_join(pa_latest_before, by = "PatientICN") %>%
  left_join(pa_any, by = "PatientICN") %>%
  transmute(PatientICN, GISURH = coalesce(GISURH_l, GISURH_any))

# Check data: table1(~GISURH, data = rurality):

# ---------------------------------------------------------------------------
# 6) VA Enrollment Group:
# ---------------------------------------------------------------------------
xw_icn_adr <- tbl(cdwwork, in_schema('Veteran', 'ADRPerson')) %>%
  inner_join(tbl(cdwwork, in_schema('Veteran', 'MVIPerson')), by = "MVIPersonSID") %>%
  filter(ICNStatusCode %in% c('P', 'T')) %>%
  select(PatientICN = ADRPersonICN, ADRPersonSID) %>%
  inner_join(cohort, by = "PatientICN", copy = T) %>%
  select(PatientICN, 
         date_first, one_years_prior_date, ADRPersonSID) %>%
  distinct()

ADREnrollmentStatus <- tbl(cdwwork, in_schema("NDim", "ADREnrollStatus"))
ADRPriorityGroup <- tbl(cdwwork, in_schema("NDim", "ADRPriorityGroup"))

pat_enrollment <- tbl(cdwwork, in_schema("ADR", "ADREnrollHistory")) %>% 
  select(ADRPersonSID, ADREnrollStatusSID, ADRPrioritySubGroupSID, ADRPriorityGroupSID, EnrollStartDate, EnrollEndDate, RecordModifiedDate, NextRecordModifiedDate) %>%
  left_join(ADREnrollmentStatus %>% select(ADREnrollStatusSID, EnrollStatusName, EnrollCategoryName), by = "ADREnrollStatusSID", copy=T) %>% 
  left_join(ADRPriorityGroup %>% select(ADRPriorityGroupSID, PriorityGroupCode, PriorityGroupName), by = "ADRPriorityGroupSID", copy=T) %>% 
  filter(EnrollStatusName == "Verified" & EnrollCategoryName == "Enrolled") %>%
  inner_join(xw_icn_adr, "ADRPersonSID") %>% 
  select(PatientICN, date_first, one_years_prior_date, EnrollStatusName, EnrollCategoryName, ADRPriorityGroupSID, ADRPrioritySubGroupSID, PriorityGroupCode, PriorityGroupName, EnrollStartDate, EnrollEndDate, RecordModifiedDate, NextRecordModifiedDate) %>%
  collect()

# Take enrollment status where, if there's an enrollment start date, end date, or record modified date before first upload date and the next record modified date is after date first.
# Group by person, take most recent record modified before date first. If none, then take most recent changed record after date_first. 
pat_enrollment_fin <- pat_enrollment %>%
  filter(case_when(
    !is.na(EnrollStartDate) ~ as.Date(EnrollStartDate),
    !is.na(EnrollEndDate) ~ as.Date(EnrollEndDate),
    TRUE ~ as.Date(RecordModifiedDate)
  ) < date_first,
  coalesce(as.Date(NextRecordModifiedDate), as.Date("2100-12-31")) >= date_first
  ) %>%
  group_by(PatientICN) %>%
  arrange(desc(RecordModifiedDate),
          desc(coalesce(as.Date(NextRecordModifiedDate), as.Date("2100-12-31")))) %>%
  mutate(MostRecentStatusChangeRecord = row_number()) %>%
  ungroup()


pat_enrollment_fin <- pat_enrollment_fin %>%
  filter(MostRecentStatusChangeRecord == 1) 
# ---------------------------------------------------------------------------
# 7) Date of death
# ---------------------------------------------------------------------------
# CDW-only Date of Death from OMOPV5.DEATH (no CMS)
omop_death_cdw <- tbl(cdwwork, in_schema('OMOPV5', 'DEATH')) %>%
  inner_join(omop_xw %>% select(PERSON_ID) %>% distinct(), by = 'PERSON_ID') %>%
  transmute(PERSON_ID, cdw_dod = DEATH_DATE) %>%
  distinct() %>%
  mutate(cdw_dod = dplyr::if_else(cdw_dod < as.Date('2020-01-01'), as.Date(NA), as.Date(cdw_dod)))

# ---------------------------------------------------------------------------
# 8.) Combine and save
# ---------------------------------------------------------------------------
demo_cohort <- cohort %>%
  left_join(omop_xw %>% select(PatientICN, PERSON_ID), by = "PatientICN", copy = T) %>%
  left_join(omop_person_cdw %>% select(PERSON_ID, Gender, Race, Ethnicity, BirthDate) %>% collect(), by = "PERSON_ID") %>%
  left_join(omop_death_cdw %>% select(PERSON_ID, cdw_dod) %>% collect(), by = "PERSON_ID") %>%
  left_join(marital_status_final, by = "PatientICN") %>% 
  left_join(rurality, by = "PatientICN") %>%
  left_join(pat_enrollment_fin %>% select(PatientICN, PriorityGroupName), by = "PatientICN")


# Clean up features and create age variable
cohort_full_demo <- demo_cohort %>% 
  transmute(
      PatientICN,
      PERSON_ID,
      dob = as.Date(BirthDate),
      dod = cdw_dod,
      Age = floor(as.numeric(as.Date(date_first) - as.Date(dob)) / 365.25), # Create age at first upload 
      Race,
      Gender,
      Ethnicity,
      MaritalStatus,
      Married,
      GISURH,
      PriorityGroup = PriorityGroupName
    )

# Save to parquet
cohort_full_demo %>% write_parquet(
  "parquet\\cohort_full_demo.parquet"
)


