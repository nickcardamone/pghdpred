### Nick Cardamone
### OCC_PGHDPred
### 2C. Medical conditions and comorobidities
### Date created: 4/28/2025
### Last updated: 8/05/2025

# Features of interest for modeling (all within 2 years prior to index unless noted):

# 1. Acute Renal Failure (binary) 
# 2. Artificial Openings for Feeding or Elimination (binary)
# 3. Chronic Pancreatitis (binary) 
# 4. Chronic Ulcer of Skin, Except Pressure (binary) 
# 5. Congestive Heart Failure (binary)
# 6. Drug/Alcohol Dependence (binary) - 
# 7. Drug/Alcohol Psychosis (binary) 
# 8. Dialysis Status (binary) 
# 9. Lung and Other Severe Cancers (binary)
# 10. Metastatic Cancer (binary)
# 11. Multimorbidity-Weighted Index (continuous, spline or mean)
# 12. Pregnancy (categorical)
# 13. Specified Heart Arrhythmias (binary)

# SDOH: Financial, Housing Instability, Non-specific psychosocial.

# All are derived from OMOP/ICD/CPT codes or comorbidity indices.

suppressPackageStartupMessages({
  library(DBI) # Working with data in databases
  library(dbplyr) # Working with data in databases
  library(dplyr) # data manipulation
  library(data.table) # fast big data processing
  library(comorbidity) #processing elixhauser data
  library(matrixStats)
  library(stringr) # string var manipulation
  library(arrow) #parquet files 
  library(parquetize) # working with parquet files
  library(tidyverse) # helper functions
  library(lubridate) # work with dates
  library(janitor) # fast tablign for big data
  library(future) # parallelization
  library(tictoc) # timing 
  library(table1) # descriptive tables
  library(odbc)  # Added explicit odbc
})

# Connect to DB using ODBC with RB03 profile
con <- dbConnect(odbc::odbc(), 
                 .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                 timeout = 10)

# Connect to specific databases using ODBC
cdwwork <- dbConnect(odbc::odbc(), 
                     .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                     timeout = 10,
                     database = "CDWWork")

db_pghpred <- dbConnect(odbc::odbc(), 
                        .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                        timeout = 10,
                        database = "OCC_PGHDPred")

# ---------------------------------------------------------------------------
# 1) Read cohort and map IDs (consistent with lab extraction pattern)
# ---------------------------------------------------------------------------

# Set working directory
setwd("C:\\Users\\VHAPHICardaN\\OneDrive - Department of Veterans Affairs\\Desktop\\Projects\\OPS_Bressman-PGHDPred\\")

# Cohort: contains PatientICN and upload window variables (e.g. date_first)
cohort <- open_dataset('parquet\\pghd_final_full_visits_ids.parquet') %>%
  collect()

# Map PatientICN -> PERSON_ID for OMOP joins
omop_xw <- tbl(cdwwork, in_schema('OMOPV5Map', 'SPatient_PERSON')) %>% 
  inner_join(cohort, by = "PatientICN", copy = TRUE) %>% 
  select(PatientICN, PERSON_ID, date_first, two_years_prior_date) %>%
  distinct()

# ---------------------------------------------------------------------------
# 2) Extract all ICD10 diagnosis and procedure codes for each person in prior 2 years
# ---------------------------------------------------------------------------
# Load all visits in 2 years prior to first upload date:
omop_visit <- tbl(cdwwork, in_schema('OMOPV5', 'VISIT_OCCURRENCE')) %>% 
  filter(VISIT_START_DATE >= '2019-01-01') %>% 
  select(VISIT_OCCURRENCE_ID, PERSON_ID, VISIT_START_DATE, VISIT_END_DATE, VISIT_CONCEPT_ID, VISIT_TYPE_CONCEPT_ID, VISIT_SOURCE_VALUE) %>% 
  inner_join(omop_xw, by = "PERSON_ID", copy = TRUE) %>% 
  distinct() %>% 
  filter(VISIT_START_DATE >= two_years_prior_date & VISIT_END_DATE <= date_first)

# ---------------------------------------------------------------------------
# 3) Define specific ICD-10 codes for medical conditions of interest
# ---------------------------------------------------------------------------

# Define specific ICD-10 codes for each condition based on provided table
condition_codes <- list(
  # Acute Renal Failure: ]
  # Source: best guess based on scrutinizing ICD10.
  acute_renal_failure = c("N17.0", "N17.1", "N17.2", "N17.8", "N17.9"),
  
  # Artificial Openings for Feeding or Elimination: 
  # Source: best guess based on scrutinizing ICD10.
  artificial_openings = c("Z93.1", "Z93.2", "Z93.3", "Z93.4",
                          # ICD-10-PCS: Gastrostomy, jejunostomy, colostomy, ileostomy procedures
                          "0DH60UZ", "0DH63UZ", "0DH64UZ", "0DH68UZ", # Gastrostomy tube insertion
                          "0D1L0Z4", "0D1L0ZA", "0D1L0ZB", # Jejunostomy
                          "0DTE0ZZ", "0DTE4ZZ", # Colostomy
                          "0DTB0ZZ", "0DTB4ZZ"), # Ileostomy
  
  # Chronic Pancreatitis
  # Source: Anite Nyguen: https://phenomics.va.ornl.gov/web/cipher/phenotype-viewer/details?uqid=d592f53acfcd4b408802a4772db102f1&name=Chronic_Pancreatitis__Nguyen_
  chronic_pancreatitis = c("K86.0", 
                           "K86.1"),
  
  # Chronic ulcer of skin, except pressure:
  # Source: MAP: https://cipherwiki.va.gov/phenotype/index.php?title=Chronic_ulcer_of_skin_(MAP)
  chronic_ulcer_skin = c("L97.1", 
                         "L97.2", 
                         "L97.3", 
                         "L97.4", 
                         "L97.5", 
                         "L97.8", 
                         "L97.9", 
                         "L98.4"),
  
  # Congestive Heart Failure:
  # Source: CART: https://phenomics.va.ornl.gov/web/cipher/phenotype-viewer/details?uqid=74fed85736694ad5a78f16d8db2504d0&name=Congestive_Heart_Failure__CART_
  # Additionally, ICD-10-PCS codes recommended by AI.
  congestive_heart_failure = c("I09.81", "I11.0", "I13.0", "I13.2", "I50.1", "I50.20", "I50.21", "I50.22", "I50.23", 
                               "I50.30", "I50.31", "I50.32", "I50.33", "I50.40", "I50.41", "I50.42", "I50.43", 
                               "I50.810", "I50.811", "I50.812", "I50.813", "I50.814", "I50.82", "I50.83", "I50.84", 
                               "I50.89", "I50.9", "I97.130", "I97.131",
                               # ICD-10-PCS: Heart transplant, ventricular assist device, cardiac resynchronization
                               "02YA0Z0", "02YA0Z1", "02YA0Z2", # Heart transplant
                               "02HA0QZ", "02HA0RZ", "02HA0SZ", # Ventricular assist device insertion
                               "0JH606Z", "0JH636Z", "0JH646Z"), # Cardiac resynchronization therapy
  
  # Drug/Alcohol Dependence:
  # Source: https://phenomics.va.ornl.gov/web/cipher/phenotype-viewer/details?uqid=41b49e62600e4e2e846d922704bbfa49&name=Substance_Abuse_Dependence__CART_
  drug_alcohol_dependence = c("F11.10", "F11.120", "F11.122", "F11.14", "F11.159", "F11.181", "F11.182", "F11.188", "F11.19",
                              "F11.20", "F11.21", "F11.220", "F11.221", "F11.222", "F11.229", "F11.23", "F11.24", "F11.250",
                              "F11.251", "F11.259", "F11.281", "F11.282", "F11.288", "F11.29", "F11.922", "F11.93", "F11.94",
                              "F11.959", "F11.981", "F11.982", "F11.988", "F11.99", "F12.10", "F12.120", "F12.122", "F12.159",
                              "F12.180", "F12.188", "F12.19", "F12.20", "F12.21", "F12.220", "F12.221", "F12.222", "F12.229",
                              "F12.250", "F12.251", "F12.259", "F12.280", "F12.288", "F12.29", "F12.922", "F12.959", "F12.980",
                              "F12.988", "F12.99", "F13.10", "F13.120", "F13.14", "F13.159", "F13.180", "F13.181", "F13.182",
                              "F13.188", "F13.19", "F13.20", "F13.21", "F13.220", "F13.221", "F13.229", "F13.230", "F13.231",
                              "F13.232", "F13.239", "F13.24", "F13.250", "F13.251", "F13.259", "F13.26", "F13.27", "F13.280",
                              "F13.281", "F13.282", "F13.288", "F13.29", "F13.930", "F13.931", "F13.932", "F13.939", "F13.94",
                              "F13.959", "F13.96", "F13.97", "F13.980", "F13.981", "F13.982", "F13.988", "F13.99", "F14.10",
                              "F14.120", "F14.122", "F14.14", "F14.159", "F14.180", "F14.181", "F14.182", "F14.188", "F14.19",
                              "F14.20", "F14.21", "F14.220", "F14.221", "F14.222", "F14.229", "F14.23", "F14.24", "F14.250",
                              "F14.251", "F14.259", "F14.280", "F14.281", "F14.282", "F14.288", "F14.29", "F14.922", "F14.94",
                              "F14.959", "F14.980", "F14.981", "F14.982", "F14.988", "F14.99", "F15.10", "F15.120", "F15.122",
                              "F15.14", "F15.159", "F15.180", "F15.181", "F15.182", "F15.188", "F15.19", "F15.20", "F15.21",
                              "F15.220", "F15.221", "F15.222", "F15.229", "F15.23", "F15.24", "F15.250", "F15.251", "F15.259",
                              "F15.280", "F15.281", "F15.282", "F15.288", "F15.29", "F15.922", "F15.93", "F15.94", "F15.959",
                              "F15.980", "F15.981", "F15.982", "F15.988", "F15.99", "F16.10", "F16.120", "F16.122", "F16.14",
                              "F16.159", "F16.180", "F16.183", "F16.188", "F16.19", "F16.20", "F16.21", "F16.220", "F16.221",
                              "F16.229", "F16.24", "F16.250", "F16.251", "F16.259", "F16.280", "F16.283", "F16.288", "F16.29",
                              "F16.90", "F16.94", "F16.959", "F16.980", "F16.983", "F16.988", "F16.99", "F18.10", "F18.120",
                              "F18.14", "F18.159", "F18.17", "F18.180", "F18.188", "F18.19", "F18.20", "F18.21", "F18.220",
                              "F18.221", "F18.229", "F18.24", "F18.250", "F18.251", "F18.259", "F18.27", "F18.280", "F18.288",
                              "F18.29", "F18.90", "F18.94", "F18.959", "F18.97", "F18.980", "F18.988", "F18.99", "F19.10",
                              "F19.120", "F19.122", "F19.14", "F19.159", "F19.16", "F19.17", "F19.180", "F19.181", "F19.182",
                              "F19.188", "F19.19", "F19.20", "F19.21", "F19.220", "F19.221", "F19.222", "F19.229", "F19.230",
                              "F19.231", "F19.232", "F19.239", "F19.24", "F19.250", "F19.251", "F19.259", "F19.26", "F19.27",
                              "F19.280", "F19.281", "F19.282", "F19.288", "F19.29", "F19.90", "F19.922", "F19.930", "F19.931",
                              "F19.932", "F19.939", "F19.94", "F19.959", "F19.96", "F19.97", "F19.980", "F19.981", "F19.982",
                              "F19.988", "F19.99", "O99.320", "O99.321", "O99.322", "O99.323", "O99.324", "O99.325"),
  
  # Drug/Alcohol Psychosis:
  # Source: best guess based on scrutinizing ICD10.
  drug_alcohol_psychosis = c("F20.0", "F20.1", "F20.2", "F20.3", "F20.5", "F20.81", "F20.89", "F20.9", 
                             "F22", "F24", "F25.0", "F25.1", "F25.8", "F25.9", "F28", "F29"),
  
  # Lung and Other Severe Cancers - placeholder codes:
  # Source: best guess based on scrutinizing ICD10.
  # Additionally, ICD-10-PCS codes recommended by AI.
  lung_severe_cancers = c("C78.0", "C78.1", "C78.2", "C34.0", "C34.1", "C34.2", "C34.3", "C34.8", "C34.9",
                          # ICD-10-PCS: Lung resection procedures
                          "0BT50ZZ", "0BT60ZZ", "0BT70ZZ", "0BT80ZZ", # Lung resection
                          "0BTC0ZZ", "0BTD0ZZ", "0BTF0ZZ", "0BTG0ZZ", # Lobectomy
                          "0BT30ZZ", "0BT40ZZ"), # Pneumonectomy
  
  # Metastatic Cancer:
  # Source: PERC https://phenomics.va.ornl.gov/web/cipher/phenotype-viewer/details?uqid=76e123312d0c48da818657ec5a0890cd&name=Metastatic_Cancer__PERC_
  metastatic_cancer = c("C77.0", "C77.1", "C77.2", "C77.3", "C77.4", "C77.5", "C77.8", "C77.9", 
                        "C78.00", "C78.01", "C78.02", "C78.1", "C78.2", "C78.30", "C78.39", "C78.4", "C78.5", 
                        "C78.6", "C78.7", "C78.80", "C78.89", "C79.00", "C79.01", "C79.02", "C79.10", "C79.11", 
                        "C79.19", "C79.2", "C79.31", "C79.32", "C79.40", "C79.49", "C79.51", "C79.52", "C79.60", 
                        "C79.61", "C79.62", "C79.63", "C79.70", "C79.71", "C79.72", "C79.81", "C79.82", "C79.89", 
                        "C79.9", "C7B.00", "C7B.01", "C7B.02", "C7B.03", "C7B.04", "C7B.09", "C7B.1", "C7B.8", "C80.0"),
  
  # Pregnancy:
  # Source: best guess based on scrutinizing ICD10.
  # Additionally, ICD-10-PCS codes recommended by AI.
  pregnancy = c("Z34.00", "Z34.01", "Z34.02", "Z34.03", "Z34.80", "Z34.81", "Z34.82", "Z34.83", 
                "Z34.90", "Z34.91", "Z34.92", "Z34.93",
                # ICD-10-PCS: Delivery procedures
                "10D00Z0", "10D00Z1", "10D00Z2", # Cesarean delivery
                "10E0XZZ", "10D07Z0", "10D07Z1"), # Vaginal delivery procedures
  
  # Specified Heart Arrhythmias:
  # Source: best guess based on scrutinizing ICD10.
  # Additionally, ICD-10-PCS codes recommended by AI.
  heart_arrhythmias = c("I47", "I48", "I49",
                        # ICD-10-PCS: Cardiac device insertions, ablation procedures
                        "02H60JZ", "02H63JZ", "02H64JZ", # Pacemaker insertion
                        "02H60KZ", "02H63KZ", "02H64KZ", # Defibrillator insertion
                        "02580ZZ", "02590ZZ", "025A0ZZ", # Cardiac ablation procedures
                        "02HN0JZ", "02HN3JZ", "02HN4JZ"), # Cardiac rhythm management device
  
  # SDOH: Financial:
  # Source: best guess based on scrutinizing ICD10.
  financial_sdoh = c("Z59.5", "Z59.6", "Z59.7", "Z59.86", "Z59.87", "Z59.9", "Z59.89"),
  
  # SDOH: Housing Instability:
  # Source: best guess based on scrutinizing ICD10.
  housing_instability = c("Z59.0", "Z59.1", "Z59.2", "Z59.3", "Z59.81", "Z59.812", "Z59.819"),
  
  # SDOH: Non-specific psychosocial:
  # Source: best guess based on scrutinizing ICD10.
  nonspecific_psychosocial = c("Z60.9", "Z62.9", "Z63.9", "Z65.9"),
  
  # Dialysis Access Procedures (ICD-10-PCS)
  # Source: ICD-10-PCS codes recommended by AI.
  dialysis_access = c(# ICD-10-PCS: Dialysis access procedures - arteriovenous fistula, graft creation
    "031C0JF", "031C0KF", "031C0ZF", # AV fistula creation - radial artery to cephalic vein
    "031D0JF", "031D0KF", "031D0ZF", # AV fistula creation - ulnar artery
    "031F0JF", "031F0KF", "031F0ZF", # AV fistula creation - brachial artery
    "02HV33Z", "02HV34Z", "02HV35Z") # Dialysis catheter insertion
)

# Define dialysis CPT codes:
# Source: best guess based on scrutinizing CPT codes.
dialysis_cpt_codes <- c('CPT|90925','CPT|90935','CPT|90937','CPT|90940',
                        'CPT|90945','CPT|90947',
                        'CPT|90957','CPT|90958','CPT|90959','CPT|90960','CPT|90961','CPT|90962','CPT|90965','CPT|90966',
                        'CPT|90969','CPT|90970','CPT|90977','CPT|90985','CPT|90988','CPT|90989'
                        ,'CPT|90993','CPT|90994','CPT|90997','CPT|90999')

#  ---------------------------------------------------------------------------
# 4) Extract diagnoses and procedures for prior period analysis
#  ---------------------------------------------------------------------------

# Descriptions of the procedure and diagnosis codes
omop_proc_concept <- tbl(cdwwork, in_schema('OMOPV5Dim', 'ICD10Procedure_CONCEPT')) %>% 
  transmute(CONCEPT_ID, ICD10ProcedureCode, source = "Procedure")

omop_dx_concept <- tbl(cdwwork, in_schema('OMOPV5Dim', 'ICD10_CONCEPT')) %>% 
  transmute(CONCEPT_ID, ICD10Code, source = "Diagnosis")

# All procedures per visit with ICD10 code in prior period
omop_proc_prior <- tbl(cdwwork, in_schema('OMOPV5', 'PROCEDURE_OCCURRENCE')) %>%
  select(VISIT_OCCURRENCE_ID, PROCEDURE_CONCEPT_ID) %>% 
  inner_join(omop_visit, by = "VISIT_OCCURRENCE_ID") %>% 
  inner_join(omop_proc_concept, by = c("PROCEDURE_CONCEPT_ID" = "CONCEPT_ID")) %>% 
  transmute(PatientICN, ICD10 = ICD10ProcedureCode) %>% 
  distinct() %>% 
  collect()

# All diagnoses per visit with ICD10 code in prior period
omop_dx_prior <- tbl(cdwwork, in_schema('OMOPV5', 'CONDITION_OCCURRENCE')) %>%
  select(VISIT_OCCURRENCE_ID, CONDITION_CONCEPT_ID) %>% 
  inner_join(omop_visit, by = "VISIT_OCCURRENCE_ID") %>% 
  inner_join(omop_dx_concept, by = c("CONDITION_CONCEPT_ID" = "CONCEPT_ID")) %>% 
  transmute(PatientICN, ICD10 = ICD10Code) %>% 
  distinct() %>% 
  collect()

# Combine all ICD10 codes (diagnosis + procedure) for prior period
omop_icd10_prior <- rbind(omop_proc_prior, omop_dx_prior) %>% 
  drop_na() %>%
  mutate(code_clean = toupper(gsub("\\.", "", ICD10))) %>%
  distinct(PatientICN, code_clean)

# ---------------------------------------------------------------------------
# 5) Extract specific medical conditions using defined ICD-10 codes:
# Extracted from Wei et al. 2024; https://pubmed.ncbi.nlm.nih.gov/38365301/
# ---------------------------------------------------------------------------

# Function to extract patients with specific ICD-10 codes
extract_condition_patients <- function(icd_codes, condition_name) {
  if (length(icd_codes) == 0) {
    cat("No codes defined for", condition_name, "\n")
    return(tibble(PatientICN = character(), !!condition_name := integer()))
  }
  
  # Remove dots and make uppercase for consistent matching
  clean_codes <- toupper(gsub("\\.", "", icd_codes))
  
  # Create pattern for exact matching or prefix matching
  pattern_codes <- paste0("^(", paste(clean_codes, collapse = "|"), ")")
  
  result <- omop_icd10_prior %>%
    filter(grepl(pattern_codes, code_clean)) %>%
    distinct(PatientICN) %>%
    mutate(!!condition_name := 1L)
  
  cat("Patients with", condition_name, ":", nrow(result), "\n")
  return(result)
}

# Extract each condition:
condition_results <- list()

condition_results$acute_renal_failure <- extract_condition_patients(
  condition_codes$acute_renal_failure, "py2_acute_renal_failure")

condition_results$artificial_openings <- extract_condition_patients(
  condition_codes$artificial_openings, "py2_artificial_openings")

condition_results$chronic_pancreatitis <- extract_condition_patients(
  condition_codes$chronic_pancreatitis, "py2_chronic_pancreatitis")

condition_results$chronic_ulcer_skin <- extract_condition_patients(
  condition_codes$chronic_ulcer_skin, "py2_chronic_ulcer_skin")

condition_results$congestive_heart_failure <- extract_condition_patients(
  condition_codes$congestive_heart_failure, "py2_congestive_heart_failure")

condition_results$drug_alcohol_dependence <- extract_condition_patients(
  condition_codes$drug_alcohol_dependence, "py2_drug_alcohol_dependence")

condition_results$drug_alcohol_psychosis <- extract_condition_patients(
  condition_codes$drug_alcohol_psychosis, "py2_drug_alcohol_psychosis")

condition_results$lung_severe_cancers <- extract_condition_patients(
  condition_codes$lung_severe_cancers, "py2_lung_severe_cancers")

condition_results$metastatic_cancer <- extract_condition_patients(
  condition_codes$metastatic_cancer, "py2_metastatic_cancer")

condition_results$pregnancy <- extract_condition_patients(
  condition_codes$pregnancy, "py2_pregnancy")

condition_results$heart_arrhythmias <- extract_condition_patients(
  condition_codes$heart_arrhythmias, "py2_heart_arrhythmias")

condition_results$financial_sdoh <- extract_condition_patients(
  condition_codes$financial_sdoh, "py2_financial_sdoh")

condition_results$housing_instability <- extract_condition_patients(
  condition_codes$housing_instability, "py2_housing_instability")

condition_results$nonspecific_psychosocial <- extract_condition_patients(
  condition_codes$nonspecific_psychosocial, "py2_nonspecific_psychosocial")

condition_results$dialysis_access <- extract_condition_patients(
  condition_codes$dialysis_access, "py2_dialysis_access")

#  ---------------------------------------------------------------------------
# 6) Extract dialysis status using CPT codes (prior period)
#  ---------------------------------------------------------------------------

# Extract dialysis status using procedure_source_value in prior period
dialysis_proc_prior <- tbl(cdwwork, in_schema('OMOPV5', 'PROCEDURE_OCCURRENCE')) %>% 
  select(PERSON_ID, PROCEDURE_DATE, PROCEDURE_SOURCE_VALUE) %>% 
  filter(!is.na(PROCEDURE_SOURCE_VALUE)) %>% 
  filter(PROCEDURE_SOURCE_VALUE %in% !!dialysis_cpt_codes) %>%
  inner_join(omop_xw %>% select(PatientICN, PERSON_ID, date_first, two_years_prior_date), by = 'PERSON_ID', copy = TRUE) %>% 
  filter(PROCEDURE_DATE >= two_years_prior_date & PROCEDURE_DATE < date_first) %>% 
  collect()

dialysis_proc_prior_icn <- dialysis_proc_prior %>% 
  distinct(PatientICN) %>% 
  mutate(py2_dialysis_status = 1L) 

# ---------------------------------------------------------------------------
# 7) Calculate Multimorbidity Weighted Index (MWI) using ICD-10 codes
# Custom implementation based on UCLA SAS macro
# ---------------------------------------------------------------------------

# Define MWI condition mapping with weights for ICD-10 codes
mwi_conditions <- list(
  list(id = 1, name = "Aneurysm, Dissection of chest area", weight = 1.96, 
       codes = c("A52.01", "I25.3", "I25.4", "I28.1", "I71", "Q25.43")),
  list(id = 2, name = "Angina", weight = 2.2, 
       codes = c("I20", "I23.7", "I25.11", "I25.7")),
  list(id = 3, name = "Arrhythmias (including atrial fibrillation, atrial flutter)", weight = 1.33, 
       codes = c("I44.1", "I44.2", "I44.3", "I45.6", "I45.9", "I47", "I48.0", "I48.1", "I48.2", "I48.3", "I48.4", "I48.91", "I48.92", "I49", "R00.0", "R00.1", "R00.8", "T82.1", "Z45.01", "Z45.09", "Z95.0")),
  list(id = 4, name = "Automated implantable cardioverter defibrillator (AICD)", weight = 1.57, 
       codes = c("Z45.02", "Z95.810")),
  list(id = 5, name = "Congestive heart failure, Cardiomyopathy", weight = 4.77, 
       codes = c("B33.24", "I09.81", "I09.9", "I11.0", "I13.0", "I13.2", "I25.5", "I42", "I43", "I50", "P29.0")),
  list(id = 6, name = "Coronary artery bypass graft surgery (CABG)", weight = 0.724, 
       codes = c("I25.70", "I25.71", "I25.72", "I25.73", "I25.76", "I25.79", "I25.810", "I25.812", "T82.21")),
  list(id = 7, name = "Coronary artery disease", weight = 1.73, 
       codes = c("I25")),
  list(id = 8, name = "High blood pressure", weight = 1.53, 
       codes = c("I10", "I11.0", "I11.9", "I12", "I13.0", "I13.1", "I13.2", "I15", "I16")),
  list(id = 9, name = "Mitral valve prolapse", weight = 0.033, 
       codes = c("I34.1")),
  list(id = 10, name = "Myocardial infarction", weight = 1.73, 
       codes = c("I21", "I22", "I23", "I25.2", "I25.6")),
  list(id = 11, name = "Peripheral artery disease, Atherosclerosis of extremities", weight = 3.25, 
       codes = c("I70", "I73.1", "I73.8", "I73.9", "I75.0", "I77.1", "I79.0", "K55.1", "K55.8", "K55.9", "Z95.811", "Z95.812", "Z95.818", "Z95.82", "Z95.9")),
  list(id = 12, name = "Valvular heart disease", weight = 0.416, 
       codes = c("A52.00", "A52.02", "A52.03", "A52.04", "A52.05", "A52.06", "A52.09", "I05", "I06", "I07", "I08", "I09.1", "I09.89", "I34.0", "I34.2", "I34.8", "I34.9", "I35", "I36", "I37", "I38", "I39", "Q22", "Q23", "Z95.2", "Z95.3", "Z95.4")),
  list(id = 13, name = "Diabetes mellitus", weight = 2.67, 
       codes = c("E08.0", "E08.1", "E08.2", "E08.3", "E08.4", "E08.5", "E08.6", "E08.8", "E08.9", "E09.0", "E09.1", "E09.2", "E09.3", "E09.4", "E09.5", "E09.6", "E09.8", "E09.9", "E10.35", "E10.36", "E10.37", "E10.39", "E10.4", "E10.5", "E10.6", "E10.8", "E10.9", "E11.0", "E11.1", "E11.2", "E11.3", "E11.4", "E11.5", "E11.6", "E11.8", "E11.9", "E13.0", "E13.1", "E13.2", "E13.3", "E13.4", "E13.5", "E13.6", "E13.8", "E13.9")),
  list(id = 14, name = "Elevated cholesterol, Hyperlipidemia", weight = 0.343, 
       codes = c("E78.0", "E78.2", "E78.3", "E78.4", "E78.5")),
  list(id = 15, name = "Hyperthyroidism", weight = 0.149, 
       codes = c("E05")),
  list(id = 16, name = "Hypertriglyceridemia", weight = 0.692, 
       codes = c("E78.1", "E78.3")),
  list(id = 17, name = "Hypothyroidism", weight = 0.808, 
       codes = c("E00", "E01.0", "E01.1", "E01.8", "E02", "E03", "E06.3", "E07.1", "E89.0")),
  list(id = 18, name = "Thyroid nodule, Goiter", weight = 0, 
       codes = c("E01.2", "E04", "E07.1")),
  list(id = 19, name = "Barrett's esophagus", weight = 0.284, 
       codes = c("K22.7")),
  list(id = 20, name = "Chronic hepatitis, Hepatocellular disease", weight = 0.293, 
       codes = c("B18", "E83.01", "E83.11", "K70.0", "K70.11", "K70.2", "K71.3", "K71.4", "K71.5", "K73", "K75.3", "K75.4", "K75.81", "K76.1", "K76.6", "K76.82", "K83.01")),
  list(id = 21, name = "Cirrhosis, Liver failure, Liver transplant", weight = 4.3, 
       codes = c("I85.0", "I85.1", "I86.4", "K70.3", "K71.1", "K71.7", "K72.1", "K72.91", "K74.0", "K74.1", "K74.2", "K74.3", "K74.4", "K74.5", "K74.6", "K76.7", "K76.81", "P78.81", "T86.4", "Z94.4")),
  list(id = 22, name = "Colon polyp", weight = 0.01, 
       codes = c("D12.0", "D12.2", "D12.3", "D12.4", "D12.5", "D12.6", "D12.7", "K51.4", "K63.5")),
  list(id = 23, name = "Diverticulosis, Diverticulitis", weight = 0.624, 
       codes = c("K57", "Q43.0")),
  list(id = 24, name = "Gallstones", weight = 0.929, 
       codes = c("K80", "K81", "K91.86")),
  list(id = 25, name = "Hepatitis not specified as chronic", weight = 0.147, 
       codes = c("A51.45", "B00.81", "B15", "B16", "B17", "B19", "B25.1", "B26.81", "B58.1", "B94.2", "K70.10", "K70.4", "K70.9", "K71.2", "K71.6", "K72.0", "K72.9", "K75.2", "K75.89", "K75.9", "K76.0", "K76.2", "K76.3", "K76.4", "K76.5", "K76.89", "K76.9")),
  list(id = 26, name = "Inflammatory bowel disease", weight = 1.07, 
       codes = c("K50", "K51", "K52")),
  list(id = 27, name = "Malnutrition", weight = 1.46, 
       codes = c("E40", "E41", "E42", "E43", "E44", "E45", "E46", "R63.4", "R64")),
  list(id = 28, name = "Pancreatitis", weight = 0.675, 
       codes = c("B25.2", "K86.0", "K86.1")),
  list(id = 29, name = "Ulcer, peptic", weight = 1.08, 
       codes = c("K22.1", "K25", "K26", "K27", "K28")),
  list(id = 30, name = "Anemia", weight = 1.82, 
       codes = c("D50.0", "D50.8", "D50.9", "D51", "D52", "D53")),
  list(id = 31, name = "Coagulopathy", weight = 1.46, 
       codes = c("D65", "D66", "D67", "D68", "D69.1", "D69.3", "D69.4", "D69.5", "D69.6")),
  list(id = 32, name = "Venous thromboembolism", weight = 1.98, 
       codes = c("I26", "I27.24", "I27.82", "I82.4", "I82.5", "I82.62", "I82.72", "I82.90")),
  list(id = 33, name = "AIDS", weight = 2.91, 
       codes = c("B20")),
  list(id = 34, name = "Solar actinic keratosis", weight = 0, 
       codes = c("L57.0")),
  list(id = 35, name = "Connective tissue disease", weight = 3.02, 
       codes = c("M31.5", "M32", "M33", "M34", "M35", "M36.0", "M36.8")),
  list(id = 36, name = "Gout", weight = 1.34, 
       codes = c("M10", "M1A")),
  list(id = 37, name = "Herniated disc", weight = 3.27, 
       codes = c("M50", "M51")),
  list(id = 38, name = "Hip fracture", weight = 3.56, 
       codes = c("M84.359", "M84.459", "M84.559", "M84.659", "M97.0", "S32.409A", "S32.409B", "S72.0", "S72.1", "S72.2", "S79.0", "S79.91")),
  list(id = 39, name = "Hip replacement surgery", weight = 3.55, 
       codes = c("T84.010", "T84.011", "T84.020", "T84.021", "T84.030", "T84.031", "T84.050", "T84.051", "T84.060", "T84.061", "T84.090", "T84.091", "T84.51", "T84.52", "Z47.32", "Z96.64")),
  list(id = 40, name = "Knee replacement surgery", weight = 9.11, 
       codes = c("M97.1", "T84.012", "T84.013", "T84.022", "T84.023", "T84.032", "T84.033", "T84.052", "T84.053", "T84.062", "T84.063", "T84.092", "T84.093", "Z47.33", "Z96.65")),
  list(id = 41, name = "Osteoarthritis", weight = 3.52, 
       codes = c("M15", "M16", "M17", "M18", "M19")),
  list(id = 42, name = "Osteoporosis", weight = 0.997, 
       codes = c("M80", "M81")),
  list(id = 43, name = "Rheumatoid arthritis", weight = 3.79, 
       codes = c("L94.0", "L94.1", "L94.3", "M05", "M06", "M08.0", "M08.2", "M08.3", "M08.4", "M08.8", "M08.9", "M12.0", "M12.3", "M30", "M31.0", "M31.3", "M45", "M46.1", "M46.8", "M46.9")),
  list(id = 44, name = "Vertebral fracture", weight = 2.07, 
       codes = c("M48.4", "M48.5", "M80.08", "M80.88", "S12.0", "S12.1", "S12.2", "S12.3", "S12.4", "S12.5", "S12.6", "S22.0", "S32.0", "S32.1", "S32.2")),
  list(id = 45, name = "Wrist fracture", weight = 0, 
       codes = c("S62.0", "S62.1")),
  list(id = 46, name = "ALS, Motor neuron disease", weight = 7.45, 
       codes = c("G12.21", "G12.22", "G12.29", "G12.8")),
  list(id = 47, name = "Cerebrovascular disease, Stroke", weight = 3.79, 
       codes = c("G45.0", "G45.1", "G45.2", "G45.3", "G45.4", "G46", "H34.0", "I60", "I61", "I62", "I63", "I65", "I66", "I67.0", "I67.1", "I67.2", "I67.3", "I67.4", "I67.5", "I67.6", "I67.7", "I67.82", "I67.83", "I67.84", "I67.85", "I67.89", "I67.9", "I68", "I69", "I97.81", "I97.82")),
  list(id = 48, name = "Dementia, Alzheimer disease", weight = 6.1, 
       codes = c("A81.0", "F01", "F02", "F03", "F10.27", "F10.97", "F13.27", "F13.97", "F18.17", "F18.27", "F18.97", "F19.17", "F19.27", "F19.97", "G30", "G31.0", "G31.83")),
  list(id = 49, name = "Migraine headache", weight = 0.614, 
       codes = c("G43")),
  list(id = 50, name = "Multiple sclerosis", weight = 10.6, 
       codes = c("G35")),
  list(id = 51, name = "Other neurologic disorders", weight = 3.79, 
       codes = c("G10", "G11", "G12.0", "G12.1", "G12.20", "G12.22", "G12.23", "G12.24", "G12.25", "G12.29", "G12.8", "G12.9", "G13", "G21", "G25.4", "G25.5", "G31.2", "G31.81", "G31.82", "G31.84", "G31.85", "G31.89", "G31.9", "G32", "G36", "G37", "G93.1", "G93.4", "R47.0", "R56")),
  list(id = 52, name = "Paralytic syndrome", weight = 8.135, 
       codes = c("G04.1", "G11.4", "G80.1", "G80.2", "G81", "G82", "G83")),
  list(id = 53, name = "Parkinson disease", weight = 8.82, 
       codes = c("G20")),
  list(id = 54, name = "Restless legs syndrome", weight = 2.23, 
       codes = c("G25.81")),
  list(id = 55, name = "Seizure disorder, Epilepsy", weight = 0.841, 
       codes = c("E88.42", "G40")),
  list(id = 56, name = "Transient ischemic attack", weight = 1.24, 
       codes = c("G45.8", "G45.9", "I67.81")),
  list(id = 57, name = "Basal cell carcinoma", weight = 0, 
       codes = c("C44.11", "C44.21", "C44.31", "C44.41", "C44.51", "C44.61", "C44.71", "C44.81", "C44.91")),
  list(id = 58, name = "Bladder cancer", weight = 0.99, 
       codes = c("C67", "C79.11")),
  list(id = 59, name = "Blood cancers", weight = 1.32, 
       codes = c("C81", "C82", "C83", "C84", "C85", "C86", "C88", "C90", "C91", "C92", "C93", "C94", "C95", "D47.Z9")),
  list(id = 60, name = "Breast cancer", weight = 0.886, 
       codes = c("C50", "C79.81")),
  list(id = 61, name = "Cervical cancer", weight = 0.723, 
       codes = c("C53")),
  list(id = 62, name = "Colorectal cancer", weight = 1.18, 
       codes = c("C18", "C19", "C20", "C78.5")),
  list(id = 63, name = "Lung cancer", weight = 6.25, 
       codes = c("C32", "C33", "C34", "C78.0")),
  list(id = 64, name = "Liver cancer", weight = 1.76, 
       codes = c("C22", "C78.7")),
  list(id = 65, name = "Melanoma", weight = 0, 
       codes = c("C43")),
  list(id = 66, name = "Other cancer", weight = 1.76, 
       codes = c("C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C21", "C23", "C24", "C25", "C26", "C30", "C31", "C37", "C38", "C39", "C40", "C41", "C45", "C46", "C47", "C48", "C49", "C51", "C52", "C57.7", "C57.8", "C57.9", "C58", "C60", "C62", "C63", "C64", "C65", "C66", "C68", "C69", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C77", "C78.1", "C78.2", "C78.3", "C78.4", "C78.6", "C78.8", "C79.0", "C79.10", "C79.19", "C79.2", "C79.3", "C79.4", "C79.5", "C79.7", "C79.82", "C79.89", "C79.9", "C7A", "C7B", "C80", "C96", "D47.0", "D47.1", "D47.2", "D47.3", "D47.4", "D47.Z", "D49")),
  list(id = 67, name = "Ovarian cancer", weight = 1.87, 
       codes = c("C56", "C57.0", "C79.6")),
  list(id = 68, name = "Prostate cancer", weight = 0.402, 
       codes = c("C61")),
  list(id = 69, name = "Squamous cell carcinoma", weight = 0, 
       codes = c("C44.12", "C44.22", "C44.32", "C44.42", "C44.52", "C44.62", "C44.72", "C44.82", "C44.92")),
  list(id = 70, name = "Uterine cancer", weight = 0.753, 
       codes = c("C54", "C55", "C57.1", "C57.2", "C57.3", "C57.4")),
  list(id = 71, name = "Cataract", weight = 0.288, 
       codes = c("E08.36", "E09.36", "E10.36", "E11.36", "E13.36", "H25", "H26", "H28", "Q12.0")),
  list(id = 72, name = "Glaucoma", weight = 0.427, 
       codes = c("B73.02", "H26.23", "H40", "H42", "H44.51", "H47.23", "Q15.0")),
  list(id = 73, name = "Macular degeneration", weight = 0.564, 
       codes = c("H35.3")),
  list(id = 74, name = "Periodontal disease", weight = 0.164, 
       codes = c("E08.630", "E09.630", "E10.630", "E11.630", "E13.630", "K04.5", "K05", "K08.12", "K08.42")),
  list(id = 75, name = "Anxiety", weight = 1.29, 
       codes = c("F40", "F41")),
  list(id = 76, name = "Depression and other psychiatric conditions", weight = 1.29, 
       codes = c("F20", "F22", "F23", "F24", "F25", "F28", "F29", "F30.2", "F31.2", "F31.3", "F31.4", "F31.5", "F32.0", "F32.1", "F32.2", "F32.3", "F32.4", "F32.5", "F32.89", "F32.9", "F32A", "F33", "F34.1", "F43.2")),
  list(id = 77, name = "Substance use disorder", weight = 1.37, 
       codes = c("E52", "F10.1", "F10.2", "F10.90", "F10.91", "F10.92", "F10.93", "F10.94", "F10.95", "F10.96", "F10.98", "F10.99", "F11", "F12", "F13.1", "F13.20", "F13.21", "F13.22", "F13.23", "F13.24", "F13.25", "F13.26", "F13.28", "F13.29", "F13.90", "F13.91", "F13.92", "F13.93", "F13.94", "F13.95", "F13.96", "F13.98", "F13.99", "F14", "F15", "F16", "F18.10", "F18.11", "F18.12", "F18.14", "F18.15", "F18.18", "F18.19", "F18.20", "F18.21", "F18.22", "F18.24", "F18.25", "F18.28", "F18.29", "F18.90", "F18.91", "F18.92", "F18.94", "F18.95", "F18.98", "F18.99", "F19.10", "F19.11", "F19.12", "F19.13", "F19.14", "F19.15", "F19.16", "F19.18", "F19.19", "F19.20", "F19.21", "F19.22", "F19.23", "F19.24", "F19.25", "F19.26", "F19.28", "F19.29", "F19.90", "F19.91", "F19.92", "F19.93", "F19.94", "F19.95", "F19.96", "F19.98", "F19.99", "G62.1", "K29.2", "T51", "Z71.4", "Z71.5")),
  list(id = 78, name = "Asthma", weight = 1.62, 
       codes = c("J45")),
  list(id = 79, name = "Chronic pulmonary diseases", weight = 4.32, 
       codes = c("J41", "J42", "J43", "J44", "J47", "J60", "J61", "J62", "J63", "J64", "J65", "J66", "J67", "J68.4", "J70.1", "J70.3")),
  list(id = 80, name = "Pulmonary vascular diseases", weight = 1.46, 
       codes = c("I27.0", "I27.1", "I27.20", "I27.21", "I27.22", "I27.23", "I27.29", "I27.81", "I27.83", "I27.89", "I27.9", "I28.0", "I28.8", "I28.9")),
  list(id = 81, name = "Calculus of kidney and ureter", weight = 0.291, 
       codes = c("N20", "N21", "N22")),
  list(id = 82, name = "Chronic kidney disease", weight = 3.98, 
       codes = c("D63.1", "E08.21", "E08.22", "E09.21", "E09.22", "E10.21", "E10.22", "E11.21", "E11.22", "E13.21", "E13.22", "I12", "I13.0", "I13.1", "I13.2", "K76.7", "N03", "N04", "N05.2", "N05.3", "N05.4", "N05.5", "N05.6", "N05.7", "N07", "N11", "N15.0", "N18", "N19", "N25.0", "Q27.1", "T86.1", "Z49.0", "Z94.0", "Z99.2")),
  list(id = 83, name = "Fluid and electrolyte disorders", weight = 1.46, 
       codes = c("E22.2", "E86", "E87")),
  list(id = 84, name = "Interstitial cystitis", weight = 0.879, 
       codes = c("N30.1")),
  list(id = 85, name = "Benign breast disease", weight = 0, 
       codes = c("D24", "N60", "N61", "N62", "N63", "N64")),
  list(id = 86, name = "Benign prostatic hyperplasia", weight = 0, 
       codes = c("N40")),
  list(id = 87, name = "Ectopic and molar pregnancy", weight = 0, 
       codes = c("O00", "O01", "O08")),
  list(id = 88, name = "Endometriosis", weight = 0.142, 
       codes = c("N80")),
  list(id = 89, name = "Erectile dysfunction", weight = 1.22, 
       codes = c("F52.21", "N52")),
  list(id = 90, name = "Dysmenorrhea", weight = 0.142, 
       codes = c("N94.4", "N94.5", "N94.6")),
  list(id = 91, name = "Polycystic ovary syndrome", weight = 0.64, 
       codes = c("E28.2")),
  list(id = 92, name = "Premenstrual syndrome", weight = 0.412, 
       codes = c("F32.81", "N94.0", "N94.3")),
  list(id = 94, name = "Uterine fibroids", weight = 0.029, 
       codes = c("D25", "D26"))
)

# Reversible conditions (conditions 18, 27, 29, 31, 71, 83, 84) - use 1-year lookback
reversible_conditions <- c(18, 27, 29, 31, 71, 83, 84)

# Pre-process MWI condition codes for faster matching

# Create a lookup table for all MWI codes with their weights
mwi_code_lookup <- tibble()
for (condition in mwi_conditions) {
  clean_codes <- toupper(gsub("\\.", "", condition$codes))
  condition_df <- tibble(
    code_clean = clean_codes,
    condition_id = condition$id,
    weight = condition$weight,
    name = condition$name
  )
  mwi_code_lookup <- bind_rows(mwi_code_lookup, condition_df)
}

# Create prefix patterns for faster matching
mwi_patterns <- unique(mwi_code_lookup$code_clean)
cat("Created lookup table with", nrow(mwi_code_lookup), "MWI code mappings\n")

# Filter omop_icd10_prior to only include codes that might match MWI conditions
cat("Filtering ICD-10 codes to relevant MWI codes only...\n")
omop_icd10_mwi_relevant <- omop_icd10_prior %>%
  filter(
    # Use efficient string matching - check if any code starts with our patterns
    str_detect(code_clean, paste0("^(", paste(mwi_patterns, collapse = "|"), ")"))
  )

# Calculate MWI scores for each patient using vectorized approach
if (nrow(omop_icd10_mwi_relevant) > 0) {
  
  # Create expanded lookup for prefix matching
  mwi_expanded_lookup <- tibble()
  
  # For each MWI pattern, find all matching codes in our data
  for (pattern in mwi_patterns) {
    matching_codes <- omop_icd10_mwi_relevant %>%
      filter(str_starts(code_clean, pattern)) %>%
      pull(code_clean) %>%
      unique()
    
    if (length(matching_codes) > 0) {
      # Get the condition info for this pattern
      pattern_info <- mwi_code_lookup %>% filter(code_clean == pattern) %>% slice(1)
      
      pattern_df <- tibble(
        code_clean = matching_codes,
        condition_id = pattern_info$condition_id,
        weight = pattern_info$weight,
        name = pattern_info$name
      )
      mwi_expanded_lookup <- bind_rows(mwi_expanded_lookup, pattern_df)
    }
  }
  
  # Join patient codes with MWI conditions
  patient_condition_matches <- omop_icd10_mwi_relevant %>%
    inner_join(mwi_expanded_lookup, by = "code_clean") %>%
    distinct(PatientICN, condition_id, weight)
  
  # Calculate MWI scores by patient using group_by and summarize
  patient_mwi_scores <- patient_condition_matches %>%
    group_by(PatientICN) %>%
    summarise(py2_mwi_score = sum(weight), .groups = "drop")
  
  # Ensure all patients in cohort are included (even those with 0 MWI)
  all_patients <- tibble(PatientICN = unique(omop_icd10_prior$PatientICN))
  mwi_scores <- all_patients %>%
    left_join(patient_mwi_scores, by = "PatientICN") %>%
    mutate(py2_mwi_score = replace_na(py_mwi_score, 0))
  
  summary(mwi_scores$py2_mwi_score)
  
} else {
  # If No relevant ICD-10 data available for MWI calculation
  mwi_scores <- tibble(PatientICN = character(), py2_mwi_score = numeric())
}

#  ---------------------------------------------------------------------------
# 8) Combine all condition flags for cohort
#  ---------------------------------------------------------------------------

# Start with cohort base
patient_condition_flags <- cohort %>% 
  select(PatientICN)

# Join all condition results
for(i in seq_along(condition_results)) {
  patient_condition_flags <- patient_condition_flags %>%
    left_join(condition_results[[i]], by = "PatientICN")
}

# Add dialysis status
patient_condition_flags <- patient_condition_flags %>%
  left_join(dialysis_proc_prior_icn, by = "PatientICN")

# Add MWI scores
patient_condition_flags <- patient_condition_flags %>%
  left_join(mwi_scores, by = "PatientICN")

# Replace NAs with 0 for all condition flags (but keep NA for MWI scores where appropriate)
patient_condition_flags <- patient_condition_flags %>%
  mutate(across(starts_with("py2_") & !ends_with("_score"), ~replace_na(.x, 0L))) %>%
  mutate(py2_mwi_score = replace_na(py2_mwi_score, 0)) %>%
  group_by(PatientICN) %>%
  mutate(py2_dialysis_status = max(py2_dialysis_status, py2_dialysis_access)) %>%
  select(-py_dialysis_access) %>%
  ungroup()

# ---------------------------------------------------------------------------
# 9. Save results and create summary
# ---------------------------------------------------------------------------

# Save to parquet
patient_condition_flags %>% write_parquet("parquet\\patient_condition_flags_prior.parquet")

# Option 4: Base R approach
names(patient_condition_flags) <- gsub("^py_", "py2_", names(patient_condition_flags))

# Create summary statistics
condition_summary <- patient_condition_flags %>%
  select(-PatientICN) %>%
  summarise(across(starts_with("py_") & !ends_with("_score"), sum, na.rm = TRUE),
            across(ends_with("_score"), list(mean = ~mean(.x, na.rm = TRUE),
                                             median = ~median(.x, na.rm = TRUE),
                                             min = ~min(.x, na.rm = TRUE),
                                             max = ~max(.x, na.rm = TRUE))))

# Summary of Prior Medical Conditions:
condition_summary

# Additional MWI-specific summary
if("py_mwi_score" %in% names(patient_condition_flags)) {
  cat("\nMWI Score Distribution:\n")
  mwi_summary <- patient_condition_flags %>%
    filter(py2_mwi_score > 0) %>%
    summarise(
      n_patients_with_mwi = n(),
      mwi_mean = mean(py_mwi_score, na.rm = TRUE),
      mwi_median = median(py_mwi_score, na.rm = TRUE),
      mwi_sd = sd(py_mwi_score, na.rm = TRUE),
      mwi_min = min(py_mwi_score, na.rm = TRUE),
      mwi_max = max(py_mwi_score, na.rm = TRUE)
    )
  print(mwi_summary)
}

# Descriptive table of conditions flag in prior 2 years
table1(~py_acute_renal_failure + py_artificial_openings + py_chronic_pancreatitis + 
         py_chronic_ulcer_skin + py_congestive_heart_failure + py_drug_alcohol_dependence + 
         py_drug_alcohol_psychosis + py_lung_severe_cancers + py_metastatic_cancer + 
         py_pregnancy + py_heart_arrhythmias + py_dialysis_status, data = patient_condition_flags %>% mutate(across(where(is.numeric), as.factor)))

# prior 2 year MWI score
table1(~py_mwi_score, data = patient_condition_flags)

hist(patient_condition_flags$py2_mwi_score)

# ---------------------------------------------------------------------------
# Note: Multimorbidity-Weighted Index (MWI) has been calculated using ICD-10 codes
# The MWI score is included as py_mwi_score in the output dataset
# Higher scores indicate greater multimorbidity burden
# ---------------------------------------------------------------------------







