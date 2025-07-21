library(tidyverse)
library(lubridate)
library(janitor)
library(ckanr)
library(DBI)
library(odbc)
library(openxlsx)
library(hablar)
library(ggplot2)
library(fastDummies)
library(keyring)

# Source functions script
source(paste(here::here(), "code", "arco", "arco_functions.R", sep = "/"))


# Set flag for use of synthetic data ----
## Synthetic data files have been created for submission to GCU which match
## the structure of the data extracted from SMR01, but do not contain any patient data.

## Set TRUE or FALSE
use_synthetic = TRUE


# Start and end dates ----
start_date <- as_date("2024-01-01")
end_date <- as_date("2024-03-31")

smr01_start_date <- start_date %m-% months(6)


# Define paths ----
output_path <- paste(here::here(), "data", "output/", sep = "/")
lookups_path <- paste(here::here(), "data", "lookups/", sep = "/")


# Read in lookup files ----
lookup_consultants <- read_csv(file.path(lookups_path, "consultants.csv"),
                               col_types = cols(GMC = col_character(),
                                                .default = col_guess())) |>
  clean_names()

## Convert the GMC vector to a data frame
gmc_df <- data.frame(GMC = lookup_consultants$gmc, stringsAsFactors = FALSE)

lookup_opcs_1_13 <- read_csv(file.path(lookups_path, "opcs_codes_v1.13.csv")) |>
  clean_names()

lookup_comorbidities <- read_csv(file.path(lookups_path, "lookup_comorbidities.csv")) |>
  clean_names()

lookup_exclusion_codes <- read_csv(file.path(lookups_path, "lookup_opcs_exclusion_codes.csv")) |>
  clean_names()

lookup_nnap_codes <- read_csv(file.path(lookups_path, "lookup_nnap_codes.csv")) |>
  clean_names()

charlson_index <- read_csv(file.path(lookups_path, "charlson_index.csv")) |>
  clean_names()


if (use_synthetic == TRUE){
  
  # Read in synthetic data files ----
  data_smr01_raw <- read_csv(file.path(paste(here::here(), "data", "synthetic_audit_data", "data_smr01_raw.csv", sep = "/")), col_types = "nncDcncDnccDcccccccccccDccDccDccDccncc?")
  data_deaths <- read_csv(file.path(paste(here::here(), "data", "synthetic_audit_data", "data_deaths.csv", sep = "/")), col_types = "nDc")

} else {
  
  # Connect to SMRA ----
  ## Unlock Keyring
  keyring::keyring_unlock(keyring = "DATABASE",
                          password = source("~/database_keyring.R")[["value"]])
  
  ## Create database connection to SMRA using Keyring
  SMRAConnection <- dbConnect(odbc(),
                              dsn = "SMRA",
                              uid = Sys.info()[["user"]], # Assumes the user's SMRA username is the same as their R server username
                              pwd = keyring::key_get("SMRA", Sys.info()[["user"]], keyring = "DATABASE"))
  
  
  # Identify relevant stays ----
  ## Create a temporary table for GMC numbers
  ## Check if the table already exists and if so, remove it
  if (dbExistsTable(SMRAConnection, "TEMP_GMC_NUMBERS")) {
    dbRemoveTable(SMRAConnection, "TEMP_GMC_NUMBERS")
  }
  
  dbWriteTable(SMRAConnection, "TEMP_GMC_NUMBERS", gmc_df, overwrite = TRUE)
  
  ## Construct the Inner Join SQL query
  sql_query <- paste0("
      SELECT p.LINK_NO, p.CIS_MARKER
      FROM SMR01_PI p
      INNER JOIN ", Sys.info()[["user"]], ".TEMP_GMC_NUMBERS g
      ON LTRIM(p.CLINICIAN_MAIN_OPERATION) = g.GMC
      OR LTRIM(p.CLINICIAN_OTHER_OPERATION_1) = g.GMC
      OR LTRIM(p.CLINICIAN_OTHER_OPERATION_2) = g.GMC
      OR LTRIM(p.CLINICIAN_OTHER_OPERATION_3) = g.GMC
      OR LTRIM(p.CONSULTANT_HCP_RESPONSIBLE) = g.GMC
      ORDER BY p.LINK_NO, p.CIS_MARKER")
  
  ## Retrieve Relevant Stay Identifiers
  data_smr01_relevant_stays <- dbGetQuery(SMRAConnection, sql_query) |>
    unique()
  
  ## Remove the temporary table
  dbRemoveTable(SMRAConnection, "TEMP_GMC_NUMBERS")
  
  
  # Extract Relevant Stays ----
  
  ## Create temporary table for the relevant stays
  ## Check if the table already exists and if so, remove it
  if (dbExistsTable(SMRAConnection, "NEURO_SMR01_STAYS")) {
    dbRemoveTable(SMRAConnection, "NEURO_SMR01_STAYS")
  }
  
  dbWriteTable(SMRAConnection, "NEURO_SMR01_STAYS", data_smr01_relevant_stays, overwrite = TRUE)
  
  ## Construct the relevant stays SQL query
  sql_query <-
    paste("
    SELECT SMR.URI,
           SMR.LINK_NO,
           SMR.UPI_NUMBER,
           SMR.DOB,
           SMR.SEX,
           SMR.CIS_MARKER,
           SMR.ADMISSION,
           SMR.ADMISSION_DATE,
           SMR.ADMISSION_TYPE,
           SMR.ADMISSION_TRANSFER_FROM,
           SMR.DISCHARGE,
           SMR.DISCHARGE_DATE,
           SMR.DISCHARGE_TYPE,
           SMR.DISCHARGE_TRANSFER_TO,
           SMR.SPECIALTY,
           LTRIM(SMR.CONSULTANT_HCP_RESPONSIBLE) AS CONSULTANT_HCP_RESPONSIBLE,
           SMR.MAIN_CONDITION,
           SMR.OTHER_CONDITION_1,
           SMR.OTHER_CONDITION_2,
           SMR.OTHER_CONDITION_3,
           SMR.OTHER_CONDITION_4,
           SMR.OTHER_CONDITION_5,
           SUBSTR(SMR.MAIN_OPERATION, 1, 4) AS MAIN_OPERATION,
           SMR.DATE_OF_MAIN_OPERATION,
           LTRIM(SMR.CLINICIAN_MAIN_OPERATION) AS CLINICIAN_MAIN_OPERATION,
           SUBSTR(SMR.OTHER_OPERATION_1, 1, 4) AS OTHER_OPERATION_1,
           SMR.DATE_OF_OTHER_OPERATION_1,
           LTRIM(SMR.CLINICIAN_OTHER_OPERATION_1) AS CLINICIAN_OTHER_OPERATION_1,
           SUBSTR(SMR.OTHER_OPERATION_2, 1, 4) AS OTHER_OPERATION_2,
           SMR.DATE_OF_OTHER_OPERATION_2,
           LTRIM(SMR.CLINICIAN_OTHER_OPERATION_2) AS CLINICIAN_OTHER_OPERATION_2,
           SUBSTR(SMR.OTHER_OPERATION_3, 1, 4) AS OTHER_OPERATION_3,
           SMR.DATE_OF_OTHER_OPERATION_3,
           LTRIM(SMR.CLINICIAN_OTHER_OPERATION_3) AS CLINICIAN_OTHER_OPERATION_3,
           SMR.LOCATION,
           SMR.SIGNIFICANT_FACILITY
    FROM ANALYSIS.SMR01_PI SMR
    INNER JOIN ", Sys.info()[["user"]], ".NEURO_SMR01_STAYS TEMP
    ON SMR.LINK_NO = TEMP.LINK_NO
    AND SMR.CIS_MARKER = TEMP.CIS_MARKER
    WHERE SMR.ADMISSION_DATE >= TO_DATE('", format(smr01_start_date, "%Y-%m-%d"), "', 'YYYY-MM-DD')",
          sep = "")
  
  ## Retrieve the data from SMR01 using the constructed query
  data_smr01_raw <- dbGetQuery(SMRAConnection, sql_query) |>
    arrange(LINK_NO,
            CIS_MARKER,
            ADMISSION_DATE,
            DISCHARGE_DATE,
            ADMISSION,
            DISCHARGE,
            URI) |>
    clean_names() |>
    group_by(link_no, cis_marker) |>
    mutate(cis_index = row_number()) |>
    ungroup() |>
    ### Create the description columns for admission and discharge based on codes
    mutate(admission_type_description = case_when(admission_type %in% c(10, 11, 12, 18, 19) ~ "Routine Admission",
                                                  admission_type %in% c(20, 21, 22, 30, 31, 32, 33, 34, 35, 36, 38, 39) ~ "Urgent or Emergency Admission")) |>
    mutate(discharge_type_description = case_when(discharge_type %in% c(10, 11, 12, 13, 18,19) ~ "Regular Discharge",
                                                  discharge_type %in% c(20, 21, 22, 23, 28, 29) ~ "Irregular Discharge",
                                                  discharge_type %in% c(40, 41, 42, 43) ~ "Death")) |>
    mutate(in_hospital_death = case_when(discharge_type %in% c(40, 41, 42, 43) ~ T,
                                         T ~ F))
  
  ## Remove the temporary table
  dbRemoveTable(SMRAConnection, "NEURO_SMR01_STAYS")
  
  
  # Extract Deaths ----
  data_deaths <- dbGetQuery(SMRAConnection,
                            paste("
                            SELECT LINK_NO,
                                   DATE_OF_DEATH,
                                   UNDERLYING_CAUSE_OF_DEATH
                            FROM ANALYSIS.GRO_DEATHS_C
                            WHERE DATE_OF_DEATH >= TO_DATE('", format(smr01_start_date, "%Y-%m-%d"), "', 'YYYY-MM-DD')",
                                  sep = "")) |>
    clean_names() |>
    ### In some instances a death will be duplicated - take the first one
    arrange(link_no, date_of_death) |>
    group_by(link_no) |>
    slice(1) |>
    ungroup()
  
  
  # Disconnect from SMRA ----
  dbDisconnect(SMRAConnection)
}

# Create 'spine' df ---
## One row per LINK_NO, per stay, per GMC.
## Only include GMCs on our list, and take first observation of each GMC with each stay
## Cases with a Consultant Neurosurgeon in the HCP Responsible field but not the
## operating field will have those cases assigned to HCP Responsible. 
data_spine <- data_smr01_raw |>
  select(link_no,
         cis_marker,
         clinician_main_operation,
         clinician_other_operation_1,
         clinician_other_operation_2,
         clinician_other_operation_3,
         consultant_hcp_responsible) |>
  pivot_longer(cols = c(clinician_main_operation, clinician_other_operation_1, clinician_other_operation_2, clinician_other_operation_3, consultant_hcp_responsible),
               names_to = "clinician_type",
               values_to = "clinician_code") |>
  filter(!is.na(clinician_code)) |>
  filter(clinician_code %in% gmc_df$GMC) |>
  select(-clinician_type) |>
  unique() |>
  left_join(lookup_consultants |>
              select(gmc, consultant_name, consultant_hb, consultant_hb_name),
            by = c("clinician_code" = "gmc"))


# Create 'first episode of stay' df ----
data_first_episde_of_stay <- data_smr01_raw |>
  select(link_no,
         upi_number,
         cis_marker,
         cis_index,
         uri,
         admission_date,
         admission_type,
         admission_type_description,
         admission_transfer_from,
         location,
         specialty,
         main_condition,
         significant_facility,
         consultant_hcp_responsible,
         sex,
         dob) |>
  filter(cis_index == 1)

# Create 'last episode of stay' df ----
data_last_episde_of_stay <- data_smr01_raw |>
  select(link_no,
         upi_number,
         cis_marker,
         cis_index,
         uri,
         discharge_date,
         discharge_type,
         location,
         specialty,
         main_condition,
         significant_facility,
         consultant_hcp_responsible,
         in_hospital_death,
         discharge_transfer_to) |>
  group_by(link_no, cis_marker) |>
  filter(cis_index == max(cis_index))


# Create operation df ----
data_operations <- data_smr01_raw |>
  select(link_no, cis_marker,
         main_operation, other_operation_1, other_operation_2, other_operation_3,
         clinician_main_operation, clinician_other_operation_1, clinician_other_operation_2, clinician_other_operation_3,
         date_of_main_operation, date_of_other_operation_1, date_of_other_operation_2, date_of_other_operation_3,
         consultant_hcp_responsible, uri) |>
  
  ### Combine data about each operation to pivot the data together
  unite("main_operation", main_operation, clinician_main_operation, date_of_main_operation, consultant_hcp_responsible, uri, sep = " | ", remove = F) |>
  unite("other_operation_1", other_operation_1, clinician_other_operation_1, date_of_other_operation_1, consultant_hcp_responsible, uri, sep = " | ", remove = F) |>
  unite("other_operation_2", other_operation_2, clinician_other_operation_2, date_of_other_operation_2, consultant_hcp_responsible, uri, sep = " | ", remove = F) |>
  unite("other_operation_3", other_operation_3, clinician_other_operation_3, date_of_other_operation_3, consultant_hcp_responsible, uri, sep = " | ", remove = F) |>
  select(link_no, cis_marker, main_operation, other_operation_1, other_operation_2, other_operation_3) |>
  pivot_longer(cols = c(main_operation, other_operation_1, other_operation_2, other_operation_3),
               names_to = "operation_type",
               values_to = "operation_details") |>
  select(-operation_type) |>
  
  ### Split operation data back out
  separate(operation_details, into = c("operation_code", "operation_clinician", "operation_date", "consultant_hcp_responsible", "uri"),
           sep = " \\| ",
           remove = T) |>
  mutate(across(c(operation_code, operation_clinician, operation_date, consultant_hcp_responsible, uri),
                ~ ifelse(. == "NA", NA, .))) |>
  
  ### Remove rows where no operation occurred
  filter(!is.na(operation_code)) |>
  
  ### Remove operations where no GMC is recorded against an operation
  filter(!is.na(operation_clinician)) |>
  
  ### Remove operations where neither operation_clinician nor consultant_hcp_responsible
  ### is a consultant neurosurgeon
  filter(operation_clinician %in% gmc_df$GMC | consultant_hcp_responsible %in% gmc_df$GMC) |>
  group_by(link_no, cis_marker) |>
  mutate(operation_sequence = row_number()) |>
  ungroup() |>
  
  ### Add on deaths
  left_join(data_deaths |>
              select(-underlying_cause_of_death),
            by = "link_no") |>
  mutate(days_from_operation_to_death = difftime(as_date(date_of_death),
                                                 as_date(operation_date),
                                                 units = "days")) |>
  mutate(death30 = case_when(days_from_operation_to_death <= 30 ~ T,
                             T ~ F)) |>
  select(-days_from_operation_to_death) |>
  mutate(neuro_operation = case_when(operation_code %in% lookup_opcs_1_13$code ~ T,
                                     T ~ F))

## Create operations neuro lookup df ----
data_operations_neuro_lookup <- data_operations |>
  select(link_no, cis_marker, operation_date, neuro_operation) |>
  filter(neuro_operation == T) |>
  group_by(link_no, cis_marker) |>
  mutate(last_neuro_operation = max(operation_date)) |>
  slice(1) |>
  ungroup()

## Pivot data_operations df wider by splitting out the combined operations columns
data_operations <- data_operations |>
  pivot_wider(names_from = operation_sequence,
              values_from = c("operation_code", "neuro_operation", "operation_clinician", "operation_date", "death30", "consultant_hcp_responsible", "uri"),
              values_fill = list(operation_code = NA, clinician_operation_code = NA, operation_date = NA, consultant_hcp_responsible = NA, uri = NA))


# Arrange the operation variables ----
## Fixed columns that should always be at the start
fixed_cols <- c("link_no", "cis_marker")

## Extract all other columns and group them by their numeric suffix
other_cols <- setdiff(names(data_operations), fixed_cols)
suffixes <- gsub(".*_(\\d+)$", "\\1", other_cols)
unique_suffixes <- unique(suffixes)

ordered_other_cols <- unlist(lapply(unique_suffixes, function(suffix) {
  cols_with_suffix <- other_cols[grep(paste0("_", suffix, "$"), other_cols)]
  return(sort(cols_with_suffix))  # Sort them to ensure consistent internal ordering
}))

## Combine fixed and ordered columns
new_order <- c(fixed_cols, ordered_other_cols)

## Reorder the dataframe columns
data_operations <- data_operations[, new_order]

## Create columns for deaths within 30 days, procedure occurred, and neuro procedure occurred.
data_operations <- data_operations |>
  rowwise() |>
  mutate(any_death30 = any(c_across(starts_with("death30")) == TRUE, na.rm = TRUE)) |>
  mutate(any_procedure_occurred = any(!is.na(c_across(starts_with("operation_code"))))) |>
  mutate(neuro_procedure_occurred = any(c_across(starts_with("operation_code")) %in% lookup_opcs_1_13$code)) |>
  ungroup() |>
  left_join(data_operations_neuro_lookup |>
              select(link_no, cis_marker, last_neuro_operation), 
            by = c("link_no", "cis_marker")) 


names <- data.frame(names(data_operations))


# Create 'first neuro episode' df ----
## Identify each GMC's first interaction with each stay. 
## It's possible for more than one episode in a stay to be a "first neuro episode" 
## if more than one GMC is associated with the stay.
data_first_neuro_episode_in_stay_cis_index <- data_smr01_raw |>
  mutate(neuro_episode = case_when(consultant_hcp_responsible %in% gmc_df$GMC ~ T,
                                   clinician_main_operation %in% gmc_df$GMC ~ T,
                                   clinician_other_operation_1 %in% gmc_df$GMC ~ T,
                                   clinician_other_operation_2 %in% gmc_df$GMC ~ T,
                                   clinician_other_operation_3 %in% gmc_df$GMC ~ T,
                                   T ~ F)) |>
  filter(neuro_episode == T) |>
  select(link_no,
         cis_marker,
         cis_index,
         consultant_hcp_responsible,
         clinician_main_operation,
         clinician_other_operation_1,
         clinician_other_operation_2,
         clinician_other_operation_3) |>
  pivot_longer(cols = c(starts_with("clinician"), "consultant_hcp_responsible"),
               names_to = "clinician_role",
               values_to = "gmc_number") |>
  filter(!is.na(gmc_number)) |>
  group_by(link_no, cis_marker, gmc_number) |>
  summarise(earliest_cis_index = min(cis_index, na.rm = TRUE)) |>
  ungroup()


data_first_neuro_episode_in_stay <- data_first_neuro_episode_in_stay_cis_index |>
  left_join(data_smr01_raw |> select(link_no,
                                      cis_marker,
                                      cis_index,
                                      uri,
                                      consultant_hcp_responsible,
                                      admission_date,
                                      admission_type,
                                      admission_transfer_from,
                                      discharge_date,
                                      discharge_type,
                                      discharge_transfer_to,
                                      specialty,
                                      main_condition,
                                      other_condition_1,
                                      other_condition_2,
                                      other_condition_3,
                                      other_condition_4,
                                      other_condition_5,
                                      location,
                                      significant_facility),
            by = c("link_no", "cis_marker", c("earliest_cis_index" = "cis_index")))


# Create NRS Deaths df ----
data_nrs_deaths <- data_deaths |>
  mutate(record = T) |> 
  select(record, everything())


# Create the final dataframe ----
## Combine the episodes, operations, and deaths dataframes and filter to only
## those that lie within the start and end dates
final_dataframe <- data_spine |>
  left_join(data_first_episde_of_stay |> 
              rename_with(~ paste("first_episode", ., sep = "_"), -c(link_no, cis_marker)),
            by = c("link_no", "cis_marker")) |>
  left_join(data_last_episde_of_stay |> 
              rename_with(~ paste("last_episode", ., sep = "_"), -c(link_no, cis_marker)), 
            by = c("link_no", "cis_marker")) |>
  left_join(data_first_neuro_episode_in_stay |> 
              rename_with(~ paste("first_neuro_episode", ., sep = "_"), -c(link_no, cis_marker, gmc_number)), 
            by = c("link_no", "cis_marker", "clinician_code" = "gmc_number")) |>
  left_join(data_operations |> 
              rename_with(~ paste("operations", ., sep = "_"), -c(link_no, cis_marker)), 
            by = c("link_no", "cis_marker")) |>
  left_join(data_nrs_deaths |>
              rename_with(~ paste("nrs_linked_death", ., sep = "_"), -c(link_no)),
            by = "link_no") |>
  mutate(across(where(is.POSIXct), as.Date)) |>
  filter(first_episode_admission_date %within% interval(start_date, end_date)) |>
  arrange(first_episode_admission_date)


# Create Excel Workbooks ----
unique_clinicians <- unique(final_dataframe$clinician_code)

## Define styles to be applied to different column types
styles <- list(
  first_episode = createStyle(fgFill = "#FFDDDD"),
  last_episode = createStyle(fgFill = "#DDFFDD"),
  first_neuro_episode = createStyle(fgFill = "#DDDDFF"),
  nrs = createStyle(fgFill = "#FFA500"),
  operations = createStyle(fgFill = "#FFFFDD")
)

## Define columns for auditors
auditor_columns <- tibble(
  audit_this_neurosurgeon_operated_this_spell = NA_character_,
  audit_this_neurosurgeon_last_operation_date = NA_character_,
  audit_other_neurosurgeon_operation_this_spell_1 = NA_character_,
  audit_other_neurosurgeon_last_operation_date_1 = NA_character_,
  audit_other_neurosurgeon_operation_this_spell_2 = NA_character_,
  audit_other_neurosurgeon_last_operation_date_2 = NA_character_,
  audit_date_of_death_correct = NA_character_,
  audit_amended_date_of_death = NA_character_,
  audit_first_neuro_episode_main_condition_correct = NA_character_,
  audit_amended_main_condition = NA_character_,
  audit_first_episode_admission_type_correct = NA_character_,
  audit_amended_admission_type = NA_character_,
  audit_comorbidity_ACUTE_MYOCARDIAL_INFARCTION = NA_character_,
  audit_comorbidity_CEREBRAL_VASCULAR_ACCIDENT = NA_character_,
  audit_comorbidity_CONGESTIVE_HEART_FAILURE = NA_character_,
  audit_comorbidity_CONNECTIVE_TISSUE_DISORDER = NA_character_,
  audit_comorbidity_DEMENTIA = NA_character_,
  audit_comorbidity_DIABETES = NA_character_,
  audit_comorbidity_LIVER_DISEASE = NA_character_,
  audit_comorbidity_PEPTIC_ULCER = NA_character_,
  audit_comorbidity_PERIPHERAL_VASCULAR_DISEASE = NA_character_,
  audit_comorbidity_PULMONARY_DISEASE = NA_character_,
  audit_comorbidity_CANCER = NA_character_,
  audit_comorbidity_DIABETES_COMPLICATIONS = NA_character_,
  audit_comorbidity_PARAPLEGIA = NA_character_,
  audit_comorbidity_RENAL_DISEASE = NA_character_,
  audit_comorbidity_METASTATIC_CANCER = NA_character_,
  audit_comorbidity_SEVERE_LIVER_DISEASE = NA_character_,
  audit_comorbidity_HIV = NA_character_
)

options(openxlsx.dateFormat = "yyyy-mm-dd")


## Create a directory to save the Excel files if it doesn't exist
if (!dir.exists(output_path)) {
  dir.create(output_path)
}


## This function receives 'final_dataframe' and a clinician code and creates a 
## workbook for the clinician from the 'arco_template.xlsx' file. Style and validation
## functions are applied to the relevant columns and the workbook is saved to 
## the output directory.
create_clinician_output <- function(clinician, df) {
  clinician_data <- df |> filter(clinician_code == clinician)
  
  file_name = construct_file_name(clinician_data, clinician)
  
  workbook <- loadWorkbook(file = paste0(lookups_path, "arco_template.xlsx"))
  
  # Create new sheets in the workbook
  addWorksheet(workbook, "Yes No Dropdown", visible = FALSE)  # Add a sheet for drop-down values
  addWorksheet(workbook, "Consultant Dropdown", visible = FALSE)  # Add a sheet for drop-down values
  
  # Add the drop-down values to the new sheet
  writeData(workbook, "Yes No Dropdown", 
            x = data.frame(yes_no = c("Yes", "No")),
            startCol = 1, 
            startRow = 1)
  
  # Write the GMC-consultant name values to the new hidden sheet
  writeData(workbook, "Consultant Dropdown", 
            x = data.frame(consultants = paste(lookup_consultants$gmc, "-", lookup_consultants$consultant_name)), 
            startCol = 1, 
            startRow = 1)
  
  # Remove empty columns
  clinician_data <- clinician_data |>
    select(where(~ !all(is.na(.))))
  
  # Add auditor columns
  clinician_data <- bind_cols(clinician_data, auditor_columns)
  
  # Write the dataframe to the workbook
  writeData(workbook, "Data", clinician_data)
  
  
  # Apply appropriate styles to columns of workbook
  # 'Map' iterates over the styles object and the names of each entry, applying the 'apply_style' function
  # 'MoreArgs' contains objects which passed to the 'apply_style' function
  Map(f=apply_style, styles, names(styles), MoreArgs = list(workbook, clinician_data))
  
  
  # Add validation formatting to 'Yes/No' audit columns
  binary_validations <- c("audit_this_neurosurgeon_operated_this_spell",
                          "audit_date_of_death_correct",
                          "audit_first_neuro_episode_main_condition_correct",
                          "audit_first_episode_admission_type_correct",
                          "audit_comorbidity_ACUTE_MYOCARDIAL_INFARCTION",
                          "audit_comorbidity_CEREBRAL_VASCULAR_ACCIDENT",
                          "audit_comorbidity_CONGESTIVE_HEART_FAILURE",
                          "audit_comorbidity_CONNECTIVE_TISSUE_DISORDER",
                          "audit_comorbidity_DEMENTIA",
                          "audit_comorbidity_DIABETES",
                          "audit_comorbidity_LIVER_DISEASE",
                          "audit_comorbidity_PEPTIC_ULCER",
                          "audit_comorbidity_PERIPHERAL_VASCULAR_DISEASE",
                          "audit_comorbidity_PULMONARY_DISEASE",
                          "audit_comorbidity_CANCER",
                          "audit_comorbidity_DIABETES_COMPLICATIONS",
                          "audit_comorbidity_PARAPLEGIA",
                          "audit_comorbidity_RENAL_DISEASE",
                          "audit_comorbidity_METASTATIC_CANCER",
                          "audit_comorbidity_SEVERE_LIVER_DISEASE",
                          "audit_comorbidity_HIV")
  
  lapply(binary_validations, apply_binary_validation, df = clinician_data, workbook = workbook)


  # Add validation formatting to date audit columns
  date_validations   <- c("audit_this_neurosurgeon_last_operation_date",
                          "audit_other_neurosurgeon_last_operation_date_1",
                          "audit_other_neurosurgeon_last_operation_date_2",
                          "audit_amended_date_of_death")

  lapply(date_validations, apply_date_validation, df = clinician_data, workbook = workbook)


  # Add validation formatting to the neurosurgeon operated columns
  gmc_validations <- c("audit_other_neurosurgeon_operation_this_spell_1",
                       "audit_other_neurosurgeon_operation_this_spell_2")

  lapply(gmc_validations, apply_neurosurgeon_validation, df = clinician_data, workbook = workbook)

  
  # Automatically adjust column widths to fit content
  setColWidths(workbook, "Data", cols = 1:ncol(clinician_data), widths = "auto")
  
  # Save the workbook
  saveWorkbook(workbook, file_name, overwrite = TRUE)
}

## Walk over 'unique_clinicians' list applying 'create_clinician_output' function
purrr::walk(unique_clinicians, create_clinician_output, df = final_dataframe, .progress = TRUE)


# Write out the main dataframe to rds ----
write_rds(final_dataframe, paste0(output_path, "arco_data.rds"))


# Prepare variables for regression ----
## Read in lookup files for diagnosis groupings and comorbidities
lookup_diagnosis_groupings <- read_csv(file.path(lookups_path, "lookup_diagnosis_grouping.csv"), show_col_types = FALSE) |> 
  clean_names()

lookup_comorbidities <- read_csv(file.path(lookups_path, "lookup_comorbidities.csv"), show_col_types = FALSE) |>
  clean_names()


## Format operations_last_neuro_operation
model_prep_data <- final_dataframe |> 
  mutate(operations_last_neuro_operation = if_else(is.infinite(as.Date(operations_last_neuro_operation)),
                                                   NA_Date_,
                                                   as.Date(operations_last_neuro_operation)))


## Derive age_on_admission and age_group
model_prep_data <- model_prep_data |>
  mutate(age_on_admission = floor(as.numeric(interval(first_episode_dob, first_episode_admission_date), "years")),
         
         age_group = case_when(is.na(age_on_admission) ~ NA_character_,
                               age_on_admission >= 0 & age_on_admission <= 16 ~ "Children (0-16)",
                               age_on_admission >= 17 & age_on_admission <= 64 ~ "Adults (17-64)",
                               age_on_admission >= 65 ~ "Older Adults (65+)",
                               TRUE ~ NA_character_))


## Derive diagnosis_grouping
model_prep_data <- model_prep_data |>
  mutate(main_condition_temp = toupper(substr(last_episode_main_condition, 1, 3))) |> 
  left_join(lookup_diagnosis_groupings, by = c("main_condition_temp" = "icd10")) |>
  mutate(diagnosis_grouping = if_else(is.na(diagnosis_grouping),
                                      "other",
                                      diagnosis_grouping)) |>
  select(-main_condition_temp)


## Derive comorbidity_score and comorbidity_category
comorbidity_scores <- data.frame(comorbidity_category = c("Comorbidity1",
                                                          "Comorbidity2",
                                                          "Comorbidity3",
                                                          "Comorbidity4",
                                                          "Comorbidity5",
                                                          "Comorbidity6",
                                                          "Comorbidity7",
                                                          "Comorbidity8",
                                                          "Comorbidity9",
                                                          "Comorbidity10",
                                                          "Comorbidity11",
                                                          "Comorbidity12",
                                                          "Comorbidity13",
                                                          "Comorbidity14",
                                                          "Comorbidity15",
                                                          "Comorbidity16",
                                                          "Comorbidity17"),
                                 comorbidity_score_value = c(5,11,13,4,14,3,8,9,6,4,8,-1,1,10,14,18,2))


comorbidity_mapping <- lookup_comorbidities |>
  left_join(comorbidity_scores, by = "comorbidity_category") |>
  select(icd10, comorbidity_score_value) |> 
  distinct() |> 
  filter(!is.na(icd10) & !is.na(comorbidity_score_value)) |> 
  deframe()


model_prep_data <- model_prep_data |>
  rowwise() |> 
  mutate(comorbidity_score = sum(get_comorbidity_scores(last_episode_main_condition),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_1),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_2),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_3),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_4),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_5),
                                 na.rm = TRUE)) |>
  ungroup() |> 
  get_comorbidity_categories()


## Derive death_within_30_days_of_procedure
model_prep_data <- model_prep_data |>
  mutate(days_to_death = as.numeric(nrs_linked_death_date_of_death - operations_last_neuro_operation),
         death_within_30_days_of_procedure = case_when(is.na(days_to_death) ~ FALSE,
                                                       days_to_death >= 0 & days_to_death <= 30 ~ TRUE,
                                                       TRUE ~ FALSE)) |>
  select(-days_to_death)


# Prepare model_input_data_preaudit ----
model_input_data_preaudit <- model_prep_data |>
  filter(operations_neuro_procedure_occurred == TRUE) |> 
  filter(!is.na(first_episode_sex) & first_episode_sex %in% c("1", "2")) |> 
  group_by(link_no, consultant_name) |> 
  summarise(op_date = if_else(all(is.na(operations_last_neuro_operation)), NA_Date_, max(operations_last_neuro_operation, na.rm = TRUE)),
            age_group = first(na.omit(age_group)),
            sex = first(na.omit(first_episode_sex)),
            type_of_admission_first_episode = first(na.omit(as.character(first_episode_admission_type_description))),
            diagnosis_grouping = first(na.omit(as.character(diagnosis_grouping))),
            comorbidity_category = if_else(all(is.na(comorbidity_category)),
                                           NA_real_,
                                           suppressWarnings(max(comorbidity_category,na.rm = TRUE))),
            death_within_30_days_of_procedure = any(death_within_30_days_of_procedure == TRUE, na.rm = FALSE),
            .groups = "drop") |>
  mutate(age_group = factor(age_group, 
                            levels = c("Adults (17-64)", "Children (0-16)", "Older Adults (65+)")),
         sex = factor(sex,
                      levels = c("1", "2")),
         type_of_admission_first_episode = factor(type_of_admission_first_episode,
                                                  levels = c("Routine Admission", "Urgent or Emergency Admission", "Unknown")),
         diagnosis_grouping = factor(diagnosis_grouping),
         comorbidity_category = factor(comorbidity_category,
                                       levels = c("1", "2", "3", "4")),
         death_within_30_days_of_procedure = as.logical(death_within_30_days_of_procedure))


## Check each factor column has the correct entries and relevel
model_input_data_preaudit <- model_input_data_preaudit |> 
  check_and_relevel("age_group", "Adults (17-64)") |> 
  check_and_relevel("sex", "1")


### Check 'other' is correctly set as a factor level if it appears in diagnosis_grouping
current_diag_levels <- levels(model_input_data_preaudit$diagnosis_grouping)

if (!("other" %in% current_diag_levels) && any(model_input_data_preaudit$diagnosis_grouping == "other", na.rm=TRUE)) { 
  levels(model_input_data_preaudit$diagnosis_grouping) <- c(current_diag_levels, "other")
}
rm(current_diag_levels)

model_input_data_preaudit <- check_and_relevel(model_input_data_preaudit, "diagnosis_grouping", "other")


### Check levels 1, 2, 3, and 4 are correctly set as factor levels in comorbidity_category
current_comorb_levels <- levels(model_input_data_preaudit$comorbidity_category)
expected_comorb_levels <- c("1", "2", "3", "4")
missing_comorb_levels <- setdiff(expected_comorb_levels, current_comorb_levels)

if(length(missing_comorb_levels) > 0) {
  levels(model_input_data_preaudit$comorbidity_category) <- c(current_comorb_levels, missing_comorb_levels)
}
rm(current_comorb_levels, expected_comorb_levels, missing_comorb_levels)

model_input_data_preaudit <- check_and_relevel(model_input_data_preaudit, "comorbidity_category", "1")


### Check "Routine Admission" is correctly set as a factor level in type_of_admission_first_episode
current_admission_levels <- levels(model_input_data_preaudit$type_of_admission_first_episode)

if (!("Routine Admission" %in% current_admission_levels) && any(model_input_data_preaudit$type_of_admission_first_episode == "Routine Admission", na.rm=TRUE)) { 
  levels(model_input_data_preaudit$type_of_admission_first_episode) <- c(current_admission_levels, "Routine Admission")
}
rm(current_admission_levels)


model_input_data_preaudit <- check_and_relevel(model_input_data_preaudit, "type_of_admission_first_episode", "Routine Admission")

