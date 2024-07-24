import csv 
import os

def gen_csv_file(filePath, fileName, fieldNames):
    """
    Inputs:
    1) file name, string
    2) field names, given as a list of strings

    Output: None; function creates a csv file with the given file name and with the given field names
    """
    with open(f"{filePath}/{fileName}", mode="w") as file:
      writer = csv.writer(file)
      writer.writerow(fieldNames)

if __name__ == "__main__":
  # Path to save files to
  PATH = os.getcwd()

  fields = ["patient_id", "enrollment_start_date", "enrollment_end_date", "ct_outpatient_visits", "ct_days_with_outpatient_visit"]
  fileName = "pt_enrollment_visits.csv"
  gen_csv_file(PATH, fileName, fields)