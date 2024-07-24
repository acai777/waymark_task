import boto3
import botocore.exceptions
import sys 
import pandas as pd 
from pandas.tseries.offsets import MonthEnd
import os
from dotenv import load_dotenv


##############################
# Setup
##############################
# Path to save files to
PATH = os.getcwd()

# Get environmental variables from .env file 
load_dotenv()

# Function to retrieve files from AWS S3 Bucket
def retrieve_file(bucket_name, object_key, aws_access_key_id, aws_secret_access_key, file_name):
  """
  Inputs:
  1) bucket name, string
  2) object key, string
  3) aws access key id, string
  4) aws secret access key, string
  5) file name, string

  Output: None; function retrieves and saves the relevant CSV file from the AWS S3 bucket
  """
  # Creates the client to interact with S3 and make the API call
  client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

  # Downloading the file and saving it as `file_name`'s string value
  # Include error handling in case there are any issues interacting with the API 
  try:  
    client.download_file(bucket_name, object_key, file_name)
  except botocore.exceptions.ClientError as error:
    print('Something went wrong downloading the file from the AWS S3 Bucket')
    print(f"Error Code: {error.response['Error']['Code']}") 
    print(f"Error message: {error.response['Error']['Message']}") 
    sys.exit(1)


##############################
# Step 1: Data Transformation
##############################
# For API calls to actual healthcare data, leaving AWS credentials in the script here might be a security risk.
# As such, for this exercise, I use a .env file to store the AWS credentials
bucket_name = os.getenv("BUCKET_NAME_1")
object_key = os.getenv("OBJECT_KEY_1")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_1")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY_1")
file_name = 'patient_id_month_year.csv'

# Retrieve and save the file 
retrieve_file(bucket_name, object_key, aws_access_key_id, aws_secret_access_key, file_name)

# Import csv file to pandas dataframe
pt_enrollment_span = pd.read_csv(object_key, usecols=['patient_id', 'month_year'], dtype={
  'patient_id': 'string',
  'month_year': 'string'
})

# Drop all NA rows
pt_enrollment_span = pt_enrollment_span.dropna(how='all')

'''
  Generate patient enrollment start and end date variables from `month_year` var. 
  Note that the enrollment periods are not necessarily contiguous; it is possible for patients to be enrolled for non-contiguous months. 
  Algorithm: create `group` groups variable by patient_id and CONTIGUOUS month-years, where AA/YYYY and BB/YYYY are contiguous if AA + 1month = BB
  Algorithm Reference: https://stackoverflow.com/questions/52901387/find-group-of-consecutive-dates-in-pandas-dataframe

  General Algorithm Steps:
  Sort the dates 
  Generate different groups `group` for each patient_id, where the group corresponds to CONTINUOUS enrollment dates.
  The continuous enrollment dates are determined by `month_year`

  By each patient_id group, generate the enrollment_start_date and enrollment_end_date
  Rework enrollment_end_date to be the last day of the month

  Drop the `month_year` and `group` variables
  De-duplicate on patient_id, enrollment_start_date, and enrollment_end_date
'''
pt_enrollment_span['month_year'] = pd.to_datetime(pt_enrollment_span['month_year'], format='%m/%d/%y') # convert to datetime type
pt_enrollment_span = pt_enrollment_span.sort_values(by=['patient_id', 'month_year']) # sort by patient and month_year

# Generate month and year columns for easier processing
pt_enrollment_span['month'] = pd.DatetimeIndex(pt_enrollment_span['month_year']).month 
pt_enrollment_span['year'] = pd.DatetimeIndex(pt_enrollment_span['month_year']).year 

# Check that we only have one year. Otherwise, algorithm won't work. E.g., if the month_years are 1/1/2024 and 12/1/2023
print((pt_enrollment_span['year'] == 2023).all())

# To ensure months are contiguous, compare the current month with previous row's month. 
# # If is off by 1, we know is contiguous and can group the rows in the same group.
dt = pt_enrollment_span['month']
in_block = ((dt  - dt.shift(-1)).abs() == 1) | (dt.diff() == 1) # `diff` calculates the difference of a DataFrame element compared with another element in the DataFrame (default is element in previous row).

filt = pt_enrollment_span.loc[in_block]
breaks = filt['month'].diff() != 1
groups = breaks.cumsum()
pt_enrollment_span['groups'] = groups

# Need to fill in for months that are NA. These are months whose enrollment period is only in that one month
dt = pt_enrollment_span['month']
breaks = dt.diff() != 1
groups = breaks.cumsum()
pt_enrollment_span['groups'] = groups

# Now that we have a `groups` column, can now generate the enrollment start and end dates
pt_enrollment_span['enrollment_start_date'] = pt_enrollment_span.groupby('groups')['month_year'].transform('min')
pt_enrollment_span['enrollment_end_date'] = pt_enrollment_span.groupby('groups')['month_year'].transform('max')

# Drop all duplicate rows and unneeded column variables
pt_enrollment_span = pt_enrollment_span[(pt_enrollment_span['enrollment_start_date'] == pt_enrollment_span['month_year']) | (pt_enrollment_span['enrollment_end_date'] == pt_enrollment_span['month_year'])]
pt_enrollment_span = pt_enrollment_span.drop(['month_year', 'groups', 'month', 'year'], axis=1)
pt_enrollment_span = pt_enrollment_span.drop_duplicates()

# Fix the enrollment_end_date to be the last DAY of the month, not the first 
# Code reference: https://stackoverflow.com/questions/37354105/find-the-end-of-the-month-of-a-pandas-dataframe-series 
pt_enrollment_span['enrollment_end_date'] = pt_enrollment_span['enrollment_end_date'] + MonthEnd(0) 

# Print number of rows and save csv file
print(f'There are {len(pt_enrollment_span.index)} rows in patient_enrollment_span.csv, excluding the variable name.') # 3105 rows
pt_enrollment_span.to_csv(f"{PATH}/patient_enrollment_span.csv", index=False)


##############################
# Step 2: Data Aggregation
##############################
bucket_name = os.getenv("BUCKET_NAME_2")
object_key = os.getenv("OBJECT_KEY_2")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_2")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY_2")
file_name = 'outpatient_visits_file.csv'

# Retrieve and save the file 
retrieve_file(bucket_name, object_key, aws_access_key_id, aws_secret_access_key, file_name)

# Import csv file to pandas dataframe
outpt_enrollment_span = pd.read_csv(object_key, usecols=['patient_id', 'date', 'outpatient_visit_count'], dtype={
  'patient_id': 'string',
  'date': 'string',
  'outpatient_visit_count': 'Int64'
})

# Drop all NA rows
outpt_enrollment_span = outpt_enrollment_span.dropna(how='all')

# Sanity check to ensure only positive outpatient numbers 
print((outpt_enrollment_span['outpatient_visit_count'] >= 1).all()) # True

# Convert date from string format to pandas datetime type 
outpt_enrollment_span['date'] = pd.to_datetime(outpt_enrollment_span['date'], format='%m/%d/%y') # convert to datetime type

# Generate ct_outpatient_visits, which is # of outpatient visis a patient had between enrollment start and end date 
# Will first merge 1:1 on patient_id 
combined = pd.merge(pt_enrollment_span, outpt_enrollment_span)

# Check to make sure all `date` values are between enrollment start date and end date. 
print(((combined['date'] >= combined['enrollment_start_date']) & (combined['date'] <= combined['enrollment_end_date'])).all()) # False

# Result above is false. This means there are some outpt visits that occur outside of the enrollment period. 
# We drop such rows, as ct_outpatient_visits is defined solely for visits IN enrollment period. 
# Same definition holds for the ct_days_with_outpatient_visit
combined = combined[(combined['enrollment_start_date'] <= combined['date']) & (combined['enrollment_end_date'] >= combined['date'])]
combined['ct_outpatient_visits'] = combined.groupby(by=['patient_id', 'enrollment_start_date', 'enrollment_end_date'])['outpatient_visit_count'].transform('sum')

# Generate ct_days_with_outpatient_visit, number of distinct days within an enrollment period where the patient had one or more outpatient visit. 
# Check if there are duplicate `date` values by patient-enrollment period first. 
# If there are, de-duplicate. Those rows correspond to outpatient visits on the same day; we want to count only one row.
exists_duplicates = combined.duplicated(subset=['patient_id', 'enrollment_start_date', 'enrollment_end_date', 'date']).any()
if exists_duplicates == True: 
  combined = combined.drop_duplicates(subset=['patient_id', 'enrollment_start_date', 'enrollment_end_date', 'date'])

combined['ct_days_with_outpatient_visit'] = combined.groupby(by=['patient_id', 'enrollment_start_date', 'enrollment_end_date'])['outpatient_visit_count'].transform('count')

# Drop unnecessary columns, de-duplicate, and save file 
combined = combined.drop(['date', 'outpatient_visit_count'], axis=1)
combined = combined.drop_duplicates()
combined.to_csv(f"{PATH}/result.csv", index=False)

# Print the number of distinct values of ct_days_with_outpatient_visit
print(f"There are {combined['ct_days_with_outpatient_visit'].nunique()} distinct values of ct_days_with_outpatient_visit.") # 32 distinct values 