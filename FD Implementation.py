import pandas as pd
import io
import os
import re
import sys
import warnings
from io import BytesIO
from datetime import datetime
from msoffcrypto import OfficeFile
from azure.storage.blob import BlobServiceClient 
warnings.filterwarnings("ignore")

# Decrypt a password protected Excel file
def read_protected_excel(file_path, password, string_cols):
    decrypted = io.BytesIO()
    with open(file_path, "rb") as file:
        office_file = msoffcrypto.OfficeFile(file)
        office_file.load_key(password=password)
        office_file.decrypt(decrypted)
    return pd.read_excel(decrypted, engine='openpyxl', dtype={col: str for col in string_cols})

# Generalized
def FD_comparison_str(F_col_name, D_col_name, F_dataframe, D_dataframe, eid):
    F_value = str(F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, F_col_name].iloc[0]).strip()
    D_value = str(D_dataframe.at[i, D_col_name]).strip()
    if D_value.lower() != F_value.lower():
        D_dataframe.at[i, D_col_name] = F_value
        print(f"Updated {D_col_name} for Employee Number {eid} from {D_value} to {F_value}")
        
        global df_updates
        data = {'Employee ID': [eid], 'Field Name':[D_col_name], 'D Data':[D_value], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FD_comparison_float(F_col_name, D_col_name, F_dataframe, D_dataframe, eid):
    F_value = float(F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, F_col_name].iloc[0])
    D_value = float(D_dataframe.at[i, D_col_name])
    if D_value != F_value:
        D_dataframe.at[i, D_col_name] = F_value
        print(f"Updated {D_col_name} for Employee Number {eid} from {D_value} to {F_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':[D_col_name], 'D Data':[D_value], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FD_comparison_date(F_col_name, D_col_name, F_dataframe, D_dataframe, eid):
    F_value = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, F_col_name].iloc[0]
    D_value = pd.to_datetime(D_dataframe.at[i, D_col_name]).strftime("%m/%d/%Y")
    if D_value != F_value:
        D_dataframe.at[i, D_col_name] = F_value
        print(f"Updated {D_col_name} for Employee Number {eid} from {D_value} to {F_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':[D_col_name], 'D Data':[D_value], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

# For specific columns
def FD_comparison_location(F_dataframe, D_dataframe, eid):
    # Extract the required fields from df_F for the given eid
    F_row = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid]
    reporting_name = F_row['Reporting Name'].iloc[0]
    try:
        bu_code = str(int(F_row['BU Code'].iloc[0]))
        cost_center = str(int(F_row['Cost Center'].iloc[0]))
        expected_location_value = f"{reporting_name}_{bu_code}_{cost_center}_OS"
    except ValueError as e:
        D_row = D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid]
        expected_location_value = D_row['Location'].iloc[0].strip()
    
    # Get the current location from df_D
    D_row = D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid]
    current_location_value = D_row['Location'].iloc[0].strip()

    if current_location_value != expected_location_value:
        D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid, 'Location'] = expected_location_value
        print(f"Updated Location column for Employee Number {eid} from {current_location_value} to {expected_location_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':["Location"], 'D Data':[current_location_value], 'F Data':[expected_location_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FD_comparison_employee(F_dataframe, D_dataframe, eid):
    # Extract the required fields from df_F for the given eid
    F_row = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid]
    last_name = F_row['Last Name'].iloc[0]
    first_name = F_row['First Name'].iloc[0]
    middle_name = F_row['Middle Name'].iloc[0] if 'Middle Name' in F_row.columns else ''
    middle_name = str(middle_name) if pd.notna(middle_name) else ''
    person_number = F_row['Person Number'].iloc[0]

    # Handle middle name initial if it exists
    if middle_name and len(middle_name) > 1:
        middle_initial = middle_name[0].strip()
    else:
        middle_initial = middle_name

    # Construct the expected Employee value
    if middle_initial:
        expected_employee_value = f"{last_name.strip()}, {first_name.strip()} {middle_initial} - {person_number}"
    else:
        expected_employee_value = f"{last_name.strip()}, {first_name.strip()} - {person_number}"

    # Get the current location from df_D
    D_row = D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid]
    current_employee_value = D_row['Employee'].iloc[0]

    # Compare and update if necessary
    if current_employee_value != expected_employee_value:
        D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid, 'Employee'] = expected_employee_value
        print(f"Updated Employee column for Employee Number {eid} from {current_employee_value} to {expected_employee_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':["Employee"], 'D Data':[current_employee_value], 'F Data':[expected_employee_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FD_base_rate_float(F_dataframe, D_dataframe, eid):
    F_row = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid]
    F_salary = F_row['Salary Amount'].iloc[0]
    base_rate_calc = round(float(F_salary / 2080), 3)
    
    D_row = D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid]
    D_base_rate = D_row['Base Rate'].iloc[0]

    if D_base_rate != base_rate_calc:
        D_dataframe.loc[D_dataframe['Employee Number'].astype(str) == eid, 'Base Rate'] = base_rate_calc
        print(f"Updated Base Rate column for Employee Number {eid} from {D_base_rate} to {base_rate_calc}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':["Base Rate"], 'D Data':[D_base_rate], 'F Data':[base_rate_calc]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FD_comparison_country(file, F_dataframe, D_dataframe, eid):
    F_value = str(F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, 'Home Address Country'].iloc[0])
    #print(file[file['Country'] == F_value])
    try:
        country_index = file[file['Country'] == F_value].index[0]
        country_code = file.at[country_index, 'Code']

        D_value = str(D_dataframe.at[i, 'Person Address Country Code']).strip()
        if D_value != country_code:
            D_dataframe.at[i, 'Person Address Country Code'] = country_code
            print(f"Updated Person Address Country Code for Employee Number {eid} from {D_value} to {country_code}")

            global df_updates
            data = {'Employee ID': [eid], 'Field Name':["Person Address Country Code"], 'D Data':[D_value], 'F Data':[country_code]}
            row = pd.DataFrame(data)
            df_updates = pd.concat([df_updates, row], ignore_index=True)
    except IndexError as e:
        print(f"Home Address Country value for Employee Number {eid} is {F_value} in F. Please review in F.")
        data = {'Employee ID': [eid], 'Field Name':["Home Address Country"], 'D Data':["Please review in F."], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

# Function to get the most recent file with a keyword in its name
def get_most_recent_file(container_client, keyword, password=None, string_cols=None):
    """
    Get the most recent file from Azure container matching the keyword.
    Automatically handles both encrypted and unencrypted files.
    """
    # Find matching blobs
    matching_blobs = []
    for blob in container_client.list_blobs():
        if keyword.lower() in blob.name.lower():
            matching_blobs.append({'name': blob.name, 'last_modified': blob.last_modified})
    
    if not matching_blobs:
        for blob in container_client.list_blobs():
            if re.search(keyword, blob.name):
                matching_blobs.append({'name': blob.name, 'last_modified': blob.last_modified})
            elif "D" in blob.name.lower():
                matching_blobs.append({'name': blob.name, 'last_modified': blob.last_modified})

    if not matching_blobs:
        print(f"No files found with keyword: '{keyword}' in container.")
        return None, None

    # Get the most recent file
    latest_blob = max(matching_blobs, key=lambda x: x['last_modified'])
    latest_blob_name = latest_blob['name']
    print(f"Found file: {latest_blob_name}")

    # Download the blob data
    blob_client = container_client.get_blob_client(latest_blob_name)
    blob_data = blob_client.download_blob().readall()
    
    # Try reading the file directly first
    try:
        df = pd.read_excel(BytesIO(blob_data), dtype={col: str for col in string_cols} if string_cols else None)
        print(f"Successfully read unencrypted file: {latest_blob_name}")
        return df, latest_blob_name
    except Exception as e:
        # If direct reading fails and we have a password, try decryption
        if password:
            try:
                decrypted = io.BytesIO()
                office_file = OfficeFile(BytesIO(blob_data))
                office_file.load_key(password=password)
                office_file.decrypt(decrypted)
                decrypted.seek(0)
                df = pd.read_excel(decrypted, engine='openpyxl', dtype={col: str for col in string_cols} if string_cols else None)
                print(f"Successfully read encrypted file: {latest_blob_name}")
                return df, latest_blob_name
            except Exception as decrypt_error:
                print(f"Failed to decrypt file: {str(decrypt_error)}")
                raise
        else:
            print(f"Failed to read file and no password provided: {str(e)}")
            raise


account_name = 'accountname'
connect_str = 'connectionstring'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

DFC_container1_name = "D-uploads"
DFC_container1_client = blob_service_client.get_container_client(DFC_container1_name)

DFC_container2_name = "D-outputs" 
DFC_container2_client = blob_service_client.get_container_client(DFC_container2_name)

if len(sys.argv) > 1:
    password = sys.argv[1]
else:
    password = None

string_columns = ['Cost Center']  # Add other column names as needed

# Find the most recent file with "All Employee" in its name
keyword1 = "All Employee"

# Find the most recent file with a date pattern in its name
keyword2 = r"\d{1,2}-\d{1,2}-\d{4}" 

# Create Dataframes and get file names
df_F, F_blob_name = get_most_recent_file(DFC_container1_client, keyword1, password, string_columns)
df_D, D_blob_name = get_most_recent_file(DFC_container1_client, keyword2, password, string_columns)

# Shorten file name portion of D file
if "Report" in D_blob_name:
    df_file_name = D_blob_name[45:-5]
else:
    df_file_name = D_blob_name[23:-5]
print(df_file_name)

# Create a data frame that will hold the changes made to the D data frame
df_updates = pd.DataFrame(columns=['Employee ID', 'Field Name', 'D Data', 'F Data'])

# Get state/country codes file to create data frames
codes_container_name = "additional-files"
codes_container_client = blob_service_client.get_container_client(codes_container_name)
codes_blob_names = []
for blob_i in codes_container_client.list_blobs():
    codes_blob_names.append(blob_i.name)
codes_blob_name = codes_blob_names[1]
codes_blob_client = codes_container_client.get_blob_client(codes_blob_name)
codes_blob_data = codes_blob_client.download_blob().readall()

country_codes_file = pd.read_excel(BytesIO(codes_blob_data), sheet_name = 'ISO Country Code')
state_codes_file = pd.read_excel(BytesIO(codes_blob_data), sheet_name = 'State & Provinces')

for i, row in state_codes_file.iterrows():
    state_codes_file.at[i, 'State or Province'] = str(row['State or Province']).split(', ')[0]

# Edit the National ID column to remove the hyphens and spaces
df_F['National ID'] = df_F['National ID'].astype(str).str.replace("-", "").str.replace(" ", "")

# List of Person Numbers from Fm as strings
F_eids_list = df_F['Person Number'].astype(str).values

# Items in D with Status of Terminated
terminated_count = 0

for i in df_D.index:
    D_employee_number = str(df_D.at[i, 'Employee Number'])
    if D_employee_number in F_eids_list:
        # First Name comparison
        FD_comparison_str('First Name', 'Employee First Name', df_F, df_D, D_employee_number)
        # Last Name comparison
        FD_comparison_str('Last Name', 'Employee Last Name', df_F, df_D, D_employee_number)
        # SSN/SIN comparison
        FD_comparison_str('National ID', 'Employee SSN/SIN', df_F, df_D, D_employee_number)
        # DOB comparison
        FD_comparison_date('DOB', 'Employee Birth Date', df_F, df_D, D_employee_number)
        # Address Line 1 comparison
        FD_comparison_str('Address 1', 'Person Address Address 1', df_F, df_D, D_employee_number)
        # Address Line 2 comparison
        FD_comparison_str('Address 2', 'Person Address Address 2', df_F, df_D, D_employee_number)
        # City comparison
        FD_comparison_str('Town or City', 'Person Address City', df_F, df_D, D_employee_number)
        # Postal Code comparison
        FD_comparison_str('Postal Code', 'Person Address Postal Code', df_F, df_D, D_employee_number)
        # Hire date comparison
        FD_comparison_date('Legal Employer Hire Date', 'Original Hire Date', df_F, df_D, D_employee_number)
        # Status comparison
        FD_comparison_str('Assignment Status', 'Status', df_F, df_D, D_employee_number)
        # Assignement Category/Pay Class comparison
        FD_comparison_str('Assignment Category', 'Pay Class', df_F, df_D, D_employee_number)
        # Job Title comparison
        FD_comparison_str('Job', 'Job', df_F, df_D, D_employee_number)
        # Pay Type comparison
        FD_comparison_str('Pay Type', 'Pay Type', df_F, df_D, D_employee_number)
        # Salary comparison
        FD_comparison_float('Salary Amount', 'Base Salary', df_F, df_D, D_employee_number)
        # D Location column comparison
        FD_comparison_location(df_F, df_D, D_employee_number)
        # D Employee column comparison
        FD_comparison_employee(df_F, df_D, D_employee_number)
        # D Base Rate calculation comparison 
        FD_base_rate_float(df_F, df_D, D_employee_number)
        # D Country Code comparison
        FD_comparison_country(country_codes_file, df_F, df_D, D_employee_number)
    else:
        if df_D.at[i, 'Status'] == 'Terminated':
            terminated_count += 1

#print(terminated_count)

# Convert the display only the date for dob and hire date
df_D['Employee Birth Date'] = pd.to_datetime(df_D['Employee Birth Date']).dt.strftime('%m/%d/%Y')
df_D['Original Hire Date'] = pd.to_datetime(df_D['Original Hire Date']).dt.strftime('%m/%d/%Y')

# Check if dataframes are not empty
if df_D.empty or df_updates.empty:
    print("One or both of the dataframes are empty.")
else:
    # Create Excel file in memory
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        df_D.to_excel(writer, sheet_name='D Employee Details', index=False) 
        df_updates.to_excel(writer, sheet_name='Changes Made', index=False)
    
    # Reset buffer position
    output_buffer.seek(0)
    
    # Create blob name with timestamp to ensure uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_blob_name = f"{df_file_name}_D_Employee_Info_Export_{timestamp}.xlsx"
    
    try:
        # Upload to Azure container
        output_blob_client = DFC_container2_client.get_blob_client(output_blob_name)
        output_blob_client.upload_blob(output_buffer.getvalue(), overwrite=True)
        print(f"Successfully uploaded file: {output_blob_name} to container: {DFC_container2_name}")
    except Exception as e:
        print(f"Error uploading to Azure container: {str(e)}")