import pandas as pd
import io
import os
import warnings
from io import BytesIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient 
warnings.simplefilter("ignore")


# Generalized
def FC_comparison_str(F_col_name, C_col_name, F_dataframe, C_dataframe, eid):
    F_value = str(F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, F_col_name].iloc[0])
    
    C_row = C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid]
    C_value = str(C_row[C_col_name].iloc[0])

    if C_value.lower() != F_value.lower():
        C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid, C_col_name] = F_value
        print(f"Updated {C_col_name} for Employee Number {eid} from {C_value} to {F_value}")
        
        global df_updates
        data = {'Employee ID': [eid], 'Field Name':[C_col_name], 'C Data':[C_value], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FC_comparison_float(F_col_name, C_col_name, F_dataframe, C_dataframe, eid):
    F_value = float(F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, F_col_name].iloc[0])

    C_row = C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid]
    C_value = float(C_row[C_col_name].iloc[0])

    if C_value != F_value:
        C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid, C_col_name] = F_value
        print(f"Updated {C_col_name} for Employee Number {eid} from {C_value} to {F_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':[C_col_name], 'C Data':[C_value], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FC_comparison_date(F_col_name, C_col_name, F_dataframe, C_dataframe, eid):
    F_value = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, F_col_name].iloc[0]
    
    C_row = C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid]
    C_value = pd.to_datetime(C_row[C_col_name].iloc[0]).strftime("%m/%d/%Y")

    if C_value != F_value:
        C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid, C_col_name] = F_value
        print(f"Updated {C_col_name} for Employee Number {eid} from {C_value} to {F_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':[C_col_name], 'C Data':[C_value], 'F Data':[F_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

# For specific columns
def FC_comparison_bucc(F_dataframe, C_dataframe, eid):
    # Extract the required fields from df_F for the given eid
    F_row = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid]
    bu_code = F_row['BU Code'].iloc[0]
    cost_center = F_row['Cost Center'].iloc[0]
    expected_bucc_value = f"{bu_code}{cost_center}"

    # Get the current location from df_dayforce
    C_row = C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid]
    current_bucc_value = str(C_row['Cost Center'].iloc[0])

    if current_bucc_value != expected_bucc_value:
        C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid, 'Cost Center'] = expected_bucc_value
        print(f"Updated Cost Center column for Employee Number {eid} from {current_bucc_value} to {expected_bucc_value}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':["Cost Center"], 'C Data':[current_bucc_value], 'F Data':[expected_bucc_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FC_comparison_ftpt(F_dataframe, C_dataframe, eid):
    # Get the FT/PT value from C
    C_row = C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid]
    current_value = str(C_row['Full Time (F) / Part Time (P)'].iloc[0])
    
    F_row = F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid]
    assignment_cat = F_row['Assignment Category'].iloc[0]
    
    global df_updates

    if assignment_cat == "Full Time Regular":
        expected_value = "F"
    elif assignment_cat == "Part Time Regular" or assignment_cat == "Part Time No Benefits":
        expected_value = "P"
    elif assignment_cat == "Intern":
        print(f"Assignment Category for Employee Number {eid} is Intern, please review Full Time (F) / Part Time (P) value: {current_value}")
        expected_value = current_value
        data = {'Changes Made': [f"Assignment Category for Employee Number {eid} is Intern, please review Full Time (F) / Part Time (P) value: {current_value}"]}
        data = {'Employee ID': [eid], 'Field Name':["Full Time (F) / Part Time (P)"], 'C Data':[current_value], 'F Data':["Intern (PLEASE REVIEW)"]}
        
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)
    elif assignment_cat == "Expatriate":
        print(f"Assignment Category for Employee Number {eid} is Expatriate, please review Full Time (F) / Part Time (P) value: {current_value}")
        expected_value = current_value
        data = {'Changes Made': [f"Assignment Category for Employee Number {eid} is Expatriate, please review Full Time (F) / Part Time (P) value: {current_value}"]}
        data = {'Employee ID': [eid], 'Field Name':["Full Time (F) / Part Time (P)"], 'C Data':[current_value], 'F Data':["Expatriate (PLEASE REVIEW)"]}
        
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)
    elif assignment_cat == "Contractor":
        print(f"Assignment Category for Employee Number {eid} is Contractor, please review Full Time (F) / Part Time (P) value: {current_value}")
        expected_value = current_value
        data = {'Changes Made': [f"Assignment Category for Employee Number {eid} is Contractor, please review Full Time (F) / Part Time (P) value: {current_value}"]}
        data = {'Employee ID': [eid], 'Field Name':["Full Time (F) / Part Time (P)"], 'C Data':[current_value], 'F Data':["Contractor (PLEASE REVIEW)"]}
        
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

    if current_value != expected_value:
        C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid, 'Cost Center'] = expected_value
        print(f"Updated Full Time (F) / Part Time (P) column for Employee Number {eid} from {current_value} to {expected_value}")

        data = {'Employee ID': [eid], 'Field Name':["Full Time (F) / Part Time (P)"], 'C Data':[current_value], 'F Data':[expected_value]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

def FC_comparison_country(file, F_dataframe, C_dataframe, eid):
    F_value = " " + str(F_dataframe.loc[F_dataframe['Person Number'].astype(str) == eid, 'Home Address Country'].iloc[0])
    
    C_row = C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid]
    C_value = str(C_row['Country'].iloc[0])

    if F_value == ' Türkiye':
        F_value = " Turkey"
        country_index = file[file['Country'] == F_value].index[0]
        country_code = file.at[country_index, 'ISO Code']
    elif F_value != ' nan':
        country_index = file[file['Country'] == F_value].index[0]
        country_code = file.at[country_index, 'ISO Code']
    else:
        country_code = C_value

    if C_value != country_code:
        C_dataframe.loc[C_dataframe['Employee Number'].astype(str) == eid, 'Country'] = country_code
        print(f"Updated Country for Employee Number {eid} from {C_value} to {country_code}")

        global df_updates
        data = {'Employee ID': [eid], 'Field Name':["Country"], 'C Data':[C_value], 'F Data':[country_code]}
        row = pd.DataFrame(data)
        df_updates = pd.concat([df_updates, row], ignore_index=True)

# Function to get the most recent file and create a data frame
def get_most_recent_file(container_client, keyword):
    # Get list of all blobs (files) that contains the keyword
    matching_blobs = []
    for blob in container_client.list_blobs():
        if keyword.lower() in blob.name.lower():
            matching_blobs.append({'name': blob.name, 'last_modified': blob.last_modified})
    
    # Ensure there are files in the list
    if not matching_blobs:
        print(f"No files found with keyword: '{keyword}' in container.")
        return None, None
    
    # Find the most recent file
    latest_blob = max(matching_blobs, key = lambda x: x['last_modified'])
    latest_blob_name = latest_blob['name']
    print(f"Found file: {latest_blob_name}")
    
    # Download the blob data
    blob_client = container_client.get_blob_client(latest_blob_name)
    blob_data = blob_client.download_blob().readall()

    # Read the file into a DataFrame based on type of file
    if "Master" in latest_blob_name:
        #C file
        df = pd.read_excel(BytesIO(blob_data), skiprows=4, engine='openpyxl')
    else:
        #F file
        df = pd.read_excel(BytesIO(blob_data), engine='openpyxl')
    return df, latest_blob_name

def do_all_comparisons(file_name, df_to_compare):
    for i in df_to_compare.index:
        employee_number = str(df_to_compare.at[i, 'Employee Number'])
        if employee_number in F_eids_list:
            # Compare Hire Date
            FC_comparison_date('Legal Employer Hire Date', 'Employee Hire date', df_F, df_to_compare, employee_number)
            # Compare First Name
            FC_comparison_str('First Name', 'Employee First Name', df_F, df_to_compare, employee_number)
            # Compare Last Name
            FC_comparison_str('Last Name', 'Employee Last Name', df_F, df_to_compare, employee_number)
            # Compare Gender
            FC_comparison_str('Gender', 'Gender', df_F, df_to_compare, employee_number)
            # Compare Marital Status
            FC_comparison_str('Marital Status', 'Marital Status', df_F, df_to_compare, employee_number)
            # Compare DOB
            FC_comparison_date('DOB', 'Date of Birth', df_F, df_to_compare, employee_number)
            # Compare Address 1
            FC_comparison_str('Address 1', 'Address Line 1', df_F, df_to_compare, employee_number)
            # Compare Address 2
            FC_comparison_str('Address 2', 'Address Line 2', df_F, df_to_compare, employee_number)
            # Compare Country (code)
            FC_comparison_country(country_codes_file, df_F, df_to_compare, employee_number)
            # Compare Email
            FC_comparison_str('Work Email', 'Work Email Address', df_F, df_to_compare, employee_number)
            # Compare FT/PT
            FC_comparison_ftpt(df_F, df_to_compare, employee_number)
            # Compare Job Title
            FC_comparison_str('Job', 'Job Title / Description', df_F, df_to_compare, employee_number)
            # Compare Salary
            FC_comparison_float('Salary Amount', 'Annual Salary', df_F, df_to_compare, employee_number)
            # Compare BU/CC which is combined
            FC_comparison_bucc(df_F, df_to_compare, employee_number)
            # Compare SSN
            FC_comparison_str('National ID', 'Social Security Number', df_F, df_to_compare, employee_number)
        else:
            print(f"Cannot find Employee {employee_number} in F.")
            #df_updates.concat()

    #print(df_updates)

    if df_to_compare.empty or df_updates.empty:
        print("One or both of the dataframes are empty.")
    else:
        # Create Excel file in memory
        output_buffer = io.BytesIO()
        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
            df_to_compare.to_excel(writer, sheet_name='C Employee Details', index=False) 
            df_updates.to_excel(writer, sheet_name='Changes Made', index=False)

        # Reset buffer position
        output_buffer.seek(0)

        # Create blob name with timestamp to ensure uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_blob_name = f"{file_name}_C_Employee_Info_Export_{timestamp}.xlsx"

        try:
            # Upload to Azure container
            output_blob_client = cp_container2_client.get_blob_client(output_blob_name)
            output_blob_client.upload_blob(output_buffer.getvalue(), overwrite=True)
            print(f"Successfully uploaded file: {output_blob_name} to container: {cp_container2_name}")
        except Exception as e:
            print(f"Error uploading to Azure container: {str(e)}")

    df_updates.drop(df_updates.index, inplace=True)

# Get Dev Account Container from Azure
account_name = 'accountname'
connect_str = 'connectionstring'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

cp_container1_name = "C-uploads"
cp_container1_client = blob_service_client.get_container_client(cp_container1_name)

cp_container2_name = "C-outputs" 
cp_container2_client = blob_service_client.get_container_client(cp_container2_name)

# Find the most recent file with "All Employee" in its name
keyword1 = "All Employee"
df_F, F_blob_name = get_most_recent_file(cp_container1_client, keyword1)

# Find the most recent file with "Master" in its name
keyword2 = "Master"
df_C, C_blob_name = get_most_recent_file(cp_container1_client, keyword2)

# Shorten file name portion of C file
cp_file_name = C_blob_name[:-28]
    
# Create a data frame that will hold the changes made to the Dayforce data frame
df_updates = pd.DataFrame(columns=['Employee ID', 'Field Name', 'C Data', 'F Data'])

# Get country codes file to create data frame
codes_container_name = "additional-files"
codes_container_client = blob_service_client.get_container_client(codes_container_name)
codes_blob_names = []
for blob_i in codes_container_client.list_blobs():
    codes_blob_names.append(blob_i.name)
codes_blob_name = codes_blob_names[0]
codes_blob_client = codes_container_client.get_blob_client(codes_blob_name)
codes_blob_data = codes_blob_client.download_blob().readall()

country_codes_file = pd.read_excel(BytesIO(codes_blob_data), sheet_name = 'ISO Country Code')

# Edit the Gender values in F to just be the first initial
df_F['Gender'] = df_F['Gender'].str[0]

# Edit the National ID column to remove the hyphens and spaces, if it is not blank and there are hyphens/spaces
df_F['National ID'] = df_F['National ID'].apply(lambda x: x.replace("-", "").replace(" ", "") if isinstance(x, str) and '-' in x and x.strip() else x)

# Enit the BU and CC codes to jusr digits
df_F['BU Code'] = df_F['BU Code'].fillna(0).astype(int).astype(str)
df_F['Cost Center'] = df_F['Cost Center'].fillna(0).astype(int).astype(str)

F_eids_list = df_F['Person Number'].astype(str).values

# Run the comparisons
do_all_comparisons(cp_file_name, df_C)
