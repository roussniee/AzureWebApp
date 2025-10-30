import os
import sys
import os.path
import subprocess
from flask import Flask, request, redirect, url_for, render_template, flash, jsonify, session
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# Azure connection set-up
connect_str = 'connection_string_here'  # Replace with your actual connection string
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# D files container client
DFC_container1_name = "D-uploads"
DFC_container1_client = blob_service_client.get_container_client(DFC_container1_name)
DFC_container2_name = "D-outputs" 
DFC_container2_client = blob_service_client.get_container_client(DFC_container2_name)

# C files container client
cp_container1_name = "C-uploads"
cp_container1_client = blob_service_client.get_container_client(cp_container1_name)
cp_container2_name = "C-outputs" 
cp_container2_client = blob_service_client.get_container_client(cp_container2_name)

# App instance
app = Flask(__name__)

# Helper function for generating unique blob names
def get_unique_blob_name(original_name):
    name, ext = os.path.splitext(original_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{name}_{timestamp}{ext}"

# Increase maximum content length
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024  # 32MB limit

# Enable debug mode
app.debug = True

app.secret_key = 'secret_key'  # Needed for flash messages

# home page route 
@app.route('/')
def home():
    return render_template('index.html') # used to return contents of the html file specified

# Serve the f_d.html page
@app.route('/f_d', methods=['GET',])
def f_d():
    return render_template('f_d.html')

# Serve the f_c.html page
@app.route('/f_c', methods=['GET'])
def f_c():
    return render_template('f_c.html') 

# Serve the files.html page
@app.route('/files', methods=['GET'])
def files():
    return render_template('files.html')

# functions of web page f_d.html
@app.route('/upload_F_D', methods=['POST'], endpoint='upload_F_D')
def upload_F_D_files():
    try:
        app.logger.debug("Starting file upload process") 

        # Retrieve files from the form
        F_file = request.files.get('F_file')   
        D_file = request.files.get('D_file')
        
        app.logger.debug(f"Received files: F_file={F_file.filename if F_file else None}, D_file={D_file.filename if D_file else None}")

        if not F_file or not D_file:
            app.logger.warning("Missing one or both files")
            flash('Please select both files')
            return redirect(url_for('f_d'))

        try:
            # Upload F file
            app.logger.debug("Uploading F file")
            F_blob_name = F_file.filename
            try:
                blob_client = DFC_container1_client.get_blob_client(F_blob_name)
                blob_client.upload_blob(F_file.read(), overwrite=False)
                app.logger.debug(f"Successfully uploaded F file as {F_blob_name}")
            except ResourceExistsError:
                app.logger.debug("F file exists, creating new name")
                F_blob_name = get_unique_blob_name(F_file.filename)
                blob_client = DFC_container1_client.get_blob_client(F_blob_name)
                F_file.seek(0)
                blob_client.upload_blob(F_file.read())
                app.logger.debug(f"Successfully uploaded F file with new name {F_blob_name}")

            # Upload D file
            app.logger.debug("Uploading D file")
            D_blob_name = D_file.filename
            try:
                blob_client = DFC_container1_client.get_blob_client(D_blob_name)
                blob_client.upload_blob(D_file.read(), overwrite=False)
                app.logger.debug(f"Successfully uploaded D file as {D_blob_name}")
            except ResourceExistsError:
                app.logger.debug("D file exists, creating new name")
                D_blob_name = get_unique_blob_name(D_file.filename)
                blob_client = DFC_container1_client.get_blob_client(D_blob_name)
                D_file.seek(0)
                blob_client.upload_blob(D_file.read())
                app.logger.debug(f"Successfully uploaded D file with new name {D_blob_name}")

            app.logger.debug("All files uploaded successfully, preparing to redirect")
            flash(f'Files successfully uploaded as {F_blob_name} and {D_blob_name}')
            response = redirect(url_for('f_d'))
            app.logger.debug("Created redirect response")
            return response

        except Exception as e:
            app.logger.error(f'Azure upload error: {str(e)}', exc_info=True)
            flash(f'Error uploading to Azure: {str(e)}')
            return redirect(url_for('f_d'))

    except Exception as e:
        app.logger.error(f'Upload error: {str(e)}', exc_info=True)
        flash('An error occurred during file upload')
        return redirect(url_for('f_d'))


# run script button for F - D
@app.route('/run_script_f_d', methods=['POST'], endpoint='run_script_f_d')
def run_script_D():
    try:
        app.logger.debug("Starting FD comparison script")
        
        # Get absolute path to the script
        base_dir = os.path.abspath(os.path.dirname(__file__))
        script_path = os.path.join(base_dir, 'FD Implementation.py')
        
        app.logger.debug(f"Script path: {script_path}")

        # Retrieve the password from the session (optional field)
        file_password = session.get('file_password', None)

        # Verify script exists
        if not os.path.exists(script_path):
            error_msg = f"Script not found at path: {script_path}"
            app.logger.error(error_msg)
            flash(error_msg)
            return redirect(url_for('f_d'))

        # Run the script, add password as an argument
        try:
            # Use the same Python interpreter that's running the Flask app
            result = subprocess.run(
                [sys.executable, script_path, file_password] if file_password else [sys.executable, script_path],
                capture_output=True,
                text=True,
                cwd=base_dir  # Set working directory to where the script is
            )
            
            # Log the output
            app.logger.debug(f"Script stdout: {result.stdout}")
            if result.stderr:
                app.logger.error(f"Script stderr: {result.stderr}")
            
            if result.returncode == 0:
                flash('Comparison completed successfully')
            else:
                error_msg = f'Script failed with error: {result.stderr}'
                app.logger.error(error_msg)
                flash(error_msg)
                
        except Exception as e:
            error_msg = f"Error executing script: {str(e)}"
            app.logger.error(error_msg, exc_info=True)
            flash(error_msg)
            
        return redirect(url_for('f_d'))
        
    except Exception as e:
        error_msg = f"Error in route handler: {str(e)}"
        app.logger.error(error_msg, exc_info=True)
        flash(error_msg)
        return redirect(url_for('f_d'))
     
@app.route('/submit_password', methods=['POST'])
def submit_password():
    # Get password that was provided (sometimes none)
    file_password = request.form.get('file_password', '')
    
    # Store the password in a session or a variable as needed
    session['file_password'] = file_password
    #pw = session['file_password']
    flash(f'Password submitted successfully!')
    return redirect(url_for('f_d'))

# Functionality of f_c.html page
@app.route('/upload_F_C', methods=['POST'], endpoint='upload_F_C')
def upload_F_C_files():
    try:
        app.logger.debug("Starting file upload process")

        # Retrieve files from the form
        F_file = request.files['F_file']
        C_file = request.files['C_file']

        app.logger.debug(f"Received files: F_file={F_file.filename if F_file else None}, C_file={C_file.filename if C_file else None}")
        
        if not F_file or not C_file:
            app.logger.warning("Missing one or both files")
            flash('Please select both files')
            return redirect(url_for('f_c'))

        try:
            # Upload F file
            app.logger.debug("Uploading F file")
            F_blob_name = F_file.filename
            try:
                blob_client = cp_container1_client.get_blob_client(F_blob_name)
                blob_client.upload_blob(F_file.read(), overwrite=False)
                app.logger.debug(f"Successfully uploaded F file as {F_blob_name}")
            except ResourceExistsError:
                app.logger.debug("F file exists, creating new name")
                F_blob_name = get_unique_blob_name(F_file.filename)
                blob_client = cp_container1_client.get_blob_client(F_blob_name)
                F_file.seek(0)
                blob_client.upload_blob(F_file.read())
                app.logger.debug(f"Successfully uploaded F file with new name {F_blob_name}")

            # Upload C file
            app.logger.debug("Uploading C file")
            C_blob_name = C_file.filename
            try:
                blob_client = cp_container1_client.get_blob_client(C_blob_name)
                blob_client.upload_blob(C_file.read(), overwrite=False)
                app.logger.debug(f"Successfully uploaded D file as {C_blob_name}")
            except ResourceExistsError:
                app.logger.debug("C file exists, creating new name")
                C_blob_name = get_unique_blob_name(C_file.filename)
                blob_client = cp_container1_client.get_blob_client(C_blob_name)
                C_file.seek(0)
                blob_client.upload_blob(C_file.read())
                app.logger.debug(f"Successfully uploaded C file with new name {C_blob_name}")

            app.logger.debug("All files uploaded successfully, preparing to redirect")
            flash(f'Files successfully uploaded as {F_blob_name} and {C_blob_name}')
            response = redirect(url_for('f_c'))
            app.logger.debug("Created redirect response")
            return response
        
        except Exception as e:
            app.logger.error(f'Azure upload error: {str(e)}', exc_info=True)
            flash(f'Error uploading to Azure: {str(e)}')
            return redirect(url_for('f_c'))
        
    except Exception as e:
        app.logger.error(f'Upload error: {str(e)}', exc_info=True)
        flash('An error occurred during file upload')
        return redirect(url_for('f_c'))


# run script button for F - C
@app.route('/run_script_f_c', methods=['POST'],  endpoint='run_script_f_c')
def run_script_C():
    try:
        app.logger.debug("Starting FC comparison script")

        # Get absolute path to the script
        base_dir = os.path.abspath(os.path.dirname(__file__))
        script_path = os.path.join(base_dir, 'FC Implementation.py')
        
        app.logger.debug(f"Script path: {script_path}")

        # Verify script exists
        if not os.path.exists(script_path):
            error_msg = f"Script not found at path: {script_path}"
            app.logger.error(error_msg)
            flash(error_msg)
            return redirect(url_for('f_c'))
        
        # Run the script
        try:
            # Use the same Python interpreter that's running the Flask app
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                cwd=base_dir  # Set working directory to where the script is
            )

            # Log the output
            app.logger.debug(f"Script stdout: {result.stdout}")
            if result.stderr:
                app.logger.error(f"Script stderr: {result.stderr}")

            if result.returncode == 0:
                flash('Comparison completed successfully')
            else:
                error_msg = f'Script failed with error: {result.stderr}'
                app.logger.error(error_msg)
                flash(error_msg)

        except Exception as e:
            error_msg = f"Error executing script: {str(e)}"
            app.logger.error(error_msg, exc_info=True)
            flash(error_msg)

        return redirect(url_for('f_c'))
    
    except Exception as e:
        error_msg = f"Error in route handler: {str(e)}"
        app.logger.error(error_msg, exc_info=True)
        flash(error_msg)
        return redirect(url_for('f_c'))


# Function to get files from Azure Blob Storage - only for D outputs atm, need to add features for cp too
@app.route('/api/files', methods=['GET'])
def get_files():
    # Create lists for files/blobs from both containers
    D_files = []
    C_files = []

    try:
        # Get D output files
        for blob in DFC_container2_client.list_blobs():
            D_files.append({'name': blob.name, 'container': 'D-outputs', 'last_modified': blob.last_modified.strftime("%Y-%m-%d %H:%M:%S")})
        
        # Get C output files
        for blob in cp_container2_client.list_blobs():
            C_files.append({'name': blob.name, 'container': 'C-outputs', 'last_modified': blob.last_modified.strftime("%Y-%m-%d %H:%M:%S")})
        
        return jsonify({'D_files': D_files, 'C_files': C_files, 'status': 'success'})
    
    except Exception as e:
        return jsonify({'status': 'error','message': str(e)}), 500

# Allow user to download the output files
@app.route('/download/<container>/<filename>')
def download_file(container, filename):
    try:
        if container == 'D-outputs':
            container_client = DFC_container2_client
        elif container == 'C-outputs':
            container_client = cp_container2_client
        else:
            return "Invalid container", 400

        blob_client = container_client.get_blob_client(filename)
        blob_data = blob_client.download_blob()
        
        return blob_data.readall(), 200, {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
    except Exception as e:
        return str(e), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True) 
