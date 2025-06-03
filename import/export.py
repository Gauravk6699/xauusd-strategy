import requests
import json
import os

# ==== CONFIGURATION (using your provided credentials) ====
CLIENT_ID = "1000.JQW928OBFSCUD6DF27H6NT6BJRY2BO"
CLIENT_SECRET = "dbfe37038ecd69d73a612c8e8f39562dc9aa96f105"
REFRESH_TOKEN = "1000.8fc73be374e3c3a17835cf808c146785.82d73e9ce9fde61bef7839d9c98a92e7"

# Token refresh URL
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

# Base URL for WorkDrive API calls
WORKDRIVE_BASE_URL = "https://workdrive.zoho.com" # Default
# WORKDRIVE_BASE_URL = "https://workdrive.zoho.in" # Alternative for Indian DC if .com fails

# === SPECIFIC FOLDER ID TO PROCESS ===
FOLDER_ID_TO_ACCESS = "3qp0y7911bff587ea4b31bca716852950bbec"

EXPORT_FOLDER = './exported_zoho_files'

HEADERS = {} # Will be populated by get_new_access_token()

os.makedirs(EXPORT_FOLDER, exist_ok=True)

# ==== AUTHENTICATION FUNCTION ====
def get_new_access_token():
    global HEADERS
    payload = {
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }
    print(f"Requesting new access token from: {TOKEN_URL}")
    try:
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        token_data = response.json()
        if 'access_token' in token_data:
            access_token = token_data['access_token']
            print("Successfully obtained new access token.")
            HEADERS = {'Authorization': f'Zoho-oauthtoken {access_token}'}
            return True
        else:
            print("Error: 'access_token' not found in token refresh response.")
            print("Token Refresh Response:", json.dumps(token_data, indent=2))
            return False
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error during token refresh: {http_err}")
        if response: print(f"Token Refresh Response content: {response.text}")
        return False
    except Exception as e:
        print(f"An error occurred during token refresh: {e}")
        return False

# ==== API FUNCTIONS ====
# Step 1: List files in the specified WorkDrive folder
def list_files_in_folder(folder_id): # Changed function name for clarity
    # Using the endpoint for listing files within a specific folder
    url = f'{WORKDRIVE_BASE_URL}/api/v1/folders/{folder_id}/files'
    all_files = []
    page_count = 1

    print(f"\nListing files in folder '{folder_id}' (Page {page_count})...")
    print(f"Request URL: {url}") # Print the initial URL
    while url:
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            print(f"Error fetching files from folder '{folder_id}': {http_err}")
            print(f"Response content: {response.text}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Request error fetching files from folder '{folder_id}': {req_err}")
            break
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"Error decoding JSON response from {url}. Response text: {response.text}")
            break

        current_files = data.get('data', [])
        all_files.extend(current_files)
        print(f"Fetched {len(current_files)} files on this page. Total fetched from folder: {len(all_files)}")

        # Pagination (assuming similar structure for this endpoint)
        meta_info = data.get('meta', {})
        if not meta_info:
             meta_info = data.get('info', {}) # Fallback from your original script

        # Check for 'more_records' and a way to get the next page
        # The API might use 'next_page_token' or provide a full 'next_link' (more_records as URL)
        next_page_token = meta_info.get('next_page_token')
        more_records_flag = meta_info.get('more_records') # This might be a boolean or a URL string

        if more_records_flag:
            if next_page_token:
                # If next_page_token is available, use it (common for Zoho APIs)
                # Note: The exact parameter name for the token might vary ('page_token', 'next_page_token')
                # This example assumes 'page_token' as a common query param. Adjust if API docs say otherwise.
                url = f'{WORKDRIVE_BASE_URL}/api/v1/folders/{folder_id}/files?page_token={next_page_token}'
                page_count += 1
                print(f"\nListing files in folder '{folder_id}' (Page {page_count} using page_token)...")
                print(f"Request URL: {url}")
            elif isinstance(more_records_flag, str) and more_records_flag.startswith('http'):
                # If more_records itself is a full URL (as in your initial script example)
                url = more_records_flag
                page_count += 1
                print(f"\nListing files in folder '{folder_id}' (Page {page_count} using direct URL)...")
                print(f"Request URL: {url}")
            else:
                # If more_records is true but no clear way to get next page from response
                print("More records might exist, but pagination method unclear from response. Stopping pagination.")
                url = None
        else:
            url = None # No more pages or no clear 'more_records' signal

    return all_files

# Step 2: Export files (remains the same)
def export_file(file_id, file_name, export_format):
    export_url = f"{WORKDRIVE_BASE_URL}/api/v1/files/{file_id}/export?format={export_format}"
    print(f"Attempting to export: {file_name} as {export_format} from {export_url}")
    try:
        res = requests.get(export_url, headers=HEADERS, stream=True)
        res.raise_for_status()
        content_type = res.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            try:
                error_data = res.json()
                print(f"⚠️ Received JSON response during export of {file_name}, might be an error: {json.dumps(error_data)}")
                return
            except json.JSONDecodeError:
                print(f"⚠️ Received undecodable JSON response during export of {file_name}. Status: {res.status_code}")
                return

        out_path = os.path.join(EXPORT_FOLDER, f"{file_name}.{export_format}")
        with open(out_path, 'wb') as f:
            for chunk in res.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Exported: {out_path}")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Failed to export {file_name} ({export_url}): {http_err}")
        print(f"Response content: {res.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"❌ Request error exporting {file_name} ({export_url}): {req_err}")
    except Exception as e:
        print(f"❌ An unexpected error occurred exporting {file_name}: {e}")

# Step 3: Run conversion
def main():
    print("Starting Zoho WorkDrive file export process...")
    if not get_new_access_token():
        print("Failed to obtain access token. Aborting.")
        return

    print(f"\nFiles will be exported to: {os.path.abspath(EXPORT_FOLDER)}")
    # Call the function to list files from the specified folder
    files_to_process = list_files_in_folder(FOLDER_ID_TO_ACCESS)

    if not files_to_process:
        print(f"\nNo files found in folder '{FOLDER_ID_TO_ACCESS}' or error during listing. Nothing to export.")
        return

    print(f"\nFound {len(files_to_process)} total files in folder '{FOLDER_ID_TO_ACCESS}'. Starting export...\n")
    exported_count = 0
    skipped_count = 0

    for file_data in files_to_process:
        attributes = file_data.get('attributes', {})
        file_id = file_data.get('id')
        file_name = attributes.get('name')
        mime_type = attributes.get('mime_type', '')

        if not file_id or not file_name:
            print(f"Skipping an item due to missing ID or name: {file_data}")
            skipped_count +=1
            continue
        
        file_name = "".join(c if c.isalnum() or c in (' ', '.', '_', '-') else '_' for c in file_name).rstrip()

        if 'zoho.writer' in mime_type:
            export_file(file_id, file_name, 'docx')
            exported_count +=1
        elif 'zoho.sheet' in mime_type:
            export_file(file_id, file_name, 'xlsx')
            exported_count +=1
        elif 'zoho.show' in mime_type:
            export_file(file_id, file_name, 'pptx')
            exported_count +=1
        else:
            print(f"Skipping non-Zoho proprietary file (or unsupported type): {file_name} ({mime_type})")
            skipped_count +=1
        print("-" * 30)

    print("\n--- Export Process Summary ---")
    print(f"Successfully attempted to export: {exported_count} files from folder '{FOLDER_ID_TO_ACCESS}'.")
    print(f"Skipped: {skipped_count} files.")
    print(f"Check the '{EXPORT_FOLDER}' directory.")
    print("------------------------------")

if __name__ == "__main__":
    main()