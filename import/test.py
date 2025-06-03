import requests

# ==== CONFIGURATION ====
CLIENT_ID = "1000.JQW928OBFSCUD6DF27H6NT6BJRY2BO"
CLIENT_SECRET = "dbfe37038ecd69d73a612c8e8f39562dc9aa96f105"
REFRESH_TOKEN = "1000.b49d341d01656bacb164702d210e2069.1ea10b8ba49b3cc1f941548f21aa1b4d"
API_BASE_URL = "https://www.zohoapis.com"

def refresh_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        token_info = response.json()
        print("✅ Access token refreshed.")
        return token_info["access_token"]
    else:
        print("❌ Failed to refresh token:", response.text)
        return None

def get_user_profile(access_token):
    url = f"{API_BASE_URL}/workdrive/api/v1/users/me"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    print("Status code:", response.status_code)
    print("Response:", response.text)

def main():
    access_token = refresh_access_token()
    if not access_token:
        return
    get_user_profile(access_token)

if __name__ == "__main__":
    main()
