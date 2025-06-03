import requests

url = "https://accounts.zoho.com/oauth/v2/token"

data = {
    "grant_type": "authorization_code",
    "client_id": "1000.JQW928OBFSCUD6DF27H6NT6BJRY2BO",
    "client_secret": "dbfe37038ecd69d73a612c8e8f39562dc9aa96f105",
    "redirect_uri": "http://localhost",
    "code": "1000.b9964c3e7b72ba1cb460853163cf0197.9340e93dbf61f0c26f1a342a8f0ac3b1"
}

response = requests.post(url, data=data)
print(response.json())
