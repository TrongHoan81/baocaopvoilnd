from google_auth_oauthlib.flow import InstalledAppFlow
import json, datetime as dt

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)
creds = flow.run_local_server(port=0, access_type="offline", include_granted_scopes="true", prompt="consent")

with open("token.json", "w", encoding="utf-8") as f:
    json.dump({
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "expiry": creds.expiry.astimezone().isoformat(),
        "scopes": list(creds.scopes),
        "client_secret": creds.client_secret,
        "client_id": creds.client_id,
        "token_uri": creds.token_uri,
    }, f, ensure_ascii=False, indent=2, default=str)

print("OK: đã tạo token.json. refresh_token =", bool(creds.refresh_token))
