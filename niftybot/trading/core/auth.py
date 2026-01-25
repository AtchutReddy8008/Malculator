"""
Zerodha authentication functions using DB-stored credentials
"""
import requests
import pyotp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect

def generate_and_set_access_token_db(kite: KiteConnect, api_key: str, secret_key: str, 
                                     zerodha_user_id: str, password: str, totp_secret: str) -> str:
    """
    Generate access token using Zerodha's API with DB-stored credentials
    Returns access token string or empty string on failure
    """
    try:
        session = requests.Session()
        login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
        resp = session.get(login_url, timeout=15)
        if resp.status_code != 200:
            raise Exception(f"Login page failed: {resp.status_code}")
        
        current_url = resp.url
        login_resp = session.post("https://kite.zerodha.com/api/login",
                                  data={"user_id": zerodha_user_id, "password": password}, timeout=15)
        login_data = login_resp.json()
        if login_data.get("status") != "success":
            raise Exception(f"Login failed: {login_data}")
        
        request_id = login_data["data"]["request_id"]
        
        # Generate TOTP
        totp_code = pyotp.TOTP(totp_secret).now()
        
        twofa_resp = session.post("https://kite.zerodha.com/api/twofa",
                                  data={"user_id": zerodha_user_id, "request_id": request_id,
                                        "twofa_value": totp_code, "twofa_type": "totp"}, timeout=15)
        if twofa_resp.json().get("status") != "success":
            raise Exception("2FA failed")
        
        redirect_resp = session.get(current_url + "&skip_session=true", allow_redirects=True, timeout=15)
        request_token = parse_qs(urlparse(redirect_resp.url).query)["request_token"][0]
        
        data = kite.generate_session(request_token=request_token, api_secret=secret_key)
        access_token = data["access_token"]
        
        return access_token
        
    except Exception as e:
        raise Exception(f"Token generation failed: {str(e)}")