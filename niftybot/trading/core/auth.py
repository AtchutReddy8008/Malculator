"""
Zerodha authentication functions using DB-stored credentials
Automatic version: tries full login (password + TOTP) when needed
No manual request_token paste required — runs after saving credentials
"""

import requests
import pyotp
import time
import traceback
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from typing import Optional
from django.utils import timezone


def generate_and_set_access_token_db(
    kite: KiteConnect,
    broker,  # Broker model instance (has api_key, secret_key, zerodha_user_id, password, totp)
    max_attempts: int = 3,
    attempt_delay: float = 15.0
) -> Optional[str]:
    """
    Automatically generate access token using stored credentials.
    Returns access token string or None on failure.
    """
    attempt = 0
    
    while attempt < max_attempts:
        attempt += 1
        print(f"[INFO] Auto login attempt {attempt}/{max_attempts} for user {broker.zerodha_user_id}")
        
        try:
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
            
            # Step 1: Get login page
            login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={broker.api_key}"
            resp = session.get(login_url, timeout=12)
            if resp.status_code != 200:
                raise Exception(f"Login page GET failed: {resp.status_code} - {resp.reason}")
            
            current_url = resp.url
            
            # Step 2: Submit userid + password
            login_resp = session.post(
                "https://kite.zerodha.com/api/login",
                data={"user_id": broker.zerodha_user_id, "password": broker.password},
                timeout=10
            )
            login_data = login_resp.json()
            
            if login_data.get("status") != "success":
                error_msg = login_data.get("message", "Unknown login error")
                raise Exception(f"Login failed: {error_msg}")
            
            request_id = login_data["data"]["request_id"]
            print(f"[INFO] Password login successful | request_id: {request_id}")
            
            # Step 3: Submit TOTP (with retry for clock drift)
            totp_attempts = 0
            while totp_attempts < 3:
                totp_code = pyotp.TOTP(broker.totp).now()
                print(f"[INFO] Generated TOTP: {totp_code} (attempt {totp_attempts+1})")
                
                twofa_resp = session.post(
                    "https://kite.zerodha.com/api/twofa",
                    data={
                        "user_id": broker.zerodha_user_id,
                        "request_id": request_id,
                        "twofa_value": totp_code,
                        "twofa_type": "totp"
                    },
                    timeout=10
                )
                
                twofa_data = twofa_resp.json()
                if twofa_data.get("status") == "success":
                    print("[INFO] 2FA successful")
                    break
                
                error = twofa_data.get("message", "Unknown 2FA error")
                if "invalid" in error.lower() or "expired" in error.lower():
                    print(f"[WARNING] TOTP rejected: {error} - retrying in 3s...")
                    time.sleep(3)
                    totp_attempts += 1
                else:
                    raise Exception(f"2FA failed: {error}")
            
            else:
                raise Exception("All TOTP attempts failed - check clock sync or secret")
            
            # Step 4: Follow redirect to get request_token
            redirect_resp = session.get(
                current_url + "&skip_session=true",
                allow_redirects=True,
                timeout=15
            )
            
            final_url = redirect_resp.url
            parsed = urlparse(final_url)
            query_params = parse_qs(parsed.query)
            request_token = query_params.get("request_token", [None])[0]
            
            if not request_token:
                raise Exception(f"Could not extract request_token from redirect URL: {final_url}")
            
            print(f"[INFO] Request token extracted successfully: {request_token[:10]}...")
            
            # Step 5: Generate session (final step)
            data = kite.generate_session(
                request_token=request_token,
                api_secret=broker.secret_key
            )
            
            access_token = data.get("access_token")
            if not access_token:
                raise Exception("generate_session did not return access_token")
            
            kite.set_access_token(access_token)
            print(f"[SUCCESS] Access token generated and set (prefix: {access_token[:8]}...)")
            
            # Save to DB
            broker.access_token = access_token
            broker.token_generated_at = timezone.now()
            broker.save(update_fields=['access_token', 'token_generated_at'])
            
            return access_token
            
        except Exception as e:
            error_msg = f"Login attempt {attempt} failed: {str(e)}\n{traceback.format_exc(limit=3)}"
            print(error_msg)
            
            if attempt < max_attempts:
                print(f"[INFO] Retrying login in {attempt_delay} seconds...")
                time.sleep(attempt_delay)
            else:
                print("[CRITICAL] All login attempts failed - giving up")
                return None

    return None
