import  jwt
import time
from typing import Dict, Any
from  .config import Config


# ========================
# Helper Functions
# ========================
def create_token(payload: Dict[str, Any], expiry_hours: int = Config.TOKEN_EXPIRY_HOURS) -> str:
    """
    Create JWT token
    """
    payload_with_exp = {
        **payload,
        "exp": time.time() + (expiry_hours * 3600),
        "iat": time.time()
    }
    return jwt.encode(payload_with_exp, Config.JWT_SECRET, algorithm=Config.JWT_ALGORITHM)

def verify_token(token: str) -> Dict[str, Any]:
    """
    Verify JWT token
    """
    try:
        payload = jwt.decode(token, Config.JWT_SECRET, algorithms=[Config.JWT_ALGORITHM])
        return {"valid": True, "payload": payload}
    
    except jwt.ExpiredSignatureError:
        return {"valid": False, "reason": "Token expired"}
    
    except jwt.InvalidTokenError:
        return {"valid": False, "reason": "Invalid token"}
    
# def verify_token(token:str,credentials_exception):
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         email: str = payload.get("sub")
#         if email is None:
#             raise credentials_exception
#         token_data = schemas.TokenData(email=email)
#     except JWTError:
#         raise credentials_exception    