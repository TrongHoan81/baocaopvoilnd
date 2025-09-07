# security.py
import os
from functools import wraps
from flask import request, abort

def require_internal_api_key():
    """
    Decorator bảo vệ API nội bộ bằng API key.
    - Đọc khóa từ biến môi trường VPS_INTERNAL_API_KEY.
    - Client phải gửi header: X-Internal-Api-Key: <key>
    """
    expected = os.getenv("VPS_INTERNAL_API_KEY", "").strip()

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            provided = request.headers.get("X-Internal-Api-Key", "").strip()
            if not expected or provided != expected:
                abort(401, description="Unauthorized")
            return fn(*args, **kwargs)
        return wrapper
    return decorator
