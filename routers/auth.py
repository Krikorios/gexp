from fastapi import APIRouter, Depends, Request, Form, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from services.auth_service import (
    verify_password, get_user_by_username, create_user, delete_user, get_all_users,
    create_session, get_session, delete_session, SESSION_TTL_SECONDS,
    needs_rehash, update_user_password, set_user_role, count_admins,
    record_login_attempt, is_login_blocked,
)
from services.backup_service import create_backup
import os

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _client_ip(request: Request) -> str:
    # Trust X-Forwarded-For when behind a reverse proxy
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_current_user_from_request(request: Request):
    session_id = request.cookies.get("session_id")
    return get_session(session_id) if session_id else None


def get_current_user(request: Request):
    user = get_current_user_from_request(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/auth/login"},
        )
    return user


def require_admin(request: Request):
    user = get_current_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user

@router.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})

@router.post("/login", response_class=HTMLResponse)
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    username = (username or "").strip()
    ip = _client_ip(request)

    if is_login_blocked(username, ip):
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "تم حجب محاولات تسجيل الدخول مؤقتاً. حاول بعد 15 دقيقة."},
            status_code=429,
        )

    user = get_user_by_username(username)
    if not user or not verify_password(user["password_hash"], password):
        record_login_attempt(username, ip, success=False)
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Invalid username or password"},
            status_code=401,
        )

    record_login_attempt(username, ip, success=True)

    # Opportunistically upgrade the password hash if it's using weaker params.
    try:
        if needs_rehash(user["password_hash"]):
            update_user_password(user["id"], password)
    except Exception:
        pass

    role = user["role"] if "role" in user.keys() else "user"
    session_id = create_session(user["id"], user["username"], role=role)

    from config import ENVIRONMENT
    is_https = ENVIRONMENT == "production" and request.headers.get("x-forwarded-proto") == "https"
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        secure=is_https,
        samesite="lax",
        max_age=SESSION_TTL_SECONDS,
        path="/",
    )
    return response

@router.get("/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/auth/login")
    session_id = request.cookies.get("session_id")
    if session_id:
        delete_session(session_id)
    response.delete_cookie("session_id")
    return response

@router.get("/users", response_class=HTMLResponse)
async def users_list(request: Request, current=Depends(require_admin)):
    users = get_all_users()
    return templates.TemplateResponse(
        request=request,
        name="users.html",
        context={"users": users, "current_user": current},
    )

@router.post("/users/create")
async def add_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form("user"),
    current=Depends(require_admin),
):
    username = (username or "").strip()
    if role not in {"admin", "user"}:
        role = "user"
    if len(password) < 8:
        users = get_all_users()
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={"users": users, "current_user": current, "error": "Password must be at least 8 characters."},
        )
    if get_user_by_username(username):
        users = get_all_users()
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={"users": users, "current_user": current, "error": f"User '{username}' already exists."},
        )
    create_user(username, password, role=role)
    return RedirectResponse(url="/auth/users", status_code=status.HTTP_303_SEE_OTHER)

@router.post("/users/delete/{user_id}")
async def remove_user(request: Request, user_id: int, current=Depends(require_admin)):
    if user_id == current["user_id"]:
        users = get_all_users()
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={"users": users, "current_user": current, "error": "لا يمكنك حذف حسابك الخاص."},
            status_code=400,
        )
    # Prevent removing the last admin
    target = next((u for u in get_all_users() if u["id"] == user_id), None)
    if target and target["role"] == "admin" and count_admins() <= 1:
        users = get_all_users()
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={"users": users, "current_user": current, "error": "لا يمكن حذف آخر مسؤول في النظام."},
            status_code=400,
        )
    delete_user(user_id)
    return RedirectResponse(url="/auth/users", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/users/{user_id}/role")
async def change_role(
    request: Request,
    user_id: int,
    role: str = Form(...),
    current=Depends(require_admin),
):
    if role not in {"admin", "user"}:
        raise HTTPException(status_code=400, detail="invalid role")
    # Prevent demoting the last admin
    target = next((u for u in get_all_users() if u["id"] == user_id), None)
    if target and target["role"] == "admin" and role != "admin" and count_admins() <= 1:
        users = get_all_users()
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={"users": users, "current_user": current, "error": "لا يمكن تخفيض رتبة آخر مسؤول."},
            status_code=400,
        )
    set_user_role(user_id, role)
    return RedirectResponse(url="/auth/users", status_code=status.HTTP_303_SEE_OTHER)

@router.get("/backup")
async def backup_db(request: Request, current=Depends(require_admin)):
    try:
        backup_path = create_backup()
        return FileResponse(backup_path, media_type="application/octet-stream", filename=os.path.basename(backup_path))
    except Exception as e:
        msg = f"Backup failed: {str(e)}"
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={
                "users": get_all_users(),
                "current_user": current,
                "backup_msg": msg,
            },
        )
