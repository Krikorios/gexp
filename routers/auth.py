from fastapi import APIRouter, Depends, Request, Form, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from services.auth_service import verify_password, get_user_by_username, create_user, delete_user, get_all_users
from services.backup_service import create_backup
import secrets
import os

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Simple memory store for sessions for this requirement. (In prod we use Redis/Cookie etc., but cookie + session dict is quickest without external deps like itsdangerous if not in requirements)
sessions = {}

def get_current_user_from_request(request: Request):
    session_id = request.cookies.get("session_id")
    if session_id and session_id in sessions:
        return sessions[session_id]
    return None

def get_current_user(request: Request):
    user = get_current_user_from_request(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/auth/login"},
        )
    return user

@router.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})

@router.post("/login", response_class=HTMLResponse)
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    user = get_user_by_username(username)
    if not user or not verify_password(user["password_hash"], password):
        return templates.TemplateResponse(request=request, name="login.html", context={"error": "Invalid username or password"})

    
    session_id = secrets.token_urlsafe(32)
    sessions[session_id] = dict(user)
    
    from config import ENVIRONMENT
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        secure=ENVIRONMENT == "production",
        samesite="lax",
        max_age=86400,  # 24 hours
    )
    return response

@router.get("/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/auth/login")
    session_id = request.cookies.get("session_id")
    if session_id in sessions:
        del sessions[session_id]
    response.delete_cookie("session_id")
    return response

@router.get("/users", response_class=HTMLResponse)
async def users_list(request: Request, _=Depends(get_current_user)):
    users = get_all_users()
    return templates.TemplateResponse(request=request, name="users.html", context={"users": users})

@router.post("/users/create")
async def add_user(username: str = Form(...), password: str = Form(...), _=Depends(get_current_user)):
    try:
        create_user(username, password)
    except Exception:
        pass # Probably duplicate
    return RedirectResponse(url="/auth/users", status_code=status.HTTP_303_SEE_OTHER)

@router.post("/users/delete/{user_id}")
async def remove_user(user_id: int, _=Depends(get_current_user)):
    delete_user(user_id)
    return RedirectResponse(url="/auth/users", status_code=status.HTTP_303_SEE_OTHER)

@router.get("/backup")
async def backup_db(request: Request, _=Depends(get_current_user)):
    try:
        backup_path = create_backup()
        return FileResponse(backup_path, media_type="application/octet-stream", filename=os.path.basename(backup_path))
    except Exception as e:
        msg = f"Backup failed: {str(e)}"
        return templates.TemplateResponse(request=request, name="users.html", context={
            "users": get_all_users(),
            "backup_msg": msg
        })
