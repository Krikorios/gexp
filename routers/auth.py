from fastapi import APIRouter, Depends, Request, Form, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from services.auth_service import (
    verify_password, get_user_by_username, create_user, delete_user, get_all_users,
    create_session, get_session, delete_session, SESSION_TTL_SECONDS,
)
from services.backup_service import create_backup
import os

router = APIRouter()
templates = Jinja2Templates(directory="templates")


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

@router.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})

@router.post("/login", response_class=HTMLResponse)
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    user = get_user_by_username(username)
    if not user or not verify_password(user["password_hash"], password):
        return templates.TemplateResponse(request=request, name="login.html", context={"error": "Invalid username or password"})

    session_id = create_session(user["id"], user["username"])

    from config import ENVIRONMENT
    # secure=True only when accessed via HTTPS (check X-Forwarded-Proto from nginx)
    is_https = ENVIRONMENT == "production" and request.headers.get("x-forwarded-proto") == "https"
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        secure=is_https,
        samesite="lax",
        max_age=SESSION_TTL_SECONDS,
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
async def users_list(request: Request, _=Depends(get_current_user)):
    users = get_all_users()
    return templates.TemplateResponse(request=request, name="users.html", context={"users": users})

@router.post("/users/create")
async def add_user(request: Request, username: str = Form(...), password: str = Form(...), _=Depends(get_current_user)):
    if len(password) < 8:
        users = get_all_users()
        return templates.TemplateResponse(request=request, name="users.html", context={"users": users, "error": "Password must be at least 8 characters."})
    if get_user_by_username(username):
        users = get_all_users()
        return templates.TemplateResponse(request=request, name="users.html", context={"users": users, "error": f"User '{username}' already exists."})
    create_user(username, password)
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
