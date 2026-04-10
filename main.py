from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from config import UPLOAD_DIR, ENVIRONMENT
from database.schema import create_tables
from routers import documents, review, search, upload, auth

# Disable docs/openapi in production
if ENVIRONMENT == "production":
    app = FastAPI(title="Lebanese Real Estate Registry", docs_url=None, redoc_url=None, openapi_url=None)
else:
    app = FastAPI(title="Lebanese Real Estate Registry")

@app.middleware("http")
async def check_authentication(request: Request, call_next):
    path = request.url.path
    allowed_paths = ["/auth/login", "/static"]
    is_allowed = any(path.startswith(p) for p in allowed_paths)
    
    if not is_allowed:
        session_id = request.cookies.get("session_id")
        if not session_id or session_id not in auth.sessions:
            return RedirectResponse(url="/auth/login", status_code=303)
            
    response = await call_next(request)
    return response


@app.on_event("startup")
async def startup():
    Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path("data").mkdir(exist_ok=True)
    create_tables()
    
    # Do NOT create default admin user automatically in production!
    # A script should be used for initial setup.


app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")
app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(upload.router)
app.include_router(review.router)
app.include_router(search.router)
app.include_router(documents.router)
app.include_router(auth.router, prefix="/auth")
