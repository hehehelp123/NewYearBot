from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def read_root():
    return {"service": "Notification Service", "status": "ok"}