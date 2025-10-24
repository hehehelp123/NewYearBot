from fastapi import APIRouter

router = APIRouter()

@router.get("/features")
def get_user_service_features():
    return []

@router.get("/")
def read_root():
    return {"service": "User Service", "status": "ok"}