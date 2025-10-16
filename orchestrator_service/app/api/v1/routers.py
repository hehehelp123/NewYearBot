import uuid
import io
from fastapi import APIRouter, Request, Form, UploadFile, File, HTTPException, status, Body
from app.kafka.producer import kafka_producer
from app.services.storage_service import storage_service

router = APIRouter()

@router.get("/menu")
def get_menu(request: Request):
    return request.app.state.menu_tree

@router.post("/tickets")
async def create_ticket_proxy(
    telegram_id: int = Form(...),
    title: str = Form(...),
    file: UploadFile = File(...)
):
    file_content = await file.read()
    file_stream = io.BytesIO(file_content)
    storage_key = f"tickets/{uuid.uuid4()}_{file.filename}"

    success = storage_service.upload_file_obj(
        object_name=storage_key,
        file_data=file_stream,
        file_len=len(file_content),
        content_type=file.content_type
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not upload file to storage."
        )

    kafka_payload = {
        "telegram_id": telegram_id,
        "title": title,
        "storage_key": storage_key,
        "original_filename": file.filename
    }
    await kafka_producer.send("ticket.create.requested", kafka_payload)

    return {"status": "accepted", "detail": "Ticket creation request has been accepted."}

@router.post("/commands/{command_path}")
async def handle_command(request: Request, command_path: str, payload: dict = Body(...)):
    command_map = request.app.state.command_map
    topic = command_map.get(command_path)

    if not topic:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Command not found")

    await kafka_producer.send(topic, payload)
    return {"status": "accepted"}


@router.get("/")
def read_root():
    return {"service": "Orchestrator Service", "status": "ok"}