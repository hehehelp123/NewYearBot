import uuid
import io
import math
import random
from fastapi import APIRouter, Request, Form, UploadFile, File, HTTPException, status, Body, Query
from app.kafka.producer import kafka_producer
from app.services.storage_service import storage_service
from app.services.menu_service import menu_service

router = APIRouter()


@router.get("/menu")
async def get_menu(request: Request):
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
    return {"status": "command_accepted", "topic": topic}


@router.get("/albums/years")
async def get_album_years():
    return storage_service.list_folders("photos/")


@router.get("/albums/{year}")
async def get_album_media(
        year: int,
        page: int = Query(1, ge=1),
        page_size: int = Query(1, ge=1, le=5)
):
    folder = f"photos/{year}/"
    all_media = storage_service.list_media(folder)

    total_items = len(all_media)
    if total_items == 0:
        return {"items": [], "total_items": 0, "total_pages": 0, "page": 0}

    total_pages = math.ceil(total_items / page_size)

    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    paginated_items = all_media[start_index:end_index]

    return {
        "items": paginated_items,
        "total_items": total_items,
        "total_pages": total_pages,
        "page": page
    }


@router.get("/albums/{year}/random")
async def get_random_album_media(year: int):
    folder = f"photos/{year}/"
    all_media = storage_service.list_media(folder)

    total_items = len(all_media)
    if total_items == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No media found in this album.")

    random_index = random.randint(0, total_items - 1)

    return {
        "page": random_index + 1
    }