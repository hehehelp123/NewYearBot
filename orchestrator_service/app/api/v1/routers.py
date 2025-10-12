from fastapi import APIRouter, Request, HTTPException, status
from app.kafka.producer import kafka_producer

router = APIRouter()


@router.get("/menu")
def get_menu(request: Request):
    return request.app.state.menu_tree


@router.post("/commands/{command_path}")
async def handle_command(command_path: str, payload: dict, request: Request):
    command_map = request.app.state.command_map
    topic = command_map.get(command_path)

    if not topic:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Command not found")

    await kafka_producer.send(topic, payload)

    return {"status": "accepted", "detail": f"Command for topic '{topic}' has been accepted."}


@router.get("/")
def read_root():
    return {"service": "Orchestrator Service", "status": "ok"}