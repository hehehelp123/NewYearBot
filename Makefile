.PHONY: restart-all restart-apps stop logs

restart-all:
	docker compose down --remove-orphans
	docker compose up -d --build

restart:
	docker compose up -d --build --no-deps tg_bot_service orchestrator_service user_service wishlist_service ticket_service notification_service music_uploader_service

stop:
	docker compose down

logs:
	docker compose logs -f

restart-hard:
	docker compose down -v --remove-orphans
	docker compose up -d --build
