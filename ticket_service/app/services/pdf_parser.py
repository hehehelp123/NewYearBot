"""Парсинг PDF-билетов: РЖД (электронный билет / контрольный купон) и авиа (маршрутная квитанция).

Все даты возвращаются как naive datetime в МЕСТНОМ времени из билета;
смещение от UTC отдаётся отдельно (meta["utc_offset"]), чтобы планировщик
уведомлений мог перевести их в UTC.
"""
import logging
import re
from datetime import datetime, timedelta
from typing import IO, Optional

import pdfplumber

logger = logging.getLogger(__name__)

DEFAULT_UTC_OFFSET = 3  # МСК: и РЖД, и большинство авиабилетов по России

FIELDS = (
    "passenger_name", "train_number", "wagon_number", "seat_number",
    "departure_station", "departure_datetime", "arrival_station", "arrival_datetime",
)

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "янв": 1, "фев": 2, "мар": 3, "апр": 4, "мая": 5, "май": 5, "июн": 6,
    "июл": 7, "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
}

NAME_STOPWORDS = (
    "ПАСПОРТ", "БИЛЕТ", "ПОСАДК", "ДОКУМЕНТ", "РАЗРЕШЕН", "ВАГОН", "ПОЕЗД", "КУПОН",
    "ПЛАТФОРМ", "СВИДЕТЕЛЬСТВО", "ПРОВОЗ", "НУМЕРАЦИЯ",
)


# ---------------------------------------------------------------- text helpers

def _page_text(page, **crop) -> str:
    # dedupe_chars убирает «жирный» текст, который в PDF РЖД нарисован дважды
    # (из-за него раньше получалось «ТТААННЫЫГГИИННАА»).
    p = page.dedupe_chars()
    if crop:
        p = p.crop((crop["x0"] * p.width, crop["top"] * p.height,
                    crop["x1"] * p.width, crop["bottom"] * p.height))
    return p.extract_text(x_tolerance=1.5, y_tolerance=2) or ""


def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = " ".join(s.split()).strip(" ,-")
    return s or None


def _dt(date_str: str, time_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
    except ValueError:
        return None


# ---------------------------------------------------------------- РЖД

def _parse_rzd(page, text: str) -> dict:
    data: dict = {}

    m = re.search(r"ПОЕЗД\s+ВАГОН\s+МЕСТО\s*\n\s*(\S+)\s+(\S+)\s+(\S+)", text)
    if m:
        data["train_number"], data["wagon_number"], data["seat_number"] = m.groups()

    # Служебная строка сбоку купона: 347АА 27.12.2025 17:45 11П 026 СТАНЦИЯ - СТАНЦИЯ ПН... ФАМИЛИЯ-ИО
    svc = re.search(
        r"(?m)^(\d{3}[А-ЯЁA-Z]{0,2})\s+(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2})\s+(\S+)\s+(\S+)\s+"
        r"([\s\S]+?)\s+-\s+([\s\S]+?)\s+(?:[А-ЯЁA-Z]{2}\d{5,}|\d{6,})\s+([А-ЯЁA-Z]+-[А-ЯЁA-Z]{1,2})?",
        text,
    )
    if svc:
        full_train, d, t, wagon, seat, dep_st, arr_st, short_name = svc.groups()
        if not data.get("train_number") or full_train.startswith(data["train_number"]):
            data["train_number"] = full_train
        data.setdefault("wagon_number", re.sub(r"\D", "", wagon) or wagon)
        data.setdefault("seat_number", seat)
        data["departure_station"] = _clean(dep_st)
        data["arrival_station"] = _clean(arr_st)
        data["departure_datetime"] = _dt(d, t)
        if short_name:
            data["passenger_name"] = short_name

    # «17:45 27.12.2025» — первое вхождение отправление, второе прибытие
    pairs = re.findall(r"(\d{2}:\d{2})\s+(\d{2}\.\d{2}\.\d{4})", text)
    if pairs:
        data["departure_datetime"] = data.get("departure_datetime") or _dt(pairs[0][1], pairs[0][0])
    if len(pairs) > 1:
        data["arrival_datetime"] = _dt(pairs[1][1], pairs[1][0])

    # Полное ФИО — крупная строка в правой верхней части купона
    right = _page_text(page, x0=0.5, top=0.0, x1=0.95, bottom=0.3)
    for line in right.splitlines():
        line = line.strip()
        if re.fullmatch(r"[А-ЯЁ\-]{2,}(?: [А-ЯЁ\-]{2,}){1,3}", line) and not any(w in line for w in NAME_STOPWORDS):
            data["passenger_name"] = line
            break

    # Станции по-человечески (слева, между временем отправления и прибытия), если служебной строки нет
    if not data.get("departure_station"):
        left = _page_text(page, x0=0.0, top=0.0, x1=0.5, bottom=0.45)
        blocks = re.split(r"\d{2}:\d{2}\s+\d{2}\.\d{2}\.\d{4}.*\n", left)
        if len(blocks) >= 3:
            def station(block):
                m2 = re.search(r"(?:Отправление|Прибытие) по\s+(.+?)\n", block)
                return _clean(m2.group(1)) if m2 else None
            data["departure_station"] = station(blocks[1])
            data["arrival_station"] = station(blocks[2])

    return data


# ---------------------------------------------------------------- авиа

FLIGHT_RE = re.compile(r"\b((?:SU|S7|U6|DP|FV|UT|N4|5N|WZ|ZF|YC|A4|EO|I8|R3|[A-Z][A-Z0-9]))\s?(\d{3,4})\b")
DATE_TXT_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-zА-Яа-яё]{3,})\.?\s+(\d{4})\b")
DATE_NUM_RE = re.compile(r"\b(\d{2})\.(\d{2})\.(\d{4})\b")


def _find_dates(text: str) -> list[tuple[int, datetime]]:
    found = []
    for m in DATE_TXT_RE.finditer(text):
        mon = MONTHS.get(m.group(2)[:3].lower())
        if mon:
            try:
                found.append((m.start(), datetime(int(m.group(3)), mon, int(m.group(1)))))
            except ValueError:
                pass
    for m in DATE_NUM_RE.finditer(text):
        try:
            found.append((m.start(), datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))))
        except ValueError:
            pass
    return sorted(found)


def _parse_flight(text: str) -> dict:
    data: dict = {}
    fm = None
    anchor = re.search(r"Flight|Рейс", text)
    for m in FLIGHT_RE.finditer(text):
        # первый номер рейса рядом с «Flight/Рейс» (до или после — вёрстка бывает разной)
        if anchor is None or abs(m.start() - anchor.start()) < 200:
            fm = m
            break
    if not fm:
        return {}
    data["train_number"] = f"{fm.group(1)} {fm.group(2)}"

    # Города: строка прямо перед номером рейса, вида «Apatity (Kirovsk) Moscow» / «Москва — Сочи»
    prefix = re.sub(r"(?:Flight|Рейс)\s*:?\s*$", "", text[:fm.start()].rstrip(), flags=re.I).rstrip()
    before = prefix.splitlines()
    if before:
        route = before[-1].strip()
        parts = re.split(r"\s*(?:→|—|–|✈| - )\s*|(?<=\))\s+", route, maxsplit=1)
        if len(parts) == 2:
            data["departure_station"], data["arrival_station"] = _clean(parts[0]), _clean(parts[1])

    tail = text[fm.start():]
    dates = [d for _, d in _find_dates(tail)]
    times = re.findall(r"(?<![\d:])(\d{2}:\d{2})(?![\d:])", tail)
    if dates and times:
        dep_date = dates[0]
        arr_date = dates[1] if len(dates) > 1 else dates[0]
        dh, dmin = map(int, times[0].split(":"))
        dep = dep_date.replace(hour=dh, minute=dmin)
        data["departure_datetime"] = dep
        if len(times) > 1:
            ah, amin = map(int, times[1].split(":"))
            arr = arr_date.replace(hour=ah, minute=amin)
            if arr < dep:
                arr += timedelta(days=1)
            data["arrival_datetime"] = arr

    # IATA-коды аэропортов: первая строка после времени с двумя трёхбуквенными кодами
    iata = re.search(r"\b([A-Z]{3})\b[^\n]*?\b([A-Z]{3})\b", tail[tail.find(times[0]) if times else 0:])
    if iata and iata.group(1) != iata.group(2):
        dep_code, arr_code = iata.groups()
        data["departure_station"] = f"{data['departure_station']} ({dep_code})" if data.get("departure_station") else dep_code
        data["arrival_station"] = f"{data['arrival_station']} ({arr_code})" if data.get("arrival_station") else arr_code

    nm = re.search(r"(?m)^([A-Z]{2,}(?: [A-Z]{2,}){1,3}?)(?: (?:MR|MRS|MS|MISS|MSTR|CHD))?$", text)
    if nm:
        data["passenger_name"] = nm.group(1)
    else:
        nm = re.search(r"(?m)^([А-ЯЁ]{2,}(?: [А-ЯЁ]{2,}){1,2})$", text)
        if nm:
            data["passenger_name"] = nm.group(1)

    seat = re.search(r"(?:Seat|Место)[:\s]+(\d{1,2}[A-KА-К])\b", text)
    if seat:
        data["seat_number"] = seat.group(1)
    return data


# ---------------------------------------------------------------- generic fallback

def _parse_generic(text: str) -> dict:
    """Хоть какая-то дата/время, чтобы напоминания всё равно сработали."""
    m = re.search(r"(\d{2}\.\d{2}\.\d{4})\D{0,5}(\d{2}:\d{2})", text) or \
        re.search(r"(\d{2}:\d{2})\D{0,5}(\d{2}\.\d{2}\.\d{4})", text)
    if not m:
        return {}
    a, b = m.groups()
    dt = _dt(a, b) if "." in a else _dt(b, a)
    return {"departure_datetime": dt} if dt else {}


# ---------------------------------------------------------------- public API

def parse_ticket(file_obj: IO[bytes]) -> tuple[dict, dict]:
    """Возвращает (поля для модели Ticket, meta) где meta = {transport, utc_offset}."""
    meta = {"transport": None, "utc_offset": DEFAULT_UTC_OFFSET}
    data: dict = {}
    try:
        with pdfplumber.open(file_obj) as pdf:
            if not pdf.pages:
                return {k: None for k in FIELDS}, meta
            page = pdf.pages[0]
            text = _page_text(page)
            full = "\n".join(_page_text(p) for p in pdf.pages[:3])

            if re.search(r"ПОЕЗД\s+ВАГОН\s+МЕСТО|КОНТРОЛЬНЫЙ КУПОН|ЭЛЕКТРОННЫЙ БИЛЕТ", text):
                meta["transport"] = "train"
                data = _parse_rzd(page, text)
            elif re.search(r"Flight|Рейс|itinerary|Маршрутная квитанция|Boarding|Посадочный талон", full, re.I):
                data = _parse_flight(full)
                if data:
                    meta["transport"] = "flight"
            if not data.get("departure_datetime"):
                data = {**_parse_generic(full), **data} if not data else {**data, **_parse_generic(full)}

            off = re.search(r"UTC\s?\+\s?(\d{1,2})", text)
            if off:
                meta["utc_offset"] = int(off.group(1))
    except Exception as e:
        logger.error(f"Ticket PDF parsing failed: {e}", exc_info=True)

    return {k: data.get(k) for k in FIELDS}, meta


def parse_rzd_ticket(file_obj: IO[bytes]) -> dict:
    """Обратная совместимость: только поля модели."""
    return parse_ticket(file_obj)[0]
