import pdfplumber
import re
import os
from datetime import datetime
from typing import IO

def clean_doubled_text(text: str) -> str:
    if not text:
        return ""
    return "".join([char for i, char in enumerate(text) if i % 2 == 0])

def parse_strategy_clean_layout(full_text: str) -> dict:
    data = {}
    search_text = full_text

    train_match = re.search(r"ПОЕЗД\s+ВАГОН\s+МЕСТО\n(\S+)\s+(\S+)\s+(\S+)", search_text)
    if not train_match:
        return {}
    data["train_number"] = train_match.group(1)
    data["wagon_number"] = train_match.group(2)
    data["seat_number"] = train_match.group(3)

    dep_match = re.search(r"(\d{2}:\d{2}\s+\d{2}\.\d{2}\.\d{4})([\s\S]+?)Посадка в поезд", search_text)
    if dep_match:
        datetime_str, block_text = dep_match.groups()
        try:
            data["departure_datetime"] = datetime.strptime(datetime_str, "%H:%M %d.%m.%Y")
            search_text = search_text.replace(dep_match.group(0), "", 1)
        except ValueError:
            pass
        station_parts = re.findall(r"\n([А-ЯЁ\d\s\-\.]+[А-ЯЁ\d])\n", block_text)
        if station_parts:
            full_station = " ".join([part.strip() for part in station_parts])
            data["departure_station"] = " ".join(full_station.split())

    arr_match = re.search(r"(\d{2}:\d{2}\s+\d{2}\.\d{2}\.\d{4})([\s\S]+?)(?:Возврат онлайн|Часовой пояс)", search_text)
    if arr_match:
        datetime_str, block_text = arr_match.groups()
        try:
            data["arrival_datetime"] = datetime.strptime(datetime_str, "%H:%M %d.%m.%Y")
        except ValueError:
            pass
        station_parts = re.findall(r"\n([А-ЯЁ\d\s\-\.]+[А-ЯЁ\d])\n", block_text)
        if station_parts:
            full_station = " ".join([part.strip() for part in station_parts])
            data["arrival_station"] = " ".join(full_station.split())

    passenger_match = re.search(r"Отправление\n([А-ЯЁ\s\.]+)", full_text)
    if passenger_match:
        data["passenger_name"] = passenger_match.group(1).strip()

    return data

def parse_strategy_garbled_layout(full_text: str) -> dict:
    data = {}
    search_text = full_text

    block_match = re.search(
        r"(\d{3}[А-Я]{1,2})\n"
        r"(\d{2}\.\d{2}\.\d{4})\n"
        r"(\d{2}:\d{2})\n"
        r"([\w\d]+)\n"
        r"(\d+)\n"
        r"(.+?)\n-\n"
        r"(.+?)\n",
        search_text
    )
    if not block_match:
        return {}

    data["train_number"] = block_match.group(1)
    dep_date, dep_time = block_match.group(2), block_match.group(3)
    data["wagon_number"] = block_match.group(4)
    data["seat_number"] = block_match.group(5)
    data["departure_station"] = block_match.group(6).strip()
    data["arrival_station"] = block_match.group(7).strip()
    try:
        data["departure_datetime"] = datetime.strptime(f"{dep_date} {dep_time}", "%d.%m.%Y %H:%M")
        search_text = search_text.replace(block_match.group(0), "", 1)
    except ValueError:
        pass

    all_possible_times = re.findall(r"(\d{4}::\d{4})\s+(\d{2}\.\d{2}\.\d{4})", search_text)
    for garbled_time, date_str in all_possible_times:
        time_str = clean_doubled_text(garbled_time)
        try:
            candidate_dt = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
            if candidate_dt != data.get("departure_datetime"):
                data["arrival_datetime"] = candidate_dt
                break
        except (ValueError, KeyError):
            continue

    cleaned_full_text = clean_doubled_text(full_text)
    name_match = re.search(r"([А-ЯЁ]{2,}\s[А-ЯЁ]+\s[А-ЯЁ]+)", cleaned_full_text)
    if name_match:
        data["passenger_name"] = name_match.group(1)

    return data

def parse_rzd_ticket(file_obj: IO[bytes]) -> dict:
    full_text = ""
    try:
        with pdfplumber.open(file_obj) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=1, y_tolerance=1)
                if page_text:
                    full_text += page_text + "\n"
    except Exception:
        return {}

    if "::" in full_text and "Отправление по" in full_text:
        parsed_data = parse_strategy_garbled_layout(full_text)
    else:
        parsed_data = parse_strategy_clean_layout(full_text)

    final_data = {
        "passenger_name": parsed_data.get("passenger_name"),
        "train_number": parsed_data.get("train_number"),
        "wagon_number": parsed_data.get("wagon_number"),
        "seat_number": parsed_data.get("seat_number"),
        "departure_station": parsed_data.get("departure_station"),
        "departure_datetime": parsed_data.get("departure_datetime"),
        "arrival_station": parsed_data.get("arrival_station"),
        "arrival_datetime": parsed_data.get("arrival_datetime"),
    }
    return final_data