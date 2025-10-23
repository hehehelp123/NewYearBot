import pdfplumber
import re
from typing import Dict, Optional
from pathlib import Path

def parse_russian_train_ticket(pdf_path: str) -> Optional[Dict[str, str]]:
    """
    Извлекает информацию из PDF-файла российского железнодорожного билета.

    Args:
        pdf_path: Путь к PDF-файлу билета.

    Returns:
        Словарь с извлеченными данными или None, если файл не удалось обработать.
        Ключи словаря: 'поезд', 'вагон', 'место', 'дата_отправления',
        'время_отправления', 'время_прибытия'.
    """
    extracted_data = {}
    pdf_path = str(Path(__file__).parent.parent) + (pdf_path)
    print(pdf_path)
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Обычно билет находится на первой странице
            if not pdf.pages:
                print(f"Ошибка: В файле '{pdf_path}' не найдено страниц.")
                return None

            page = pdf.pages[0]
            text = page.extract_text()

            print(text)

            if not text:
                print(f"Ошибка: Не удалось извлечь текст из файла '{pdf_path}'. Возможно, это скан-копия.")
                return None

            # Определяем регулярные выражения для поиска данных
            # \s* - ноль или более пробельных символов
            # (\S+) - захватывает одну или более последовательностей непробельных символов (номер поезда)
            # (\d{2}) - захватывает ровно две цифры (номер вагона)
            # (\d+) - захватывает одну или более цифр (номер места)
            # (\d{2}\.\d{2}\.\d{4}) - захватывает дату в формате ДД.ММ.ГГГГ
            # (\d{2}:\d{2}) - захватывает время в формате ЧЧ:ММ
            patterns = {
                'поезд': r'Поезд\s*№\s*(\S+)',
                'вагон': r'Вагон\s+(\d{2})',
                'место': r'Место\(а\)\s+(\d+)',
                'отправление': r'Отправление\s+\d{2}\.\d{2}\.\d{4}\s+в\s+(\d{2}\.\d{2}\.\d{4})\s+в\s+(\d{2}:\d{2})',
                'прибытие': r'Прибытие.*в\s+(\d{2}:\d{2})'
            }

            # Поиск по шаблонам
            train_match = re.search(patterns['поезд'], text)
            if train_match:
                extracted_data['поезд'] = train_match.group(1)

            wagon_match = re.search(patterns['вагон'], text)
            if wagon_match:
                extracted_data['вагон'] = wagon_match.group(1)

            seat_match = re.search(patterns['место'], text)
            if seat_match:
                extracted_data['место'] = seat_match.group(1)

            departure_match = re.search(patterns['отправление'], text)
            if departure_match:
                extracted_data['дата_отправления'] = departure_match.group(1)
                extracted_data['время_отправления'] = departure_match.group(2)

            arrival_match = re.search(patterns['прибытие'], text)
            if arrival_match:
                extracted_data['время_прибытия'] = arrival_match.group(1)

            return extracted_data

    except FileNotFoundError:
        print(f"Ошибка: Файл не найден по пути '{pdf_path}'")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None

# --- Пример использования ---
if __name__ == '__main__':
    # ЗАМЕНИТЕ 'path/to/your/ticket.pdf' НА РЕАЛЬНЫЙ ПУТЬ К ВАШЕМУ ФАЙЛУ
    # Например: 'C:/Users/User/Documents/ticket.pdf' или '/home/user/ticket.pdf'
    ticket_file_path = '\\downloads\\order_blank_265978027_394423444.pdf'

    print(f"Анализ файла: {ticket_file_path}\n")

    ticket_info = parse_russian_train_ticket(ticket_file_path)

    if ticket_info:
        print("="*30)
        print("      Извлеченная информация")
        print("="*30)
        for key, value in ticket_info.items():
            # Форматируем вывод для лучшей читаемости
            print(f"{key.replace('_', ' ').capitalize():<20}: {value}")
        print("="*30)
    else:
        print("\nНе удалось извлечь информацию из билета.")
        print("Пожалуйста, проверьте путь к файлу и его формат.")
