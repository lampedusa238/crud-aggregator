import os
import sys
from datetime import datetime, timedelta

import pandas as pd

INPUT_DIR = "input"
OUTPUT_DIR = "output"


def load_data(start_date: str, days: int = 7) -> pd.DataFrame:
    """
    Загрузка данных за последние `days` дней, начиная с даты `start_date`.
    Если данных за какой-то день нет, возвращаем пустой DataFrame
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    data_frames = []

    # Загрузка данных из csv файлов за последние 7 дней
    for i in range(days):
        target_date = start_dt - timedelta(days=i+1)
        file_name = os.path.join(INPUT_DIR, f"{target_date.strftime('%Y-%m-%d')}.csv")
        if os.path.exists(file_name):
            df = pd.read_csv(file_name, names=["email", "action", "dt"])
            data_frames.append(df)
        else:
            print(f'{file_name} not exists')
            return pd.DataFrame(columns=["email", "action", "dt"])

    # Объединяем все загруженные DataFrame в один
    return pd.concat(data_frames, ignore_index=True)



def aggregate_data(df: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """
    Агрегация данных по каждому пользователю и типу действия.
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = start_dt - timedelta(days=7)

    # Фильтрация по временным рамкам за последние 7 дней
    df["dt"] = pd.to_datetime(df["dt"])
    filtered_df = df[(df["dt"] >= end_dt) & (df["dt"] < start_dt)]

    # Создаем сводную таблицу с подсчетом каждого действия
    aggregated = filtered_df.pivot_table(
        index="email", columns="action", aggfunc="size", fill_value=0
    ).reset_index()

    # Добавляем недостающие колонки, если какие-то действия отсутствуют
    for action in ["CREATE", "READ", "UPDATE", "DELETE"]:
        if action not in aggregated.columns:
            aggregated[action] = 0

    # Переименовываем колонки в требуемый формат
    aggregated.columns = [
        "email",
        "create_count",
        "delete_count",
        "read_count",
        "update_count",
    ]

    # Убедимся, что порядок столбцов правильный
    aggregated = aggregated[
        ["email", "create_count", "read_count", "update_count", "delete_count"]
    ]

    return aggregated


def save_aggregated_data(aggregated_df: pd.DataFrame, target_date: str):
    """
    Сохранение агрегированных данных в CSV файл.
    """
    output_file = os.path.join(OUTPUT_DIR, f"{target_date}.csv")
    aggregated_df.to_csv(output_file, index=False)
    print(f"Aggregated data saved to {output_file}")


def main(target_date: str):
    # Загружаем данные
    print(f"Loading data for the 7 days before {target_date}...")
    data = load_data(target_date)

    if data.empty:
        print("No data found for the given date range.")
        return

    # Агрегируем данные
    print("Aggregating data...")
    aggregated_data = aggregate_data(data, target_date)

    # Сохраняем результат
    print("Saving the result...")
    save_aggregated_data(aggregated_data, target_date)


if __name__ == "__main__":
    # Получаем дату из аргументов командной строки
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)

    target_date = sys.argv[1]

    # Создаем директорию output, если её нет
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Запускаем основной процесс
    main(target_date)
