import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import TimestampType

# Инициализация SparkSession
spark = SparkSession.builder.appName("Weekly Log Aggregator").getOrCreate()


# Функция для загрузки CSV файлов за последние 7 дней
def load_inputs_for_last_7_days(log_dir, end_date):
    # Преобразуем строку даты в объект datetime
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_date_dt = end_date_dt - timedelta(days=7)

    # Список файлов за последние 7 дней
    date_range = [
        (start_date_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)
    ]

    # Чтение всех csv файлов за указанные дни
    all_inputs = None
    for date in date_range:
        log_file_path = os.path.join(log_dir, f"{date}.csv")
        if os.path.exists(log_file_path):
            daily_inputs = spark.read.csv(log_file_path, header=False, inferSchema=True)
            daily_inputs = (
                daily_inputs.withColumnRenamed("_c0", "email")
                .withColumnRenamed("_c1", "action")
                .withColumnRenamed("_c2", "dt")
            )
            daily_inputs = daily_inputs.withColumn(
                "dt", col("dt").cast(TimestampType())
            )

            if all_inputs is None:
                all_inputs = daily_inputs
            else:
                all_inputs = all_inputs.union(daily_inputs)

    return all_inputs


# Основная функция для агрегации логов
def aggregate_logs(log_dir, output_dir, end_date):
    inputs = load_inputs_for_last_7_days(log_dir, end_date)

    if inputs is None:
        print("No logs found for the specified period.")
        return

    # Агрегация данных по пользователю и действию
    aggregated_inputs = (
        inputs.groupBy("email")
        .pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"])
        .agg(count("action"))
    )

    # Заменяем null на 0 (если не было действий какого-то типа)
    aggregated_inputs = aggregated_inputs.fillna(0)

    # Переименовываем колонки для удобства
    aggregated_inputs = (
        aggregated_inputs.withColumnRenamed("CREATE", "create_count")
        .withColumnRenamed("READ", "read_count")
        .withColumnRenamed("UPDATE", "update_count")
        .withColumnRenamed("DELETE", "delete_count")
    )

    # Записываем результат в CSV файл
    output_file_path = os.path.join(output_dir, f"{end_date}.csv")
    aggregated_inputs.coalesce(1).write.csv(
        output_file_path, header=True, mode="overwrite"
    )
    print(f"Aggregated log data has been written to {output_file_path}")



input_dir = "/input/"
output_dir = "/output/"

end_date = datetime.now().strftime("%Y-%m-%d")
# end_date = '2024-10-10'


aggregate_logs(input_dir, output_dir, end_date)
