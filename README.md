# Задача

Нужно разработать приложение для пакетной обработки потока данных и вычисления на нём агрегированных показателей на определенном временном окне данных.

**Входные данные:**\
Есть логи CRUD действий пользователей в формате csv файлов, каждый из которых содержит логи за один определённый день с тремя колонками `email`, `action`, `dt`. 

`action` это один из CREATE READ UPDATE DELETE.

Для генерации входных данных можно воспользоваться скриптом `generate.py`.

# Сценарий использования
Нужно реализовать функционал вычисления суммарного количества каждого действия каждого пользователя за 7 предыдущих дней. В выходном csv файле для каждого уникального пользователя должна быть ровно одна строка с почтой пользователя и количествами каждого из типов действий, итоговая схема будет email, create_count, read_count, update_count, delete_count.

Получившийся агрегированный файл нужно класть в директорию output, именовать файл по правилу YYYY-mm-dd.csv. 

То есть, если запустить вычисление агрегата, например, за 2024-09-16, то должны быть подсчитаны действия от 2024-09-09 до 2024-09-15 включительно, и результат должен быть записан в output/2024-09-16.csv.
 
# Требуемый функционал

## Минимальный
Питон скрипт, который при вызове python script.py <YYYY-mm-dd> посчитал бы недельный агрегат на дату и записал в соответствующий csv файл.

## Рекомендуемый
Развернуть Apache Airflow с одним DAG, где по расписанию каждый день в 7:00 запускается вычисление недельного агрегата на Apache Spark. Развернуть Airflow можно по инструкции тут, там есть ссылка на пример docker-compose.yaml файла. В качестве input и output директорий из условия можно взять произвольные директории хост машины и замаунтить их в контейнеры.

## Продвинутый

В дополнение к предыдущему нужно вычисление организовать так, чтобы не просматривать все данные при вычислении каждого агрегата. Для этого можно отдельно сохранять результаты некоторых промежуточных вычислений для каждого дня. И потом уже на их основе строить недельные агрегаты. Это обусловлено тем, что на практике обычно один дневной лог содержит миллиарды событий с миллионами уникальных пользователей, и каждый раз просматривать все данные за период выходит затратно по времени.

Для хранения промежуточных данных можно использовать отдельную директорию.


В поле ответа вставь ссылку на GitHub репозиторий.