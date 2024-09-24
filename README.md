# Задача

Нужно разработать приложение для пакетной обработки потока данных и вычисления на нём агрегированных показателей на определенном временном окне данных.

**Входные данные:**\
Есть логи CRUD действий пользователей в формате csv файлов, каждый из которых содержит логи за один определённый день с тремя колонками `email`, `action`, `dt`. 

`action` это один из CREATE READ UPDATE DELETE.

Для генерации входных данных можно воспользоваться скриптом `generate.py`.

# Сценарий использования
Нужно реализовать функционал вычисления суммарного количества каждого действия каждого пользователя за 7 предыдущих дней. В выходном csv файле для каждого уникального пользователя должна быть ровно одна строка с почтой пользователя и количествами каждого из типов действий, итоговая схема будет `email`, `create_count`, `read_count`, `update_count`, `delete_count`.

Получившийся агрегированный файл нужно класть в директорию output, именовать файл по правилу `YYYY-mm-dd.csv`. 

То есть, если запустить вычисление агрегата, например, за 2024-09-16, то должны быть подсчитаны действия от 2024-09-09 до 2024-09-15 включительно, и результат должен быть записан в output/2024-09-16.csv.
 
# Требуемый функционал

## Минимальный
Питон скрипт, который при вызове python script.py <YYYY-mm-dd> посчитал бы недельный агрегат на дату и записал в соответствующий csv файл.
