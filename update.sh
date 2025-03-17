#!/bin/bash

# Определение директории, в которой находится скрипт
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Переход в корневую директорию проекта (предполагается, что scripts находится в корне проекта)
cd "$SCRIPT_DIR" || exit

# Активация виртуального окружения
source .venv/bin/activate

# Обновление кода
git pull

# Установка зависимостей
pip install -r requirements.txt

echo "Обновление Dagster-приложения завершено."