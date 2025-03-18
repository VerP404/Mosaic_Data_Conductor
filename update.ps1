# Определяем директорию скрипта
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptDir

# Переход в корневую директорию проекта (если скрипт находится в корне проекта)
# Если update.ps1 лежит в отдельной папке, замените следующий путь на нужный.
Set-Location "$ScriptDir"

# Активация виртуального окружения
# Для PowerShell в Windows активировать виртуальное окружение можно так:
& .\.venv\Scripts\Activate.ps1

# Обновление кода из Git
git pull

# Установка зависимостей
pip install -r requirements.txt

# Создание структуры папок (если имеется скрипт create_folders.py)
python create_folders.py

Write-Host "Обновление Dagster-приложения завершено."
