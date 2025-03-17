# Мозаика: поток данных - Инструкция по разворачиванию на продуктовом сервере Ubuntu

Эта инструкция описывает пошаговый процесс разворачивания
проекта [Mosaic_Data_Conductor](https://github.com/VerP404/Mosaic_Data_Conductor) на продуктовом сервере Ubuntu без
использования Docker. Инструкция основана на [официальной документации Dagster](https://docs.dagster.io/) и включает
рекомендации по настройке виртуального окружения, переменных окружения и службы systemd для веб-сервера и демона.

> **Примечание:** Перед выполнением инструкций убедитесь, что у вас есть пользователь с правами на установку пакетов и
> настройку служб.

---

## 1. Клонирование репозитория

```bash
git clone https://github.com/VerP404/Mosaic_Data_Conductor.git
cd Mosaic_Data_Conductor
```

## 2. Создание и настройка виртуального окружения

1. Создание и активация виртуального окружения

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

2. Установка зависимостей проекта

```bash
pip install -r requirements.txt
```

## 3. Настройка Dagster

### 3.1. Создание директории для Dagster Instance

```bash
mkdir -p dagster_home
```

### 3.2. Создание файла конфигурации dagster.yaml

```bash
sudo nano dagster_home/dagster.yaml
```

Можно добавить базовую конфигурацию для очереди (опционально):

```bash
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator

run_launcher:
  module: dagster._core.launcher.default_run_launcher
  class: DefaultRunLauncher
```

### 3.3. Установка переменной окружения DAGSTER_HOME

```bash
echo 'export DAGSTER_HOME=/home/{user}/code/Mosaic_Data_Conductor/dagster_home' >> ~/.bashrc
source ~/.bashrc
```

Проверьте, что переменная установлена:

```bash
echo $DAGSTER_HOME
```

## 4. Настройка systemd служб для Dagster
### 4.1. Служба для Dagster Webserver
Создайте файл `sudo nano /etc/systemd/system/dagster-webserver.service`

```bash
[Unit]
Description=Dagster Webserver
After=network.target

[Service]
User={user}
WorkingDirectory=/home/{user}/code/Mosaic_Data_Conductor
ExecStart=/home/{user}/code/Mosaic_Data_Conductor/.venv/bin/dagster-webserver --host 0.0.0.0 --port 3000
Restart=on-failure
Environment=DAGSTER_HOME=/home/{user}/code/Mosaic_Data_Conductor/dagster_home

[Install]
WantedBy=multi-user.target
```
### 4.2. Служба для Dagster Daemon
Создайте файл `sudo nano /etc/systemd/system/dagster-daemon.service`

```bash
[Unit]
Description=Dagster Daemon
After=network.target

[Service]
User={user}
WorkingDirectory=/home/{user}/code/Mosaic_Data_Conductor
ExecStart=/home/{user}/code/Mosaic_Data_Conductor/.venv/bin/dagster-daemon run
Restart=on-failure
Environment=DAGSTER_HOME=/home/{user}/code/Mosaic_Data_Conductor/dagster_home

[Install]
WantedBy=multi-user.target
```

### 4.3. Перезагрузка systemd и запуск служб

```bash
sudo systemctl daemon-reload

sudo systemctl start dagster-webserver.service
sudo systemctl enable dagster-webserver.service

sudo systemctl start dagster-daemon.service
sudo systemctl enable dagster-daemon.service
```

Проверьте статус служб:

```bash
sudo systemctl status dagster-webserver.service
sudo systemctl status dagster-daemon.service
```

Конфигурация для запуска в pycharm для разработки:
![img.png](img.png)