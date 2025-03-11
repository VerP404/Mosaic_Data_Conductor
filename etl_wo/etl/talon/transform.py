import json
import pandas as pd
from dagster import asset, OpExecutionContext

@asset
def talon_transform(context: OpExecutionContext, talon_extract: dict):
    """
    Применяет маппинг колонок, заполняет отсутствующие значения дефолтами,
    и определяет, какие строки являются комплексными (если для пары (talon, source)
    найдено более одной записи). Затем разделяет данные на два датафрейма.
    """
    if talon_extract is None:
        text_value = f"❌ Ошибка: extract не передал данные!"
        context.log.info(text_value)
        raise ValueError(text_value)

    table_name = talon_extract.get("table_name", "Неизвестная таблица")
    df = talon_extract.get("data", None)
    if df is None:
        text_value = f"❌ Ошибка: Данные для {table_name} отсутствуют!"
        context.log.info(text_value)
        raise ValueError(text_value)

    MAPPING_PATH = "etl_wo/config/mapping.json"
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        mappings = json.load(f)
    table_config = mappings["tables"].get(table_name, {})
    column_mapping = table_config.get("mapping_fields", {})

    df = df.rename(columns=column_mapping)
    df = df[list(column_mapping.values())]

    required_columns = [
        "talon", "report_period", "source", "account_number", "upload_date", "status",
        "talon_type", "goal", "patient", "birth_date", "gender", "policy", "smo_code", "enp",
        "treatment_start", "treatment_end", "doctor", "doctor_profile", "staff_position",
        "department", "care_conditions", "medical_assistance_type", "disease_type", "main_disease_character",
        "visits", "mo_visits", "home_visits", "case_code", "main_diagnosis", "additional_diagnosis",
        "mp_profile", "bed_profile", "dispensary_monitoring", "specialty", "outcome", "result",
        "operator", "initial_input_date", "last_change_date", "amount", "sanctions", "ksg", "uet",
        "additional_status_info", "is_complex"
    ]

    for col in required_columns:
        if col not in df.columns and col != "is_complex":
            df[col] = "-"

    df["is_complex"] = False

    grouped = df.groupby(["talon", "source"])
    for (talon, source), group in grouped:
        if len(group) > 1:
            df.loc[group.index, "is_complex"] = True

    normal_df = df[df["is_complex"] == False].copy()
    complex_df = df[df["is_complex"] == True].copy()
    normal_count = len(normal_df)
    complex_count = len(complex_df)
    context.log.info(f"🔄 Трансформация завершена. Всего строк: {len(df)}. "
                     f"Обычных (будут обновлены/вставлены): {normal_count}. "
                     f"Комплексных (будут обновлены/вставлены): {complex_count}.")
    return {"normal": {"table_name": "load_data_talons", "data": normal_df},
            "complex": {"table_name": "load_data_complex_talons", "data": complex_df}}
