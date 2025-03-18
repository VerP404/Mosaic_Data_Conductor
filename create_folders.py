import os


def create_folders():
    base_dir = os.path.join(os.getcwd(), 'etl_wo', 'data')

    # Структура папок
    folder_structure = {
        'kvazar': ['eln', 'emd', 'recipe', 'death', 'reference'],
    }

    for parent_folder, subfolders in folder_structure.items():
        parent_path = os.path.join(base_dir, parent_folder)
        # Создаем родительскую папку, если её нет
        if not os.path.exists(parent_path):
            os.makedirs(parent_path)
            print(f"Создана папка: {parent_path}")
        else:
            print(f"Папка уже существует: {parent_path}")

        # Создаем вложенные папки
        for subfolder in subfolders:
            subfolder_path = os.path.join(parent_path, subfolder)
            if not os.path.exists(subfolder_path):
                os.makedirs(subfolder_path)
                print(f"Создана папка: {subfolder_path}")
            else:
                print(f"Папка уже существует: {subfolder_path}")


if __name__ == "__main__":
    create_folders()
