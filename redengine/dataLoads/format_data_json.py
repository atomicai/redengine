import json
import os
from pathlib import Path

# Пример исходного JSON
input_json = {
    "keywords_and_keyphrases": [
        "apple",
        "machine learning",
        "banana",
        "deep learning",
        "AI",
    ]
}


def split_keywords_and_keyphrases_for_all_posts():
    # Разделение на keywords и keyphrases
    data_dir = (
        Path.home() / "projects" / "redengine" / "redengine" / "dataLoads" / "dataSets/"
    )
    filename = Path("polaroids.ai.data_fresh.json")
    data_dir = Path(data_dir)
    with open(data_dir / filename) as json_file:
        data = json.load(json_file)

        for p in data["posts"]:
            keywords = []
            keyphrases = []

            # Перебор строк из поля keywords_and_keyphrases
            if p["keywords_or_phrases"] is None:
                p["keywords_or_phrases"] = []
            for item in p["keywords_or_phrases"]:
                #  print(item)

                if item["keyword_or_phrase"] is None:
                    item["keyword_or_phrase"] = []
                if (
                    " " in item["keyword_or_phrase"]
                ):  # Если строка содержит пробел, значит это фраза
                    keyphrases.append(item)

                else:  # В противном случае это одиночное слово
                    keywords.append(item)

            # Добавляем разделенные поля в текущий пост
            p["keywords"] = keywords
            p["keyphrases"] = keyphrases

            # Удаляем оригинальное поле keywords_and_keyphrases
            p.pop("keywords_or_phrases", None)

        return data["posts"]


# Выполнение преобразования
output_json = split_keywords_and_keyphrases_for_all_posts()

# Определение пути для нового файла
current_directory = os.getcwd()  # Получаем текущую директорию
output_file_path = os.path.join(
    current_directory, "polaroids.ai.data_fresh_updated.json"
)  # Указываем имя нового файла

# Сохранение результата в новый JSON-файл
with open(output_file_path, "w", encoding="utf-8") as f:
    json.dump(output_json, f, ensure_ascii=False)

print(f"Обновленный JSON сохранен в файл: {output_file_path}")
