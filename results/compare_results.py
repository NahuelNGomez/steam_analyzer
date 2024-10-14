import json
import difflib
import os
import logging

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Error al cargar el archivo {file_path}: {e}")
        return None

def clean_responses(responses):
    cleaned_responses = []
    for response in responses:
        if isinstance(response, str):
            if response.startswith("OK") or "ACK de fin" in response:
                continue
        try:
            parsed_response = json.loads(response)
            cleaned_responses.append(parsed_response)
        except json.JSONDecodeError:
            continue
    return cleaned_responses


def compare_jsons(json1, json2):
    # Convertir ambos JSON en cadenas con formato JSON indentado
    json1_str = json.dumps(json1, indent=4, ensure_ascii=False)
    json2_str = json.dumps(json2, indent=4, ensure_ascii=False)

    # Comparar ambos strings y obtener las diferencias
    diff = difflib.unified_diff(
        json1_str.splitlines(),
        json2_str.splitlines(),
        fromfile='responses.json',
        tofile='serial_queries_1_results.json',
        lineterm=''
    )
    return list(diff)

def main():
    # Archivos JSON a comparar
    file1 = 'responses.json'
    file2 = 'completed_results.json'

    # Cargar ambos archivos
    json1 = load_json(file1)
    json2 = load_json(file2)

    if json1 is None or json2 is None:
        logging.error("No se pudo cargar uno o ambos archivos JSON")
        return

    # Limpiar y preparar los datos de responses.json
    cleaned_json1 = clean_responses(json1)

    # Comparar y obtener las diferencias
    differences = compare_jsons(cleaned_json1, json2)

    # Mostrar diferencias
    if differences:
        logging.info("Diferencias encontradas:")
        for line in differences:
            print(line)
    else:
        print("No se encontraron diferencias, los resultados son equivalentes.")

if __name__ == "__main__":
    main()