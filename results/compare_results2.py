import json

# Función para leer archivos JSON
def load_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# Cargar los archivos
file1_parsed = load_json_file('responses.json')
file2_parsed = load_json_file('serial_queries_1_results.json')

# Función para ordenar listas de diccionarios por una clave común
def sort_list_by_key(lst, key):
    return sorted(lst, key=lambda x: x.get(key))

# Función para comparar dos diccionarios o listas
def compare_json(data1, data2, path=""):
    if isinstance(data1, dict) and isinstance(data2, dict):
        # Comparar claves en ambos diccionarios
        for key in data1.keys() | data2.keys():
            new_path = f"{path}/{key}" if path else key
            if key in data1 and key not in data2:
                print(f"Clave {new_path} está en file1 pero no en file2.")
            elif key not in data1 and key in data2:
                print(f"Clave {new_path} está en file2 pero no en file1.")
            else:
                compare_json(data1[key], data2[key], new_path)
    elif isinstance(data1, list) and isinstance(data2, list):
        # Intentar ordenar las listas por una clave común, si existe
        if all(isinstance(item, dict) and 'name' in item for item in data1 + data2):
            data1 = sort_list_by_key(data1, 'name')
            data2 = sort_list_by_key(data2, 'name')
        
        # Comparar longitud de las listas
        if len(data1) != len(data2):
            print(f"Diferente longitud en {path}: {len(data1)} vs {len(data2)}")
        # Comparar elementos por índice
        for index, (item1, item2) in enumerate(zip(data1, data2)):
            new_path = f"{path}[{index}]"
            compare_json(item1, item2, new_path)
    else:
        # Comparar valores directos
        if data1 != data2:
            print(f"Diferencia en {path}: {data1} vs {data2}")

# Comparar los dos archivos cargados
compare_json(file1_parsed, file2_parsed)