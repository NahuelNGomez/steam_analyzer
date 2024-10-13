import json

# Función para cargar y comparar dos archivos JSON
def compare_json_files(file1, file2):
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        json1 = json.load(f1)
        json2 = json.load(f2)
    
    # Comparar los JSON como diccionarios
    if json1 == json2:
        print("Los archivos JSON son equivalentes.")
    else:
        print("Los archivos JSON son diferentes.")

# Usar la función
compare_json_files('serial_queries_1_results.json', 'responses.json')