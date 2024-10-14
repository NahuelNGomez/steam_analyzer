import json
import sys
import difflib

def load_and_sort_json(file_path):
    """
    Carga un archivo JSON y lo devuelve en formato ordenado.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return sorted(data, key=lambda x: json.dumps(x, sort_keys=True))

def compare_json_files(file1, file2):
    """
    Compara dos archivos JSON y devuelve True si son iguales.
    """
    data1 = load_and_sort_json(file1)
    data2 = load_and_sort_json(file2)
    return data1 == data2, data1, data2

def print_differences(data1, data2):
    """
    Imprime las diferencias línea por línea entre dos JSONs.
    """
    json1_str = json.dumps(data1, indent=4, sort_keys=True).splitlines()
    json2_str = json.dumps(data2, indent=4, sort_keys=True).splitlines()

    diff = difflib.unified_diff(json1_str, json2_str, fromfile='file1', tofile='file2', lineterm='')
    print('\n'.join(diff))

def main():
    """
    Punto de entrada del programa. Compara dos archivos JSON pasados como argumentos.
    """
    if len(sys.argv) != 3:
        print("Uso: python compare_json.py <file1> <file2>")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    are_equal, data1, data2 = compare_json_files(file1, file2)

    if are_equal:
        print("Los archivos JSON son iguales.")
    else:
        print("Los archivos JSON son diferentes.")
        print("Diferencias:")
        print_differences(data1, data2)

if __name__ == "__main__":
    main()
