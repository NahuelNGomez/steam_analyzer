import json
import sys

def load_and_sort_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return sorted(data, key=lambda x: json.dumps(x, sort_keys=True))

def compare_json_files(file1, file2):
    data1 = load_and_sort_json(file1)
    data2 = load_and_sort_json(file2)
    return data1 == data2

def main():
    if len(sys.argv) != 3:
        print("Uso: python compare_json.py <file1> <file2>")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    if compare_json_files(file1, file2):
        print("Los archivos JSON son iguales.")
    else:
        print("Los archivos JSON son diferentes.")

if __name__ == "__main__":
    main()