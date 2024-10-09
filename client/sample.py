# import pandas as pd

# # Ruta al archivo CSV
# input2_file = "../datasets/games.csv"  # Archivo games.csv
# output2_file = "datasets/sample_1000_games.csv"  # Archivo de salida

# input_file = "../datasets/dataset.csv"  # Archivo reviews.csv
# output_file = "datasets/sample_1000_reviews.csv"  # Arch



# # Leer el CSV completo asegurando que pandas reconozca la columna 'AppID'
# #df2 = pd.read_csv(input2_file)
# df = pd.read_csv(input_file)
# sample = df.sample(n=1000, random_state=42)
# sample.to_csv(output_file, index=False)

# # # Verificar si la columna 'AppID' está presente
# # if 'AppID' not in df2.columns:
# #     print("La columna 'AppID' no está presente en el archivo.")
# # else:
# #     # Crear una muestra aleatoria de 1000 filas, preservando el AppID
# #     #sample_2_df = df2.sample(n=1000, random_state=42)
# #     sample = df.sample(n=1000, random_state=42)

# #     # Guardar la muestra en un nuevo archivo CSV, manteniendo el AppID original
# #     sample_to_csv(output_file, index=False)

# #     print(f"Sample con IDs generado en: {output_file}")
import pandas as pd

# Ruta al archivo CSV original
input_file = "../datasets/dataset.csv"  # Ajusta según la ubicación real
output_file = "datasets/sample_1000_reviews.csv"  # Archivo corregido
df = pd.read_csv(input_file)
sample = df.sample(n=3000, random_state=42)
sample.to_csv(output_file, index=False)
# # Leer el archivo CSV, especificando el delimitador correcto
# df = pd.read_csv(input_file, delimiter=",", dtype=str)

# # Eliminar los ceros adicionales antes de la columna 'About the game'
# # Aquí se considera que los ceros están en las columnas antes de 'About the game'
# # Verifica y ajusta si la posición de las columnas es diferente
# columns_to_check = ['Peak CCU', 'Required age', 'Price', 'DiscountDLC count']
# for col in columns_to_check:
#     df[col] = df[col].replace('0', '', regex=True)

# # Guardar el archivo corregido
# df.to_csv(output_file, index=False)

# print(f"Archivo corregido guardado en: {output_file}")
