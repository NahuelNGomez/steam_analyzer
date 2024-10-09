import re

def split_complex_string(s):
    # Expresión regular que captura comas vacías o valores complejos (arrays o comillas)
    pattern = r'''
        (?:\[.*?\])   # Captura arrays entre corchetes
        |             # O
        (?:".*?")     # Captura texto entre comillas dobles
        |             # O
        (?:'.*?')     # Captura texto entre comillas simples
        |             # O
        (?:[^,]+)     # Captura cualquier cosa que no sea una coma
        |             # O
        (?:,)         # Captura comas vacías
    '''

    tokens = re.findall(pattern, s, re.VERBOSE)
    
    return result