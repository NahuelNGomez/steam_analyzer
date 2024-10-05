import re

def split_complex_string(s):
    # Usamos una expresión regular que captura comas, pero no dentro de arrays [] ni dentro de comillas
    # Esto identifica bloques entre comillas o corchetes como un solo token
    pattern = r'''
        (?:\[.*?\])   # Captura arrays entre corchetes
        |             # O
        (?:".*?")     # Captura texto entre comillas dobles
        |             # O
        (?:'.*?')     # Captura texto entre comillas simples
        |             # O
        (?:[^,]+)     # Captura cualquier cosa que no sea una coma
    '''
    return re.findall(pattern, s, re.VERBOSE)
