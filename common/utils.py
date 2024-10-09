import re

def split_complex_string(s):
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