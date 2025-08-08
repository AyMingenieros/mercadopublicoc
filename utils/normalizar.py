# utils/normalizar.py

import unidecode
import re

def normalizar(texto):
    texto = texto.lower()
    texto = unidecode.unidecode(texto)  # Quita tildes
    texto = re.sub(r"s\b", "", texto)   # Quita plurales simples
    return texto.strip()
