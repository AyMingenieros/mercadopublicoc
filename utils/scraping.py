# utils/sheets.py
import os
import json
from datetime import datetime

import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

SPREADSHEET_ID = "1TqiNXXAgfKlSu2b_Yr9r6AdQU_WacdROsuhcHL0i6Mk"

# =========================
# Conexión
# =========================
def conectar_google_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    if "GCP_SERVICE_ACCOUNT_KEY" in os.environ:
        creds_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)

    client = gspread.authorize(creds)
    print("✅ Conexión con Google Sheets exitosa")
    return client.open_by_key(SPREADSHEET_ID)

# =========================
# Utilidades de encabezados
# =========================
def _normaliza(s: str) -> str:
    return (s or "").strip().lower() \
        .replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u") \
        .replace("ñ","n")

def _encabezado_idx(headers, candidatos):
    norm_headers = [_normaliza(h) for h in headers]
    for cand in candidatos:
        c = _normaliza(cand)
        for i, h in enumerate(norm_headers):
            if h == c:
                return i
    return None

def _asegurar_encabezados(hoja, esperados):
    headers_actuales = hoja.row_values(1)
    if not headers_actuales:
        hoja.update('A1', [esperados])
        return esperados
    faltantes = [h for h in esperados if h not in headers_actuales]
    if faltantes:
        nuevos = headers_actuales + faltantes
        hoja.update('A1', [nuevos])
        return nuevos
    return headers_actuales

def _ultimo_numero(hoja, headers):
    # Soporta nombres alternos de la columna Número
    candidatos = ["Número","Numero","N°","Nro","#","Num","No."]
    idx = _encabezado_idx(headers, candidatos)
    if idx is None:
        return 0
    valores = hoja.col_values(idx + 1)[1:]  # omite encabezado
    nums = []
    for v in valores:
        v = (v or "").strip()
        digits = "".join(ch for ch in v if ch.isdigit())
        if digits:
            try:
                n = int(digits)
                if n > 0:
                    nums.append(n)
            except:
                pass
    return max(nums) if nums else 0

def _ids_existentes(hoja, headers):
    idx = _encabezado_idx(headers, ["ID","Id","id"])
    if idx is None:
        return set()
    vals = hoja.col_values(idx + 1)[1:]
    return set((v or "").strip() for v in vals if (v or "").strip())

# =========================
# Guardado robusto
# =========================
def guardar_en_hoja(resultados, fecha_objetivo: str):
    """
    Escribe resultados en la hoja del mes correspondiente.
    - Crea/completa encabezados si faltan.
    - Calcula el consecutivo aunque la columna no se llame exactamente "Número".
    - Deduplica por ID.
    - Aplica formato simple a columnas clave.
    """
    if not resultados:
        print("⚠️ No hay resultados para guardar.")
        return

    # Nombre de la pestaña (mes)
    mes = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%B").capitalize()
    sheet = conectar_google_sheets()

    # Orden/nombres canónicos a usar en la hoja
    columnas_ordenadas = [
        "Número", "FyH Extracción", "FyH Publicación", "ID", "Título",
        "Descripción", "Tipo", "Monto", "Tipo Monto",
        "LINK FICHA", "FyH TERRENO", "OBLIG?", "FyH CIERRE"
    ]

    df_nuevo = pd.DataFrame(resultados)

    # Abre o crea la pestaña del mes con encabezados
    try:
        hoja = sheet.worksheet(mes)
    except gspread.exceptions.WorksheetNotFound:
        hoja = sheet.add_worksheet(title=mes, rows="1000", cols="20")
        hoja.update('A1', [columnas_ordenadas])

    headers = _asegurar_encabezados(hoja, columnas_ordenadas)
    ultimo = _ultimo_numero(hoja, headers)
    ids_exist = _ids_existentes(hoja, headers)

    # Deduplicación por ID
    if "id" in df_nuevo.columns:
        df_nuevo = df_nuevo[~df_nuevo["id"].isin(ids_exist)]
    else:
        print("⚠️ No se encontró la columna 'id' en resultados. No se puede deduplicar.")

    if df_nuevo.empty:
        print("📄 No hay nuevas licitaciones para agregar (todas ya existen en la hoja).")
        return

    # Mapeo a columnas finales (usa .get para robustez)
    df_nuevo = df_nuevo.copy()
    df_nuevo["Número"]            = range(ultimo + 1, ultimo + 1 + len(df_nuevo))
    df_nuevo["FyH Extracción"]    = df_nuevo.get("fecha_extraccion", "")
    df_nuevo["FyH Publicación"]   = df_nuevo.get("fecha_publicacion", "")
    df_nuevo["ID"]                = df_nuevo.get("id", "")
    df_nuevo["Título"]            = df_nuevo.get("titulo", "")
    df_nuevo["Descripción"]       = df_nuevo.get("descripcion", "")
    df_nuevo["Tipo"]              = df_nuevo.get("tipo", "")
    df_nuevo["Monto"]             = df_nuevo.get("monto", "")
    df_nuevo["Tipo Monto"]        = df_nuevo.get("tipo_monto", "")
    df_nuevo["LINK FICHA"]        = df_nuevo.get("link_ficha", "")
    df_nuevo["FyH TERRENO"]       = df_nuevo.get("fecha_visita", "")
    df_nuevo["OBLIG?"]            = df_nuevo.get("visita_obligatoria", "")
    df_nuevo["FyH CIERRE"]        = df_nuevo.get("fecha_cierre", "")

    # Asegura todas las columnas y reordena
    for col in columnas_ordenadas:
        if col not in df_nuevo.columns:
            df_nuevo[col] = ""
    df_nuevo = df_nuevo[columnas_ordenadas]

    # Append
    hoja.append_rows(df_nuevo.values.tolist(), value_input_option="USER_ENTERED")

    # Formato (verde si hay valor distinto de NF, rojo si vacío o NF)
    verde = {"backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}}
    rojo  = {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}
    cols_format = ["Monto", "Tipo Monto", "FyH TERRENO", "OBLIG?"]

    # Calcula rango de filas recién agregadas
    total_vals = hoja.get_all_values()
    end_row = len(total_vals)            # última fila con datos (1-based)
    n = len(df_nuevo)                    # filas nuevas agregadas
    start_row = end_row - n + 1          # primera fila nueva

    for nombre in cols_format:
        c_idx = columnas_ordenadas.index(nombre) + 1  # 1-based
        # Aplica formato celda por celda (simple y seguro)
        for r in range(start_row, end_row + 1):
            celda = rowcol_to_a1(r, c_idx)
            val = hoja.acell(celda).value or ""
            hoja.format(celda, verde if (val.strip() and val.strip() != "NF") else rojo)

    print(f"✅ {len(df_nuevo)} nuevas licitaciones guardadas en la hoja '{mes}'")
