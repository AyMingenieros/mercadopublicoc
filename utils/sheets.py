import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime

SPREADSHEET_ID = "1TqiNXXAgfKlSu2b_Yr9r6AdQU_WacdROsuhcHL0i6Mk"

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
    

def cargar_palabras_clave(sheet):
    try:
        hoja = sheet.worksheet("Palabras Clave")
        palabras_raw = hoja.col_values(6)[7:19]  # Columna F, desde fila 8 (índice 7)
        palabras_clave = [p.strip() for p in palabras_raw if p.strip()]
        print(f"🔑 {len(palabras_clave)} palabras clave cargadas desde Google Sheets.")
        return palabras_clave
    except Exception as e:
        print(f"❌ Error al cargar palabras clave: {e}")
        return []


# =========================
# Helpers robustos de hoja
# =========================
def _norm(s: str) -> str:
    return (s or "").strip().lower()\
        .replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")\
        .replace("ñ","n").replace("°","")

def _find_header_idx(headers, candidatos):
    H = [_norm(h) for h in headers]
    for cand in candidatos:
        c = _norm(cand)
        for i, h in enumerate(H):
            if h == c:
                return i
    return None

def _asegurar_encabezados(hoja, esperados):
    headers = hoja.row_values(1)
    if not headers:
        hoja.update('A1', [esperados])
        return esperados
    # agrega faltantes al final SIN borrar los existentes
    faltantes = [h for h in esperados if h not in headers]
    if faltantes:
        nuevos = headers + faltantes
        hoja.update('A1', [nuevos])
        return nuevos
    return headers

def _ultimo_numero(hoja, headers):
    idx = _find_header_idx(headers, ["Número","Numero","N°","Nro","#","Num","No."])
    if idx is None:
        return 0
    vals = hoja.col_values(idx+1)[1:]  # sin encabezado
    nums = []
    for v in vals:
        v = (v or "").strip()
        digits = "".join(ch for ch in v if ch.isdigit())
        if digits:
            try:
                n = int(digits)
                if n > 0: nums.append(n)
            except:
                pass
    return max(nums) if nums else 0

def _ids_existentes(hoja, headers):
    idx = _find_header_idx(headers, ["ID","Id","id"])
    if idx is None:
        return set()
    vals = hoja.col_values(idx+1)[1:]
    return set((v or "").strip() for v in vals if (v or "").strip())


def guardar_en_hoja(resultados, fecha_objetivo):
    """
    Apila resultados en la pestaña del mes (August, September, ...).
    - Crea encabezados si faltan.
    - Tolera 'Número'/'N°' y variantes.
    - Deduplica por 'ID'.
    - Evita 429 quitando formateo por-celda (solo append).
    """
    if not resultados:
        print("⚠️ No hay resultados para guardar.")
        return

    mes = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%B").capitalize()
    sheet = conectar_google_sheets()

    columnas_ordenadas = [
        "Número", "FyH Extracción", "FyH Publicación", "ID", "Título",
        "Descripción", "Tipo", "Monto", "Tipo Monto",
        "LINK FICHA", "FyH TERRENO", "OBLIG?", "FyH CIERRE"
    ]

    df = pd.DataFrame(resultados)

    # abrir o crear pestaña del mes
    try:
        hoja = sheet.worksheet(mes)
    except gspread.exceptions.WorksheetNotFound:
        hoja = sheet.add_worksheet(title=mes, rows="1000", cols="20")
        hoja.update('A1', [columnas_ordenadas])

    # asegurar encabezados
    headers = _asegurar_encabezados(hoja, columnas_ordenadas)

    # leer último consecutivo y IDs ya guardados (para APILAR sin duplicar)
    ultimo = _ultimo_numero(hoja, headers)
    ids_guardados = _ids_existentes(hoja, headers)

    # filtrar duplicados por ID
    if "id" in df.columns:
        df = df[~df["id"].isin(ids_guardados)]

    if df.empty:
        print("📄 No hay nuevas licitaciones para agregar (todas ya existen en la hoja).")
        return

    # mapear a columnas finales
    df_out = pd.DataFrame()
    df_out["Número"]           = range(ultimo + 1, ultimo + 1 + len(df))
    df_out["FyH Extracción"]   = df.get("fecha_extraccion", "")
    df_out["FyH Publicación"]  = df.get("fecha_publicacion", "")
    df_out["ID"]               = df.get("id", "")
    df_out["Título"]           = df.get("titulo", "")
    df_out["Descripción"]      = df.get("descripcion", "")
    df_out["Tipo"]             = df.get("tipo", "")
    df_out["Monto"]            = df.get("monto", "")
    df_out["Tipo Monto"]       = df.get("tipo_monto", "")
    df_out["LINK FICHA"]       = df.get("link_ficha", "")
    df_out["FyH TERRENO"]      = df.get("fecha_visita", "")
    df_out["OBLIG?"]           = df.get("visita_obligatoria", "")
    df_out["FyH CIERRE"]       = df.get("fecha_cierre", "")

    # asegurar orden exacto
    for col in columnas_ordenadas:
        if col not in df_out.columns:
            df_out[col] = ""
    df_out = df_out[columnas_ordenadas]

    # append (apilar)
    hoja.append_rows(df_out.values.tolist(), value_input_option="USER_ENTERED")

    # IMPORTANTE: quitamos formateo por-celda para no exceder cuotas (429).
    # Si quieres colores automáticos, configura reglas de formato condicional manualmente
    # o agrega una función de "aplicar_formato_condicional" que se ejecute SOLO cuando
    # se cree la pestaña (1 batch de reglas, sin tocar cada fila).

    print(f"✅ {len(df_out)} nuevas licitaciones guardadas en la hoja '{mes}'")
