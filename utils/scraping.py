# utils/scraping.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
# Si en tu main importas conectar_google_sheets desde utils.sheets,
# esta importación siguiente puede no ser necesaria; la dejo para no perder nada.
from utils.sheets import conectar_google_sheets
import time
import os
import json

BASE_URL = "https://www.mercadopublico.cl/BuscarLicitacion"
SPREADSHEET_ID = "1TqiNXXAgfKlSu2b_Yr9r6AdQU_WacdROsuhcHL0i6Mk"

R_COMMIT = True

# --- (OPCIONAL) Duplicada si ya la tienes en utils/sheets.py ---
def conectar_google_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    # Producción: lee el JSON desde la variable de entorno
    if "GCP_SERVICE_ACCOUNT_KEY" in os.environ:
        creds_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        # Local: usa el fichero service_account.json
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)

    client = gspread.authorize(creds)
    print("✅ Conexión con Google Sheets exitosa")
    return client.open_by_key(SPREADSHEET_ID)
# --- fin duplicada ---

def cargar_palabras_clave(sheet):
    try:
        hoja = sheet.worksheet("Palabras Clave")
        # Lee desde la celda B9 hacia abajo
        palabras_raw = hoja.col_values(2)[8:]  # Índice 8 porque empieza desde fila 9 (0-based)
        palabras_clave = [p.strip() for p in palabras_raw if p.strip()]
        print(f"🔑 {len(palabras_clave)} palabras clave cargadas desde Google Sheets.")
        return palabras_clave
    except Exception as e:
        print(f"❌ Error al cargar palabras clave: {e}")
        return []

def iniciar_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def buscar_y_extraer(driver, palabra, fecha_objetivo):
    print(f"\U0001F50D Buscando: {palabra}")
    resultados = []

    try:
        driver.get(BASE_URL)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "textoBusqueda")))
        input_busqueda = driver.find_element(By.ID, "textoBusqueda")
        input_busqueda.clear()
        input_busqueda.send_keys(palabra)
        input_busqueda.send_keys(Keys.ENTER)

        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "lic-block-body")))
        time.sleep(2)

        tarjetas = driver.find_elements(By.CLASS_NAME, "lic-block-body")

        for tarjeta in tarjetas:
            try:
                onclick_elem = tarjeta.find_element(By.CSS_SELECTOR, "a")
                onclick = onclick_elem.get_attribute("onclick") if onclick_elem else None
                if not onclick or "DetailsAcquisition.aspx?" not in onclick:
                    continue

                start = onclick.find("DetailsAcquisition.aspx?")
                qs = onclick[start:].split("'")[0]
                link_ficha = "https://www.mercadopublico.cl/Procurement/Modules/RFB/" + qs
                id_real = qs.split("idlicitacion=")[-1].strip()

                # Filtrado por tipo
                if not any(tipo in id_real for tipo in ["LE", "LP", "LQ", "LR"]):
                    continue

                if "LE" in id_real:
                    tipo = "100-1000 UTM"
                elif "LP" in id_real:
                    tipo = "1000-2000 UTM"
                elif "LQ" in id_real:
                    tipo = "2000-5000 UTM"
                elif "LR" in id_real:
                    tipo = "5000+ UTM"
                else:
                    tipo = ""

                # Abrir ficha en pestaña nueva
                driver.execute_script("window.open(arguments[0]);", link_ficha)
                driver.switch_to.window(driver.window_handles[1])
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(2)

                # Extracciones con try individual (mantengo tu lógica)
                try:
                    titulo = driver.find_element(By.ID, "lblNombreLicitacion").text.strip()
                except:
                    titulo = ""

                try:
                    descripcion = driver.find_element(By.ID, "lblFicha1Descripcion").text.strip()
                except:
                    descripcion = ""

                try:
                    fecha_publicacion = driver.find_element(By.ID, "lblFicha3Publicacion").text.strip()
                except:
                    fecha_publicacion = ""

                try:
                    fecha_cierre = driver.find_element(By.ID, "lblFicha3Cierre").text.strip()
                except:
                    fecha_cierre = ""

                try:
                    fecha_apertura = driver.find_element(By.ID, "lblFicha3ActoAperturaTecnica").text.strip()
                except:
                    fecha_apertura = ""

                try:
                    obligatoria = driver.find_element(By.ID, "grvFechasUsuario_ctl02_lblFicha3TituloFechaUsuario").text.strip() or "NF"
                    fecha_visita = driver.find_element(By.ID, "grvFechasUsuario_ctl02_lblFicha3FechaUsuario").text.strip() or "ok"
                except Exception:
                    obligatoria = "No encontrada en Ficha"
                    fecha_visita = "No disponible en ficha"

                try:
                    tipo_monto = driver.find_element(By.ID, "lblFicha7TituloMontoEstimado").text.strip()
                except:
                    tipo_monto = "NO PUBLICO"

                try:
                    monto = driver.find_element(By.ID, "lblFicha7MontoEstimado").text.strip()
                except:
                    monto = "NF"

                driver.close()
                driver.switch_to.window(driver.window_handles[0])

                resultados.append({
                    "palabra": palabra,
                    "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "fecha_publicacion": fecha_publicacion,
                    "id": id_real,
                    "titulo": titulo,
                    "descripcion": descripcion,
                    "tipo": tipo,
                    "monto": monto,
                    "tipo_monto": tipo_monto,
                    "link_ficha": link_ficha,
                    "fecha_visita": fecha_visita,
                    "visita_obligatoria": obligatoria,
                    "fecha_cierre": fecha_cierre,
                    "fecha_apertura": fecha_apertura
                })

            except Exception as e:
                print(f"⚠️ Error en tarjeta individual: {e}")

    except Exception as e:
        print(f"❌ Error general en búsqueda: {e}")

    return resultados

def ejecutar_scraping(fecha_objetivo, palabras):
    driver = iniciar_driver()
    resultados_totales = []
    try:
        for palabra in palabras:
            resultados = buscar_y_extraer(driver, palabra, fecha_objetivo)
            resultados_totales.extend(resultados)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return resultados_totales
