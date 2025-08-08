from utils.scraping import ejecutar_scraping
from utils.sheets import guardar_en_hoja, conectar_google_sheets, cargar_palabras_clave
from utils.fechas import obtener_fecha_ayer_formateada

FORZAR_COMMIT = True

def main():
    fecha_objetivo = obtener_fecha_ayer_formateada()
    print(f"📆 Ejecutando scraping para la fecha: {fecha_objetivo}")

    sheet = conectar_google_sheets()
    palabras = cargar_palabras_clave(sheet)

    resultados = ejecutar_scraping(fecha_objetivo, palabras)

    print(f"✅ Total licitaciones encontradas: {len(resultados)}")
    for lic in resultados:
        print(f"{lic['id']} | {lic['titulo']} | {lic['fecha_cierre']}")

    guardar_en_hoja(resultados, fecha_objetivo)

if __name__ == "__main__":
    main()
 
 #hoaasa
 