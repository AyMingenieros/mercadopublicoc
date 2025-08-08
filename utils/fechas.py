from datetime import datetime, timedelta
import pytz

def obtener_fecha_ayer_formateada():
    zona_chile = pytz.timezone("America/Santiago")
    ahora_chile = datetime.now(zona_chile)
    ayer_chile = ahora_chile - timedelta(days=1)
    return ayer_chile.strftime("%Y-%m-%d")

