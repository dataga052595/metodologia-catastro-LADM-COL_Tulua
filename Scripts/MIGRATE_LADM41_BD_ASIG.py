import os
import psycopg2
from psycopg2 import Error
import pandas as pd
import geopandas as gpd
from uuid import uuid4
import logging
from datetime import datetime
import numpy as np
from shapely.geometry import MultiPolygon

# ==============================================================
# REPORTE DE MIGRACIÓN
# ==============================================================
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
reporte_csv = f"REPORTE_MIGRACION_TULUA_{timestamp}.csv"
reporte_txt = f"REPORTE_MIGRACION_TULUA_{timestamp}.txt"

migracion_log = []  # <-- ¡¡ESTA ES LA CLAVE!!

def log_migracion(numero_predial, entidad, estado, motivo=""):
    migracion_log.append({
        "numero_predial": str(numero_predial).zfill(30),
        "entidad": entidad,
        "estado": estado,
        "motivo": str(motivo)[:200],
        "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# Logging + archivo de texto
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(reporte_txt, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === CONFIGURACIÓN ===
db_params = {
    "host": "localhost",
    "port": "5432",
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres123"),
    "dbname": "bd_ladm_05042026"
}

# Rutas
r1_excel_path = 'C:/Users/dan05/Desktop/ENTREGA FINAL_TG/METODOLOGIA/DIAGNOSTICO INICIAL/DATOS_TULUA_FN/76834_R1_30102024.xlsx'
r2_excel_path = 'C:/Users/dan05/Desktop/ENTREGA FINAL_TG/METODOLOGIA/DIAGNOSTICO INICIAL/DATOS_TULUA_FN/76834_R1_30102024.xlsx'
terrenos_shp_path = 'C:/Users/dan05/Desktop/ENTREGA FINAL_TG/METODOLOGIA/DIAGNOSTICO INICIAL/DATOS_TULUA_FN/TERRENOS.shp'
unidades_shp_path = 'C:/Users/dan05/Desktop/ENTREGA FINAL_TG/METODOLOGIA/DIAGNOSTICO INICIAL/DATOS_TULUA_FN/UNIDADES.shp'

# === MAPEO DESTINACIÓN ECONÓMICA ===
destino_map = {
    'A': 'Habitacional', 'B': 'Industrial', 'C': 'Comercial', 'D': 'Agropecuario',
    'E': 'Acuicola', 'F': 'Cultural', 'G': 'Recreacional', 'H': 'Salubridad',
    'I': 'Institucional', 'J': 'Educativo', 'K': 'Religioso', 'L': 'Agricola',
    'M': 'Pecuario', 'N': 'Agroindustrial', 'P': 'Uso_Publico',
    'R': 'Lote_Urbanizable_No_Urbanizado', 'S': 'Lote_Urbanizado_No_Construido',
    'X': 'Infraestructura_Hidraulica', 'Y': 'Infraestructura_Saneamiento_Basico',
}
DEFAULT_DESTINACION = 'Lote_No_Urbanizable'

doc_tipo_map = {
    'C': 'Cedula_Ciudadania', 'N': 'NIT', 'E': 'Cedula_Extranjeria',
    'T': 'Tarjeta_Identidad', 'X': 'Pasaporte',
}

condicion_map = {
    '0': ('NPH', 'Privado'),
    '2': ('Informal', 'Privado'),
    '3': ('Bien_Uso_Publico', 'Publico'),
    '4': ('Via', 'Publico'),
    '7': ('Parque_Cementerio_Unidad_Predial', 'Privado'),
    '8': ('Condominio_Unidad_Predial', 'Privado'),
    '9': ('PH_Unidad_Predial', 'Privado'),
}

# === CONEXIÓN ===
def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        logger.info("Conexión exitosa.")
        return conn
    except Error as e:
        logger.error(f"Error al conectar: {e}")
        return None

# === CARGAR PREDIOS ===
def load_predios(cursor):
    cursor.execute("SELECT numero_predial, t_ili_tid FROM ladm.cr_predio")
    return {row[0]: row[1] for row in cursor.fetchall()}

# ==============================================================
# ==============================================================
# 1. MIGRAR PREDIOS - CON REPORTE (VERSIÓN FINAL CORREGIDA)
# ==============================================================
def migrate_predios(conn, r1_path, r2_path):
    # Cargar R1
    r1_df = pd.read_excel(r1_path)
    r1_df.columns = r1_df.columns.str.lower()
    r1_df['numero_predial'] = r1_df['numero_predial'].astype(str).str.strip().str.zfill(30)

    # Cargar R2 Hoja1 (para ORIP y matrícula)
    r2_df = pd.read_excel(r2_path, sheet_name='Hoja1')
    r2_df.columns = r2_df.columns.str.lower()
    r2_df['numero_predial'] = r2_df['numero_predial'].astype(str).str.strip().str.zfill(30)
    r2_grouped = r2_df.groupby('numero_predial').first().reset_index()
    r2_dict = r2_grouped.set_index('numero_predial')[['orip', 'matricula']].to_dict('index')

    cursor = conn.cursor()
    predios_dict = load_predios(cursor)  # {numero_predial: t_ili_tid}
    insert_data = []

    r1_grouped = r1_df.groupby('numero_predial').first().reset_index()

    for _, row in r1_grouped.iterrows():
        npred = row['numero_predial']

        if npred in predios_dict:
            log_migracion(npred, "cr_predio", "YA_EXISTE")
            continue

        t_ili_tid = str(uuid4())

        # DEPARTAMENTO Y MUNICIPIO (de los primeros dígitos del número predial)
        dept = npred[0:2]
        mun = npred[2:5]

        # ORIP (como entero)
        r2_row = r2_dict.get(npred, {})
        orip_raw = r2_row.get('orip')
        orip = int(orip_raw) if pd.notna(orip_raw) and orip_raw != '' else 0

        # MATRÍCULA INMOBILIARIA (limpia, sin .0)
        matricula_raw = r2_row.get('matricula')
        if pd.isna(matricula_raw) or matricula_raw == '' or matricula_raw is None:
            matricula = None
        elif isinstance(matricula_raw, float):
            if matricula_raw.is_integer():
                matricula = str(int(matricula_raw))
            else:
                matricula = str(matricula_raw).replace('.0', '')
        else:
            matricula = str(matricula_raw).strip()

        # AVALUO CATASTRAL
        avaluo_raw = row.get('avaluo_catastral', 0)
        avaluo = round(float(avaluo_raw) if pd.notna(avaluo_raw) and avaluo_raw not in ['', None] else 0.0, 2)

        # DESTINACIÓN ECONÓMICA
        codigo = str(row.get('destino_economico', '')).strip().upper()
        destinacion = destino_map.get(codigo, DEFAULT_DESTINACION)

        # ÁREA CATASTRAL
        area_raw = row.get('area_terreno', 0)
        area_catastral = round(float(area_raw) if pd.notna(area_raw) and area_raw not in ['', None] else 0.0, 2)

        # TIPO Y CONDICIÓN DEL PREDIO (basado en dígito 22 del número predial)
        digito_22 = npred[21]
        condicion, tipo_predio = condicion_map.get(digito_22, ('NPH', 'Privado'))

        # Agregar a la lista de inserción
        insert_data.append((
            t_ili_tid, npred, dept, mun, orip, matricula, destinacion,
            area_catastral, avaluo, 'Activo', tipo_predio, condicion
        ))
        log_migracion(npred, "cr_predio", "MIGRADO")

    # Insertar todos los predios nuevos
    if insert_data:
        try:
            cursor.executemany("""
                INSERT INTO ladm.cr_predio 
                (t_ili_tid, numero_predial, departamento, municipio, Codigo_ORIP, Matricula_inmobiliaria,
                 destinacion_economica, area_catastral, avaluo_catastral, estado, tipo_predio, condicion_predio)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, insert_data)
            conn.commit()
            logger.info(f"{len(insert_data)} predios nuevos insertados correctamente.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error al insertar predios: {e}")
            for data in insert_data:
                log_migracion(data[1], "cr_predio", "ERROR", str(e)[:200])
    else:
        logger.info("No se encontraron predios nuevos para migrar.")

    cursor.close()

# ==============================================================
# 2. MIGRAR INTERESADOS - CON REPORTE (VERSIÓN FINAL CORREGIDA)
# ==============================================================
def migrate_interesados(conn, r1_path):
    df = pd.read_excel(r1_path)
    df.columns = df.columns.str.lower()
    
    cursor = conn.cursor()
    predios_dict = load_predios(cursor)  # {numero_predial: t_ili_tid}
    insert_data = []

    for _, row in df.iterrows():
        npred = str(row['numero_predial']).strip().zfill(30)

        if npred not in predios_dict:
            log_migracion(npred, "cr_interesado", "NO_MIGRADO", "Predio no existe en cr_predio")
            continue

        t_ili_tid = str(uuid4())

        # TIPO DE DOCUMENTO
        tipo_doc_raw = str(row.get('tipo_documento', '')).strip().upper()
        tipo_doc = doc_tipo_map.get(tipo_doc_raw, None)
        if not tipo_doc:
            log_migracion(npred, "cr_interesado", "NO_MIGRADO", f"Tipo documento inválido: {tipo_doc_raw}")
            continue

        # TIPO DE INTERESADO
        tipo_interesado = 'Persona_Natural' if tipo_doc != 'NIT' else 'Persona_Juridica'

        # NÚMERO DE DOCUMENTO (limpio, sin .0)
        num_doc_raw = row.get('numero_documento')
        if pd.isna(num_doc_raw) or str(num_doc_raw).strip() in ['', 'nan', 'None']:
            num_doc = None
        elif isinstance(num_doc_raw, float):
            if num_doc_raw.is_integer():
                num_doc = str(int(num_doc_raw))  # 123456789.0 → "123456789"
            else:
                num_doc = str(num_doc_raw).replace('.0', '').strip()
        else:
            num_doc = str(num_doc_raw).strip()

        # NOMBRES Y APELLIDOS (vacío → NULL)
        def clean_value(val):
            if pd.isna(val) or str(val).strip() in ['', 'nan']:
                return None
            return str(val).strip()

        p_nombre = clean_value(row.get('primer_nombre'))
        s_nombre = clean_value(row.get('segundo_nombre'))
        p_apellido = clean_value(row.get('primer_apellido'))
        s_apellido = clean_value(row.get('segundo_apellido'))
        razon_social = clean_value(row.get('razon_social'))

        # SEXO
        sexo_raw = str(row.get('sexo', '')).strip().upper()
        if sexo_raw.startswith('M'):
            sexo = 'Masculino'
        elif sexo_raw.startswith('F'):
            sexo = 'Femenino'
        else:
            sexo = 'No_Especificado'

        # Agregar a la lista de inserción
        insert_data.append((
            t_ili_tid, 
            tipo_interesado, 
            tipo_doc, 
            num_doc, 
            p_nombre, 
            s_nombre,
            p_apellido, 
            s_apellido, 
            razon_social, 
            sexo, 
            predios_dict[npred]  # predio_t_ili_tid
        ))
        log_migracion(npred, "cr_interesado", "MIGRADO")

    # Insertar todos los interesados
    if insert_data:
        try:
            cursor.executemany("""
                INSERT INTO ladm.cr_interesado 
                (t_ili_tid, tipo_interesado, tipo_documento, numero_documento,
                 primer_nombre, segundo_nombre, primer_apellido, segundo_apellido,
                 razon_social, sexo, predio_t_ili_tid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, insert_data)
            conn.commit()
            logger.info(f"{len(insert_data)} interesados insertados correctamente.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error al insertar interesados: {e}")
            for data in insert_data:
                # data[3] es el numero_documento, data[0] es t_ili_tid → usamos npred si lo tuviéramos, pero como no, log genérico
                log_migracion("DESCONOCIDO", "cr_interesado", "ERROR", str(e)[:200])
    else:
        logger.info("No se encontraron interesados nuevos para migrar.")

    cursor.close()

# === 4. MIGRAR TERRENOS - RELACIÓN DIRECTA CON predio_t_ili_tid ===
def migrate_terrenos(conn, shp_path):
    if not os.path.exists(shp_path):
        logger.warning(f"Archivo de terrenos no encontrado: {shp_path}")
        return

    gdf = gpd.read_file(shp_path)
    gdf.columns = gdf.columns.str.lower()
    cursor = conn.cursor()
    predios_dict = load_predios(cursor)  # {numero_predial: t_ili_tid}
    terreno_data = []

    for _, row in gdf.iterrows():
        npred = str(row['codigo']).strip().zfill(30)
        if npred not in predios_dict:
            continue

        predio_tid = predios_dict[npred]
        t_ili_tid = str(uuid4())

        geom = row.geometry
        if geom.is_empty or not geom.is_valid:
            continue
        if geom.geom_type == 'Polygon':
            geom = MultiPolygon([geom])

        area = round(geom.area, 2) if hasattr(geom, 'area') else round(float(row.get('st_area_sh', 0)) or 0.0, 2)

        terreno_data.append((
            t_ili_tid,
            predio_tid,        # ← Aquí va la relación directa
            npred,
            geom.wkt,
            area,
            'En_Rasante'      # o 'En_Rasante' según tu lógica
        ))

    if terreno_data:
        cursor.executemany("""
            INSERT INTO ladm.cr_terreno
            (t_ili_tid, predio_t_ili_tid, numero_predial, geometria, area, Relacion_Superficie)
            VALUES (%s, %s, %s, ST_MakeValid(ST_GeomFromText(%s, 9377)), %s, %s)
        """, terreno_data)
        logger.info(f"{len(terreno_data)} terrenos migrados con relación directa al predio")
    conn.commit()

# === 5. MIGRAR UNIDADES - VERSIÓN DEFINITIVA (USO DESDE SHP + CARACTERÍSTICA ÚNICA) ===
def migrate_unidades(conn, shp_path, r2_path=None):
    """
    r2_path es opcional ahora, porque el uso viene del SHP
    """
    if not os.path.exists(shp_path):
        logger.error(f"Archivo SHP no encontrado: {shp_path}")
        return

    gdf = gpd.read_file(shp_path)
    gdf.columns = gdf.columns.str.lower()
    gdf['codigo'] = gdf['codigo'].astype(str).str.strip().str.zfill(30)

    # === COLUMNA DEL USO NUMÉRICO (ajusta el nombre si es diferente) ===
    uso_col_candidates = ['uso', 'destino', 'codigo_uso', 'cod_uso', 'destino_economico']
    uso_col = None
    for col in uso_col_candidates:
        if col in gdf.columns:
            uso_col = col
            break
    if uso_col is None:
        logger.error("No se encontró columna de uso numérico en el SHP. Revisar: uso, destino, cod_uso, etc.")
        return

    # Limpiar identificador
    if 'identifica' not in gdf.columns:
        logger.error("Falta columna 'identifica' en SHP de unidades")
        return
    gdf['identifica'] = gdf['identifica'].fillna('A').astype(str).str.strip()
    gdf['identifica'] = gdf['identifica'].replace({'nan': 'A', '': 'A', 'None': 'A', '<NULL>': 'A'})

    # Convertir columna de uso a entero
    gdf[uso_col] = pd.to_numeric(gdf[uso_col], errors='coerce').fillna(0).astype(int)

    cursor = conn.cursor()
    predios_dict = load_predios(cursor)

    # === MAPEO OFICIAL: número del SHP → ilicode del dominio LADM ===
    USO_MAP = {
        71: 'Residencial.Apartamentos_4_y_mas_pisos_en_PH',
        56: 'Residencial.Apartamentos_En_Edificio_4_y_5_Pisos_Cartagena',
        35: 'Residencial.Apartamentos_Mas_De_4_Pisos',
        40: 'Residencial.Barracas',
        27: 'Residencial.Casa_Elbas',
        14: 'Residencial.Garajes_Cubiertos',
        79: 'Residencial.Garajes_En_PH',
        53: 'Residencial.Vivienda_Colonial',
        1:  'Residencial.Vivienda_Hasta_3_Pisos',
        70: 'Residencial.Vivienda_Hasta_3_Pisos_En_PH',
        63: 'Residencial.Vivienda_Recreacional',
        72: 'Residencial.Vivienda_Recreacional_En_PH',
        16: 'Comercial.Bodegas_Comerciales_Grandes_Almacenes',
        74: 'Comercial.Bodegas_Comerciales_en_PH',
        58: 'Comercial.Centros_Comerciales',
        76: 'Comercial.Centros_Comerciales_en_PH',
        33: 'Comercial.Clubes_Casinos',
        28: 'Comercial.Comercio',
        54: 'Comercial.Comercio_Colonial',
        75: 'Comercial.Comercio_en_PH',
        89: 'Comercial.Hotel_Colonial',
        31: 'Comercial.Hoteles',
        25: 'Comercial.Hoteles_en_PH',
        34: 'Comercial.Oficinas_Consultorios',
        55: 'Comercial.Oficinas_Consultorios_Coloniales',
        94: 'Residencial.Salon_Comunal',
        95: 'Residencial.Secadero_Ropa',
        77: 'Comercial.Oficinas_Consultorios_en_PH',
        39: 'Comercial.Parqueaderos',
        78: 'Comercial.Parqueaderos_en_PH',
        37: 'Comercial.Pensiones_y_Residencias',
        90: 'Comercial.Restaurante_Colonial',
        36: 'Comercial.Restaurantes',
        88: 'Comercial.Restaurantes_en_PH',
        86: 'Comercial.Teatro_Cinemas_en_PH',
        6:  'Industrial.Bodega_Casa_Bomba',
        7:  'Industrial.Industrias',
        80: 'Industrial.Industrias_en_PH',
        45: 'Industrial.Talleres',
        42: 'Institucional.Aulas_de_Clases',
        13: 'Institucional.Biblioteca',
        51: 'Institucional.Carceles',
        44: 'Institucional.Casas_de_Culto',
        19: 'Institucional.Clinicas_Hospitales_Centros_Medicos',
        12: 'Institucional.Colegio_y_Universidades',
        43: 'Institucional.Coliseos',
        65: 'Institucional.Fuertes_y_Castillos',
        29: 'Institucional.Iglesia',
        87: 'Institucional.Iglesia_en_PH',
        46: 'Institucional.Jardin_Infantil_en_Casa',
        52: 'Institucional.Parque_Cementerio',
        38: 'Institucional.Puestos_de_Salud',
        97: 'Comercial.Plaza_Mercado',
        99: 'Industrial.Bodegas_Casa_Bomba_en_PH',
        98: 'Comercial.Teatro_Cinemas',
        100: 'Institucional.Estadios',
        102: 'Institucional.Planetario',
        103: 'Institucional.Plaza_de_Toros',
        41: 'Institucional.Teatro',
        23: 'Anexo.Albercas_Banaderas',
        11: 'Anexo.Beneficiaderos',
        64: 'Anexo.Camaroneras',
        60: 'Anexo.Canchas_de_Tenis',
        85: 'Anexo.Carretera',
        104: 'Institucional.Museos',
        26: 'Anexo.Corrales',
        4:  'Anexo.Establos_Pesebreras_Caballerizas',
        3:  'Anexo.Galpones_Gallineros',
        21: 'Anexo.Kioscos',
        82: 'Anexo.Marquesinas_Patios_Cubiertos',
        48: 'Anexo.Muelles',
        66: 'Anexo.Murallas',
        9:  'Anexo.Piscinas',
        20: 'Anexo.Pozos',
        2:  'Anexo.Ramadas_Cobertizos_Caneyes',
        105: 'Institucional.Seminarios_Conventos',
        106: 'Institucional.Unidad_Deportiva',
        107: 'Institucional.Velodromo_Patinodromo',
        109: 'Anexo.Cerramiento',
        110: 'Anexo.Cimientos_Estructura_Muros_y_Placa_Base',
        111: 'Anexo.Construccion_en_Membrana_Arquitectonica',
        112: 'Anexo.Contenedor',
        113: 'Anexo.Estacion_Bombeo',
        114: 'Anexo.Estacion_Sistema_Transporte',
        115: 'Anexo.Hangar',
        83: 'Anexo.Lagunas_de_Oxidacion',
        116: 'Anexo.Pergolas',
        117: 'Anexo.Pista_Aeropuerto',
        18: 'Anexo.Secaderos',
        8:  'Anexo.Silos',
        10: 'Anexo.Tanques',
        62: 'Anexo.Toboganes',
        47: 'Anexo.Torres_de_Enfriamiento',
        84: 'Anexo.Via_Ferrea',
        93: 'Residencial.Depositos_Lockers',
        96: 'Comercial.Parque_Diversiones',
        91: 'Institucional.Entidad_Educativa_Colonial_Colegio_Colonial',
        101: 'Institucional.Instalaciones_Militares',
        108: 'Anexo.Canchas',
        118: 'Anexo.Torre_de_Control',
        119: 'Anexo.Unidad_Predial_por_Construir',
        5: 'Anexo.Cocheras_Marraneras_Porquerizas',
        0: 'Residencial.Vivienda_Hasta_3_Pisos',  # fallback seguro
    }

    # === TIPO PLANTA (como antes) ===
    TIPO_PLANTA_OFICIAL = {
        'PS-01': 'Piso', 'PS-02': 'Piso', 'PS-03': 'Piso', 'PS-04': 'Piso',
        'Sótano': 'Sotano', 'Sótano 1': 'Sotano', 'Sótano 2': 'Sotano',
        'Semisótano': 'Semisotano',
        'Piso 1': 'Piso', 'Piso 2': 'Piso', 'Piso 3': 'Piso', 'Piso 4': 'Piso',
        'Piso 5': 'Piso', 'Piso 6': 'Piso', 'Piso 7': 'Piso', 'Piso 8': 'Piso',
        'Mezzanine': 'Mezanine', 'Mezanine': 'Mezanine',
        'Cubierta': 'Cubierta', 'Terraza': 'Cubierta',
        'Subterráneo': 'Subterraneo',
    }

    # === CACHE Y DATOS ===
    caracteristicas_cache = {}  # (npred, identificador, uso_ilicode) → t_ili_tid_car
    caracteristicas_data = []
    unidad_data = []

    for _, row in gdf.iterrows():
        npred = row['codigo']
        if npred not in predios_dict:
            log_migracion(npred, "cr_unidadconstruccion", "NO_MIGRADO", "Predio no existe en cr_predio")
            continue

        predio_tid = predios_dict[npred]
        identificador = row['identifica']
        codigo_uso = int(row[uso_col])
        uso_ilicode = USO_MAP.get(codigo_uso, 'Residencial.Vivienda_Hasta_3_Pisos')

        clave = (npred, identificador, uso_ilicode)

        # === REUTILIZAR O CREAR CARACTERÍSTICA ===
        if clave not in caracteristicas_cache:
            t_ili_tid_car = str(uuid4())
            tipo_unidad = uso_ilicode.split('.')[0]  # Residencial, Comercial, etc.

            caracteristicas_data.append((
                t_ili_tid_car,
                identificador,
                tipo_unidad,
                1,  # total_plantas (puedes mejorar con lógica posterior)
                0,  # habitaciones
                0,  # baños
                0,  # locales
                uso_ilicode,
                0.0,  # área (se puede calcular después)
                'Bueno'
            ))
            caracteristicas_cache[clave] = t_ili_tid_car
            log_migracion(npred, "cr_caracteristicasunidadconstruccion", "MIGRADO", f"Uso: {uso_ilicode}")

        else:
            t_ili_tid_car = caracteristicas_cache[clave]

        # === UNIDAD DE CONSTRUCCIÓN ===
        t_ili_tid_uni = str(uuid4())
        planta_raw = str(row.get('planta', 'Piso 1')).strip()
        tipo_planta = TIPO_PLANTA_OFICIAL.get(planta_raw, 'Piso')
        planta_ubicacion = float(row.get('planta_ubicacion', 1) or 1)
        altura = float(row.get('altura', 0) or 0)

        geom = row.geometry
        if geom.is_empty or not geom.is_valid:
            log_migracion(npred, "cr_unidadconstruccion", "NO_MIGRADO", "Geometría vacía o inválida")
            continue
        if geom.geom_type == 'Polygon':
            geom = MultiPolygon([geom])
        area = round(geom.area, 2)

        unidad_data.append((
            t_ili_tid_uni,
            predio_tid,
            npred,
            tipo_planta,
            planta_ubicacion,
            altura,
            geom.wkt,
            area,
            t_ili_tid_car,
            'En_Rasante'
        ))
        log_migracion(npred, "cr_unidadconstruccion", "MIGRADO")

    # === INSERTAR ===
    if caracteristicas_data:
        try:
            cursor.executemany("""
                INSERT INTO ladm.cr_caracteristicasunidadconstruccion
                (t_ili_tid, identificador, tipo_unidad, total_plantas, total_habitaciones,
                 total_banios, total_locales, uso, area_construida, estado_conservacion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, caracteristicas_data)
            logger.info(f"{len(caracteristicas_data)} características únicas creadas (por identificador + uso del SHP)")
        except Exception as e:
            logger.error(f"Error insertando características: {e}")

    if unidad_data:
        try:
            cursor.executemany("""
                INSERT INTO ladm.cr_unidadconstruccion
                (t_ili_tid, predio_t_ili_tid, numero_predial, tipo_planta, planta_ubicacion,
                 altura, geometria, area, caracteristicas_t_ili_tid, Relacion_Superficie)
                VALUES (%s, %s, %s, %s, %s, %s, ST_MakeValid(ST_GeomFromText(%s, 9377)), %s, %s, %s)
            """, unidad_data)
            logger.info(f"{len(unidad_data)} unidades de construcción migradas con uso homologado desde SHP")
        except Exception as e:
            logger.error(f"Error insertando unidades: {e}")

    conn.commit()
    logger.info("Migración de unidades completada: uso desde SHP + característica única por grupo")

#6 ==============================================================
def _insertar_derechos_exactos(derechos_data, interesados_list, predio_tid):
    total = len(interesados_list)
    if total == 0:
        return

    # CASO DOMINIO PLENO: solo un interesado → fracción EXACTA 1.000000
    if total == 1:
        derechos_data.append({
            't_ili_tid': str(uuid4()),
            'predio_t_ili_tid': predio_tid,
            'tipo': 'Dominio',
            'interesado_t_ili_tid': interesados_list[0],
            'fraccion_derecho': 1.000000,   # ← EXACTO
            'informalidad_tipo': None,
            'restriccion_tipo': None
        })
        return

    # Múltiples interesados → repartimos con el método de partes enteras
    partes_totales = 1000000
    parte_base = partes_totales // total
    resto = partes_totales % total
    fracciones_partes = [parte_base + 1 if i < resto else parte_base for i in range(total)]

    for i, partes in enumerate(fracciones_partes):
        fraccion = round(partes / 1000000.0, 6)
        derechos_data.append({
            't_ili_tid': str(uuid4()),
            'predio_t_ili_tid': predio_tid,
            'tipo': 'Dominio',
            'interesado_t_ili_tid': interesados_list[i],
            'fraccion_derecho': fraccion,
            'informalidad_tipo': None,
            'restriccion_tipo': None
        })

    # === MÉTODO EXACTO CON PARTES ENTERAS (elimina error de punto flotante) ===
    partes_totales = 1000000                # 1.000000 → 1 millón de partes
    parte_base = partes_totales // total     # división entera
    resto = partes_totales % total           # lo que sobra (siempre < total)

    # Construimos la lista de partes (la mayoría recibe parte_base, algunos +1)
    fracciones_partes = [parte_base + 1 if i < resto else parte_base for i in range(total)]

    # Convertimos a decimal con 6 dígitos
    for i, partes in enumerate(fracciones_partes):
        fraccion = round(partes / 1000000.0, 6)   # siempre será exacto
        derechos_data.append({
            't_ili_tid': str(uuid4()),
            'predio_t_ili_tid': predio_tid,
            'tipo': 'Dominio',
            'interesado_t_ili_tid': interesados_list[i],
            'fraccion_derecho': fraccion,
            'informalidad_tipo': None,
            'restriccion_tipo': None
        })


# === 6. MIGRAR DERECHOS - VERSIÓN FINAL DEFINITIVA ===
def migrate_derechos(conn):
    cursor = conn.cursor()

    # 1. Limpiamos derechos anteriores (por si ya corriste versiones locas)
    cursor.execute("DELETE FROM ladm.cr_derecho;")
    conn.commit()
    logger.info("Derechos anteriores eliminados.")

    # 2. Obtenemos UN interesado por registro, sin duplicados
    cursor.execute("""
        SELECT 
            p.t_ili_tid AS predio_tid,
            p.numero_predial,
            i.t_ili_tid AS interesado_tid
        FROM ladm.cr_predio p
        JOIN ladm.cr_interesado i ON p.t_ili_tid = i.predio_t_ili_tid
        ORDER BY p.t_ili_tid, i.t_ili_tid
    """)
    filas = cursor.fetchall()

    derechos_data = []
    predio_tid_actual = None
    interesados_del_predio = []
    npred_actual = None

    for predio_tid, npred, interesado_tid in filas:
        if predio_tid != predio_tid_actual:
            # Procesamos el predio anterior
            if interesados_del_predio:
                repartir_derechos_exactos(derechos_data, interesados_del_predio, predio_tid_actual)
                log_migracion(npred_actual, "cr_derecho", "MIGRADO", 
                            f"{len(interesados_del_predio)} titular(es) → suma 1.000000")
            interesados_del_predio = [interesado_tid]
            predio_tid_actual = predio_tid
            npred_actual = npred
        else:
            # Mismo predio → agregamos solo si no está duplicado (por seguridad)
            if interesado_tid not in interesados_del_predio:
                interesados_del_predio.append(interesado_tid)

    # Último predio
    if interesados_del_predio:
        repartir_derechos_exactos(derechos_data, interesados_del_predio, predio_tid_actual)
        log_migracion(npred_actual, "cr_derecho", "MIGRADO", 
                     f"{len(interesados_del_predio)} titular(es) → suma 1.000000")

    # 3. Insertamos todos los derechos
    if derechos_data:
        # Detectar columnas
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='ladm' AND table_name='cr_derecho'")
        columnas = {r[0] for r in cursor.fetchall()}
        base = ['t_ili_tid', 'predio_t_ili_tid', 'tipo', 'interesado_t_ili_tid', 'fraccion_derecho', 'informalidad_tipo']
        extra = ['restriccion_tipo'] if 'restriccion_tipo' in columnas else []
        cols = base + extra

        sql_cols = ', '.join(cols)
        placeholders = ', '.join(['%s'] * len(cols))
        valores = [tuple(d[c] for c in cols) for d in derechos_data]

        cursor.executemany(f"INSERT INTO ladm.cr_derecho ({sql_cols}) VALUES ({placeholders})", valores)
        logger.info(f"INSERTADOS {len(derechos_data)} derechos → 1 por interesado → suma exacta 1.000000")
    conn.commit()


# ==============================================================
# FUNCIÓN AUXILIAR → SIEMPRE SUMA 1.000000
# ==============================================================
def repartir_derechos_exactos(derechos_data, interesados_list, predio_tid):
    total = len(interesados_list)
    if total == 0:
        return

    if total == 1:
        # DOMINIO PLENO PERFECTO
        derechos_data.append({
            't_ili_tid': str(uuid4()),
            'predio_t_ili_tid': predio_tid,
            'tipo': 'Dominio',
            'interesado_t_ili_tid': interesados_list[0],
            'fraccion_derecho': 1.000000,
            'informalidad_tipo': None,
            'restriccion_tipo': None
        })
        return

    # Múltiples interesados → método exacto con partes enteras
    partes = 1000000
    parte_base = partes // total
    resto = partes % total
    fracciones = [parte_base + 1 if i < resto else parte_base for i in range(total)]

    for i, p in enumerate(fracciones):
        fraccion = round(p / 1000000.0, 6)
        derechos_data.append({
            't_ili_tid': str(uuid4()),
            'predio_t_ili_tid': predio_tid,
            'tipo': 'Dominio',
            'interesado_t_ili_tid': interesados_list[i],
            'fraccion_derecho': fraccion,
            'informalidad_tipo': None,
            'restriccion_tipo': None
        })
# ==============================================================
# REPORTE FINAL
# ==============================================================
def generar_reporte_final():
    if not migracion_log:
        logger.warning("No se registraron eventos de migración.")
        return

    df = pd.DataFrame(migracion_log)
    df.to_csv(reporte_csv, index=False, encoding='utf-8-sig')

    resumen = df.groupby(['entidad', 'estado']).size().unstack(fill_value=0)
    total_predios = df['numero_predial'].nunique()

    with open(reporte_txt, 'a', encoding='utf-8') as f:
        f.write("\n" + "="*80 + "\n")
        f.write("RESUMEN FINAL DE MIGRACIÓN\n")
        f.write(f"Total predios únicos procesados: {total_predios}\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(resumen.to_string())
        f.write(f"\n\nArchivo detallado: {os.path.abspath(reporte_csv)}\n")
        f.write("="*80 + "\n")

    logger.info("REPORTE FINAL GENERADO CORRECTAMENTE")
    print("\n=== RESUMEN FINAL ===")
    print(resumen)
# ==============================================================
# MAIN
# ==============================================================
if __name__ == "__main__":
    conn = connect_db()
    if not conn:
        exit(1)

    logger.info("MIGRACIÓN COMPLETA CON REPORTE DETALLADO - INICIADA")
    try:
        migrate_predios(conn, r1_excel_path, r2_excel_path)
        migrate_interesados(conn, r1_excel_path)
        migrate_terrenos(conn, terrenos_shp_path)
        migrate_unidades(conn, unidades_shp_path, r2_excel_path)
        migrate_derechos(conn)  # ← Ahora SÍ funcionará

        generar_reporte_final()
        logger.info("MIGRACIÓN COMPLETA Y EXITOSA - REPORTE GENERADO")

    except Exception as e:
        conn.rollback()
        logger.error(f"ERROR CRÍTICO: {e}")
        generar_reporte_final()  # Genera reporte aunque falle
        raise
    finally:
        conn.close()