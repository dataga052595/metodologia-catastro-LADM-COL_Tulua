import os
import psycopg2
from psycopg2 import Error
import pandas as pd
import geopandas as gpd
from uuid import uuid4
import logging
import numpy as np
from shapely.geometry import MultiPolygon

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIGURACIÓN ===
db_params = {
    "host": "localhost",
    "port": "5432",
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres123"),
    "dbname": "bd_ladm_23012026"
}

# Rutas
r1_excel_path = 'C:/Users/dan05/Desktop/ENTREGA_06122025/DATOS_TULUA/SHP_12112025/76834_R1_ZE_DEP.xlsx'
r2_excel_path = 'C:/Users/dan05/Desktop/ENTREGA_06122025/DATOS_TULUA/SHP_12112025/76834_R2_ZE_DEP.xlsx'
terrenos_shp_path = 'C:/Users/dan05/Desktop/ENTREGA_06122025/DATOS_TULUA/SHP_12112025/TERRENOS.shp'
unidades_shp_path = 'C:/Users/dan05/Desktop/ENTREGA_06122025/DATOS_TULUA/SHP_12112025/UNIDADES.shp'

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

# === 1. MIGRAR PREDIOS ===
def migrate_predios(conn, r1_path, r2_path):
    # Cargar R1
    r1_df = pd.read_excel(r1_path)
    r1_df.columns = r1_df.columns.str.lower()
    r1_df['numero_predial'] = r1_df['numero_predial'].astype(str).str.strip().str.zfill(30)

    # Cargar R2 Hoja1
    r2_df = pd.read_excel(r2_path, sheet_name='Hoja1')
    r2_df.columns = r2_df.columns.str.lower()
    r2_df['numero_predial'] = r2_df['numero_predial'].astype(str).str.strip().str.zfill(30)
    r2_grouped = r2_df.groupby('numero_predial').first().reset_index()
    r2_dict = r2_grouped.set_index('numero_predial')[['orip', 'matricula']].to_dict('index')

    cursor = conn.cursor()
    predios_dict = load_predios(cursor)
    insert_data = []

    r1_grouped = r1_df.groupby('numero_predial').first().reset_index()

    for _, row in r1_grouped.iterrows():
        npred = row['numero_predial']
        if npred in predios_dict:
            continue

        t_ili_tid = str(uuid4())

        # DEPARTAMENTO Y MUNICIPIO
        dept = npred[0:2]
        mun = npred[2:5]

        # ORIP (entero)
        r2_row = r2_dict.get(npred, {})
        orip_raw = r2_row.get('orip')
        orip = int(orip_raw) if pd.notna(orip_raw) and orip_raw != '' else 0

        # MATRÍCULA
        matricula = str(r2_row.get('matricula', '')) if r2_row else ''

        # AVALUO CATASTRAL
        avaluo = round(float(row.get('avaluo_catastral', 0)) or 0.0, 2)

        # DESTINACIÓN
        codigo = str(row.get('destino_economico', '')).strip().upper()
        destinacion = destino_map.get(codigo, DEFAULT_DESTINACION)

        # ÁREA
        area_catastral = round(float(row.get('area_terreno', 0)) or 0.0, 2)

        # TIPO Y CONDICIÓN
        digito_22 = npred[21]  # Posición 22
        condicion, tipo_predio = condicion_map.get(digito_22, ('NPH', 'Privado'))

        insert_data.append((
            t_ili_tid, npred, dept, mun, orip, matricula, destinacion,
            area_catastral, avaluo, 'Activo', tipo_predio, condicion
        ))

    if insert_data:
        cursor.executemany("""
            INSERT INTO ladm.cr_predio 
            (t_ili_tid, numero_predial, departamento, municipio, Codigo_ORIP, Matricula_inmobiliaria,
             destinacion_economica, area_catastral, avaluo_catastral, estado, tipo_predio, condicion_predio)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, insert_data)
        logger.info(f"{len(insert_data)} predios insertados.")
    conn.commit()

# === 2. MIGRAR INTERESADOS ===
def migrate_interesados(conn, r1_path):
    df = pd.read_excel(r1_path)
    df.columns = df.columns.str.lower()
    cursor = conn.cursor()
    predios_dict = load_predios(cursor)
    insert_data = []

    for _, row in df.iterrows():
        npred = str(row['numero_predial']).strip().zfill(30)
        if npred not in predios_dict:
            continue

        t_ili_tid = str(uuid4())

        # TIPO DOCUMENTO
        tipo_doc_raw = str(row.get('tipo_documento', '')).strip().upper()
        tipo_doc = doc_tipo_map.get(tipo_doc_raw, None)
        if not tipo_doc:
            continue

        # TIPO INTERESADO
        tipo_interesado = 'Persona_Natural' if tipo_doc != 'NIT' else 'Persona_Juridica'

        # === CAMPOS: VACÍO → NULL ===
        def clean_value(val):
            if pd.isna(val) or str(val).strip() == '' or str(val).lower() == 'nan':
                return None
            return str(val).strip()

        num_doc = clean_value(row.get('numero_documento'))
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

        insert_data.append((
            t_ili_tid, tipo_interesado, tipo_doc, num_doc, p_nombre, s_nombre,
            p_apellido, s_apellido, razon_social, sexo, predios_dict[npred]
        ))

    if insert_data:
        cursor.executemany("""
            INSERT INTO ladm.cr_interesado 
            (t_ili_tid, tipo_interesado, tipo_documento, numero_documento, primer_nombre, segundo_nombre,
             primer_apellido, segundo_apellido, razon_social, sexo, predio_t_ili_tid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, insert_data)
        logger.info(f"{len(insert_data)} interesados insertados.")
    conn.commit()


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
            continue
        if geom.geom_type == 'Polygon':
            geom = MultiPolygon([geom])
        area = round(geom.area, 2)

        unidad_data.append((
            t_ili_tid_uni,
            predio_tid,           # ← RELACIÓN DIRECTA
            npred,
            tipo_planta,
            planta_ubicacion,
            altura,
            geom.wkt,
            area,
            t_ili_tid_car,
            'En_Rasante'
        ))

    # === INSERTAR ===
    if caracteristicas_data:
        cursor.executemany("""
            INSERT INTO ladm.cr_caracteristicasunidadconstruccion
            (t_ili_tid, identificador, tipo_unidad, total_plantas, total_habitaciones,
             total_banios, total_locales, uso, area_construida, estado_conservacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, caracteristicas_data)
        logger.info(f"{len(caracteristicas_data)} características únicas creadas (por identificador + uso del SHP)")

    if unidad_data:
        cursor.executemany("""
            INSERT INTO ladm.cr_unidadconstruccion
            (t_ili_tid, predio_t_ili_tid, numero_predial, tipo_planta, planta_ubicacion,
             altura, geometria, area, caracteristicas_t_ili_tid, Relacion_Superficie)
            VALUES (%s, %s, %s, %s, %s, %s, ST_MakeValid(ST_GeomFromText(%s, 9377)), %s, %s, %s)
        """, unidad_data)
        logger.info(f"{len(unidad_data)} unidades de construcción migradas con uso homologado desde SHP")

    conn.commit()
    logger.info("Migración de unidades completada: uso desde SHP + característica única por grupo")

# === 6. MIGRAR DERECHOS - FRACCIÓN EXACTA 100% (SUMA SIEMPRE = 1.000000) ===
def migrate_derechos(conn):
    cursor = conn.cursor()

    # Obtener interesados únicos por predio (evita duplicados)
    cursor.execute("""
        SELECT DISTINCT
            p.t_ili_tid AS predio_tid,
            i.t_ili_tid AS interesado_tid
        FROM ladm.cr_predio p
        JOIN ladm.cr_interesado i ON p.t_ili_tid = i.predio_t_ili_tid
        ORDER BY p.t_ili_tid
    """)
    rows = cursor.fetchall()

    # Derechos como diccionarios para soportar inserciones dinámicas según columnas existentes
    derechos_data = []
    predio_actual = None
    interesados = []

    for predio_tid, interesado_tid in rows:
        if predio_actual != predio_tid:
            if interesados:
                _insertar_derechos_exactos(derechos_data, interesados, predio_actual)
            interesados = []
            predio_actual = predio_tid
        interesados.append(interesado_tid)

    # Último predio
    if interesados:
        _insertar_derechos_exactos(derechos_data, interesados, predio_actual)

    if derechos_data:
        # Obtener columnas reales de la tabla para armar SQL dinámico
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'ladm' AND table_name = 'cr_derecho'
        """)
        existing_columns = {row[0] for row in cursor.fetchall()}

        # Columnas que queremos insertar por orden
        base_columns = ['t_ili_tid', 'predio_t_ili_tid', 'tipo', 'interesado_t_ili_tid', 'fraccion_derecho', 'informalidad_tipo']
        insert_columns = base_columns.copy()
        if 'restriccion_tipo' in existing_columns:
            insert_columns.append('restriccion_tipo')

        # Construir SQL dinámico e iterables acorde a columnas disponibles
        cols_sql = ', '.join(insert_columns)
        placeholders = ', '.join(['%s'] * len(insert_columns))
        tuples = [tuple(d.get(c) for c in insert_columns) for d in derechos_data]

        cursor.executemany(f"INSERT INTO ladm.cr_derecho ({cols_sql}) VALUES ({placeholders})", tuples)
        logger.info(f"{len(derechos_data)} derechos insertados (fracción exacta - suma 1.000000)")
    conn.commit()

# === FUNCIÓN MÁGICA: SUMA SIEMPRE 1.000000 ===
def _insertar_derechos_exactos(derechos_data, interesados_list, predio_tid):
    total = len(interesados_list)
    if total == 0:
        return

    # Fracción con máxima precisión (12 decimales)
    fraccion_exacta = 1.0 / total
    
    suma = 0.0
    for i, interesado_tid in enumerate(interesados_list):
        if i == total - 1:
            # El último se lleva TODO lo que falte → suma EXACTA 1.000000
            fraccion = round(1.0 - suma, 6)
        else:
            fraccion = round(fraccion_exacta, 6)
            suma += fraccion

        t_ili_tid = str(uuid4())
        derechos_data.append({
            't_ili_tid': t_ili_tid,
            'predio_t_ili_tid': predio_tid,
            'tipo': 'Dominio',
            'interesado_t_ili_tid': interesado_tid,
            'fraccion_derecho': fraccion,
            'informalidad_tipo': None,
            'restriccion_tipo': None  # Puede ser ignorado si no existe en DB
        })

# === MAIN ===
if __name__ == "__main__":
    conn = connect_db()
    if not conn:
        exit(1)

    try:
        migrate_predios(conn, r1_excel_path, r2_excel_path)
        migrate_interesados(conn, r1_excel_path)
        migrate_terrenos(conn, terrenos_shp_path)
        migrate_unidades(conn, unidades_shp_path, r2_excel_path)
        # === NUEVO: MIGRAR DERECHOS ===
        migrate_derechos(conn)
        logger.info("MIGRACIÓN COMPLETA Y EXITOSA.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error crítico: {e}")
        raise
    finally:
        conn.close()