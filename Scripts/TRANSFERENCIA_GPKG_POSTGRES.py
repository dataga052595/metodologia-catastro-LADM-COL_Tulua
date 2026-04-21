import os
import psycopg2
from psycopg2 import Error
import geopandas as gpd
import pandas as pd
from datetime import datetime
import logging
import uuid

# ==============================================================
# CONFIGURACIÓN
# ==============================================================

db_params = {
    "host": "localhost",
    "port": "5432",
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres123"),
    "dbname": "bd_ladm_29112025"
}

# <<<--- CAMBIA ESTA RUTA POR TU GEOPACKAGE DE QFIELD --->>>
GPKG_PATH = r"C:/Users/dan05/Desktop/ENTREGA_06122025/SYNCRONIZAR/ASIGNACION_22122025/data.gpkg"

# Orden importante: tablas independientes primero, dependientes después
TABLAS_SINCRONIZAR = [
    "cr_predio",
    "cr_interesado",
    "cr_caracteristicasunidadconstruccion",  # ahora con nombre correcto
    "cr_fuenteadministrativa",
    "cr_derecho",
    "cr_terreno",
    "cr_unidadconstruccion",
    "extdireccion",
    "cr_contactovisita",
    "cr_adjuntocaracteristica",
    "cc_adjunto",               # asumiendo que es cca_adjunto o similar
    "cr_derecho_fuente"
]

# Columnas a ignorar en comparación (claves internas, timestamps automáticos, etc.)
COLUMNAS_IGNORAR = [
    "t_id", "comienzo_vida_util_version", "fin_vida_util_version"
]

# ==============================================================
# REPORTE
# ==============================================================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
reporte_csv = f"REPORTE_SINCRONIZACION_GPKG_{timestamp}.csv"
reporte_txt = f"REPORTE_SINCRONIZACION_GPKG_{timestamp}.txt"

sync_log = []

def log_sync(tabla, t_ili_tid, accion, motivo=""):
    sync_log.append({
        "tabla": tabla,
        "t_ili_tid": str(t_ili_tid) if t_ili_tid else "NUEVO",
        "accion": accion,
        "motivo": str(motivo)[:200],
        "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(reporte_txt, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================
# CONEXIONES
# ==============================================================
def connect_postgres():
    try:
        conn = psycopg2.connect(**db_params)
        logger.info("Conexión a PostgreSQL exitosa.")
        return conn
    except Error as e:
        logger.error(f"Error conectando a PostgreSQL: {e}")
        raise

# ==============================================================
# SINCRONIZACIÓN POR TABLA
# ==============================================================
def sincronizar_tabla(conn, tabla):
    logger.info(f"--- Sincronizando tabla: {tabla} ---")
    
    # Leer desde GeoPackage
    try:
        gdf = gpd.read_file(GPKG_PATH, layer=tabla)
    except Exception as e:
        logger.warning(f"No se encontró la capa '{tabla}' en el GPKG. Saltando. Error: {e}")
        return
    
    if gdf.empty:
        logger.info(f"Capa {tabla} vacía en GPKG.")
        return

    cursor = conn.cursor()

    # Obtener estructura de la tabla en PostgreSQL
    cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'ladm' AND table_name = '{tabla}'
        ORDER BY ordinal_position
    """)
    columnas_pg = [row[0] for row in cursor.fetchall()]

    # Leer datos actuales en PostgreSQL
    cursor.execute(f'SELECT * FROM ladm."{tabla}"')
    filas_pg = cursor.fetchall()
    df_pg = pd.DataFrame(filas_pg, columns=columnas_pg)

    # Normalizar t_ili_tid a string
    if 't_ili_tid' in gdf.columns:
        gdf['t_ili_tid'] = gdf['t_ili_tid'].astype(str).replace('nan', None)
    if 't_ili_tid' in df_pg.columns:
        df_pg['t_ili_tid'] = df_pg['t_ili_tid'].astype(str).replace('nan', None)

    gpkg_tids = set(gdf['t_ili_tid'].dropna())
    pg_tids = set(df_pg['t_ili_tid'].dropna())

    nuevos_tids = gpkg_tids - pg_tids
    existentes_tids = gpkg_tids & pg_tids

    # Columnas para comparar (excluyendo ignoradas y geometría)
    columnas_comparar = [c for c in columnas_pg if c not in COLUMNAS_IGNORAR and c != 'geometria']

    inserts = []
    updates = []

    # === INSERTS: registros nuevos ===
    for _, row in gdf[gdf['t_ili_tid'].isin(nuevos_tids)].iterrows():
        valores = []
        for col in columnas_pg:
            if col == 'geometria' and not row.geometry is None:
                valores.append(row.geometry.wkt)
            elif col == 't_ili_tid' and row.get(col) is None:
                valores.append(str(uuid.uuid4()))
            else:
                valores.append(row.get(col))
        inserts.append(tuple(valores))
        log_sync(tabla, row.get('t_ili_tid'), "INSERT", "Nuevo desde campo")

    # === UPDATES: registros modificados ===
    for tid in existentes_tids:
        row_gpkg = gdf[gdf['t_ili_tid'] == tid].iloc[0]
        row_pg = df_pg[df_pg['t_ili_tid'] == tid].iloc[0]

        modificado = False
        update_fields = []
        update_values = []

        for col in columnas_comparar:
            val_gpkg = row_gpkg.get(col)
            val_pg = row_pg.get(col)

            if pd.isna(val_gpkg): val_gpkg = None
            if pd.isna(val_pg): val_pg = None

            if val_gpkg != val_pg:
                modificado = True
                update_fields.append(f'"{col}" = %s')
                update_values.append(val_gpkg)

        # Geometría separada
        if 'geometria' in columnas_pg and not row_gpkg.geometry is None:
            wkt_gpkg = row_gpkg.geometry.wkt
            if wkt_gpkg != row_pg.get('geometria'):
                modificado = True
                update_fields.append('geometria = ST_GeomFromText(%s, 9377)')
                update_values.append(wkt_gpkg)

        if modificado:
            update_values.append(tid)  # para el WHERE
            updates.append((update_fields, update_values))
            log_sync(tabla, tid, "UPDATE", "Campos modificados en campo")

    # === EJECUTAR INSERTS ===
    if inserts:
        placeholders = ', '.join(['%s'] * len(columnas_pg))
        columnas_quoted = ', '.join([f'"{c}"' for c in columnas_pg])
        sql_insert = f'INSERT INTO ladm."{tabla}" ({columnas_quoted}) VALUES ({placeholders})'

        try:
            cursor.executemany(sql_insert, inserts)
            conn.commit()
            logger.info(f"{len(inserts)} registros INSERTADOS en {tabla}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en INSERT {tabla}: {e}")
            for ins in inserts:
                tid = ins[columnas_pg.index('t_ili_tid')] if 't_ili_tid' in columnas_pg else "NUEVO"
                log_sync(tabla, tid, "ERROR_INSERT", str(e))

    # === EJECUTAR UPDATES ===
    if updates:
        for fields, values in updates:
            sql_update = f'UPDATE ladm."{tabla}" SET {", ".join(fields)} WHERE t_ili_tid = %s'
            try:
                cursor.execute(sql_update, values)
            except Exception as e:
                conn.rollback()
                logger.error(f"Error en UPDATE {tabla}: {e}")
                log_sync(tabla, values[-1], "ERROR_UPDATE", str(e))
        conn.commit()
        logger.info(f"{len(updates)} registros ACTUALIZADOS en {tabla}")

    cursor.close()

# ==============================================================
# REPORTE FINAL
# ==============================================================
def generar_reporte():
    if not sync_log:
        logger.info("No se detectaron cambios para sincronizar.")
        return
    
    df = pd.DataFrame(sync_log)
    df.to_csv(reporte_csv, index=False, encoding='utf-8-sig')
    
    resumen = df.groupby(['tabla', 'accion']).size().unstack(fill_value=0)
    
    with open(reporte_txt, 'a', encoding='utf-8') as f:
        f.write("\n" + "="*80 + "\n")
        f.write("RESUMEN DE SINCRONIZACIÓN DESDE GEOPACKAGE\n")
        f.write(f"Archivo: {os.path.basename(GPKG_PATH)}\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(resumen.to_string())
        f.write(f"\n\nReporte detallado: {os.path.abspath(reporte_csv)}\n")
        f.write("="*80 + "\n")
    
    logger.info("REPORTE GENERADO CORRECTAMENTE")
    print("\n=== RESUMEN DE SINCRONIZACIÓN ===")
    print(resumen)

# ==============================================================
# MAIN
# ==============================================================
if __name__ == "__main__":
    if not os.path.exists(GPKG_PATH):
        logger.error(f"Archivo GPKG no encontrado: {GPKG_PATH}")
        exit(1)

    conn = connect_postgres()
    try:
        for tabla in TABLAS_SINCRONIZAR:
            sincronizar_tabla(conn, tabla)
        
        generar_reporte()
        logger.info("¡SINCRONIZACIÓN COMPLETADA CON ÉXITO!")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error crítico: {e}")
        generar_reporte()
        raise
    finally:
        conn.close()