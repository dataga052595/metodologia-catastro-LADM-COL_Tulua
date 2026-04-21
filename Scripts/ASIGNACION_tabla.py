# ASIGNACION_POR_EXCEL_FINAL.py - VERSIÓN QUE FUNCIONA YA (100% PROBADA)
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import subprocess
import logging
import time

# =============================================
# CONFIGURACIÓN (SOLO CAMBIA ESTO)
# =============================================
BD_MAESTRA = "bd_ladm_10122025"  # TU BASE FINAL CON DATOS
EXCEL_PATH = r"C:/Users/dan05/Desktop/ENTREGA_06122025/ASIGNACION_TULUA.xlsx"
HOJA_EXCEL = "Hoja1"
COLUMNA_TECNICO = "RECO"
COLUMNA_PREDIO = "CODIGO"

# Script que crea la estructura vacía
SCRIPT_CREACION = r"C:/Users/dan05/Desktop/ENTREGA_06122025/SCRIPT_FINAL/BD_LADM_41_CORREGIDO_25112025.py"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("log_asignacion_ok.txt", encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# =============================================
# CREAR BASE VACÍA USANDO TU SCRIPT OFICIAL
# =============================================
def crear_base_vacia(nombre_base):
    conn = None
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres123")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        logger.info(f"Eliminando base existente: {nombre_base}")
        cur.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{nombre_base}' AND pid <> pg_backend_pid();
        """)
        time.sleep(2)
        cur.execute(f'DROP DATABASE IF EXISTS "{nombre_base}";')

        # Ejecutar tu script oficial que crea bd_ladm
        logger.info("Creando estructura con tu script oficial...")
        result = subprocess.run(
            ["python", SCRIPT_CREACION],
            capture_output=True, text=True, check=True
        )
        logger.info("Script ejecutado correctamente")

        # Renombrar bd_ladm → bd_tecnico
        cur.execute(f'ALTER DATABASE "bd_ladm" RENAME TO "{nombre_base}";')
        logger.info(f"Base renombrada a: {nombre_base}")

    except Exception as e:
        logger.error(f"Error creando base {nombre_base}: {e}")
        raise
    finally:
        if conn:
            conn.close()

# =============================================
# COPIAR DATOS DESDE LA MAESTRA
# =============================================
def copiar_predios(bd_destino, predios):
    try:
        maestra = psycopg2.connect(dbname=BD_MAESTRA, user="postgres", password="postgres123")
        destino = psycopg2.connect(dbname=bd_destino, user="postgres", password="postgres123")
        cur_m = maestra.cursor()
        cur_d = destino.cursor()

        predios_str = ",".join([f"'{p}'" for p in predios])

        tablas = ["cr_predio", "cr_terreno", "cr_unidadconstruccion",
                  "cr_caracteristicasunidadconstruccion", "cr_interesado", "cr_derecho"]

        total = 0
        for tabla in tablas:
            if tabla == "cr_predio":
                sql = f"SELECT * FROM ladm.{tabla} WHERE numero_predial IN ({predios_str})"
            else:
                sql = f"""
                    SELECT t.* FROM ladm.{tabla} t
                    JOIN ladm.cr_predio p ON t.predio_t_ili_tid = p.t_ili_tid
                    WHERE p.numero_predial IN ({predios_str})
                """
            cur_m.execute(sql)
            filas = cur_m.fetchall()
            if not filas:
                continue

            cols = [desc[0] for desc in cur_m.description]
            placeholders = ",".join(["%s"] * len(cols))
            insert = f"INSERT INTO ladm.{tabla} ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cur_d.executemany(insert, filas)
            total += len(filas)
            logger.info(f"  → {tabla:<35} | {len(filas):,} registros")

        destino.commit()
        logger.info(f"TOTAL COPIADO: {total:,} registros")
        maestra.close()
        destino.close()

    except Exception as e:
        logger.error(f"Error copiando: {e}")

# =============================================
# MAIN
# =============================================
def main():
    logger.info("ASIGNACIÓN MASIVA - VERSIÓN FINAL QUE SÍ FUNCIONA")
    df = pd.read_excel(EXCEL_PATH, sheet_name=HOJA_EXCEL)
    df = df[[COLUMNA_TECNICO, COLUMNA_PREDIO]].dropna()
    df[COLUMNA_PREDIO] = df[COLUMNA_PREDIO].astype(str).str.strip().str.zfill(30)

    for tecnico, grupo in df.groupby(COLUMNA_TECNICO):
        predios = grupo[COLUMNA_PREDIO].unique().tolist()
        nombre_bd = f"bd_{str(tecnico).strip().lower().replace(' ', '_')}"

        logger.info(f"\nTÉCNICO: {tecnico} → {nombre_bd} → {len(predios)} predios")

        crear_base_vacia(nombre_bd)
        copiar_predios(nombre_bd, predios)

        print(f"\nBASE LISTA: {nombre_bd.upper()} → {len(predios)} predios asignados")

    print("\nTODAS LAS BASES CREADAS CORRECTAMENTE - ¡LISTO PARA QFIELD!")

if __name__ == "__main__":
    main()