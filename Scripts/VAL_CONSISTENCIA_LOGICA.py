# VALIDADOR_LADM_COL_UNIFICADO.py
# Validación completa de calidad LADM-COL (Alfanumérico + IGAC IN-GCT-PC01-04)
# Autor: Tú + Grok (2025)

import os
import psycopg2
import pandas as pd
from datetime import datetime
import logging

# === CONFIGURACIÓN ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db_params = {
    "host": "localhost",
    "port": "5432",
    "dbname": "bd_ladm_29112025",
    "user": "postgres",
    "password": "postgres123"
}

schema = "ladm"
output_dir = "reportes_unificados"
os.makedirs(output_dir, exist_ok=True)

# === CONEXIÓN ===
def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(f"SET search_path TO {schema}, public;")
        conn.commit()
        cur.close()
        logger.info("Conexión a BD establecida y search_path configurado.")
        return conn
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        return None

# === REPORTE UNIFICADO ===
def generar_reporte(resultados, conteo_tablas, conteo_propietarios):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = os.path.join(output_dir, f"REPORTE_LADM_COMPLETO_{ts}.txt")
    csv_path = os.path.join(output_dir, f"REPORTE_LADM_COMPLETO_{ts}.csv")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("REPORTE COMPLETO DE CALIDAD LADM-COL\n")
        f.write("Validaciones Alfanuméricas + Instructivo IGAC IN-GCT-PC01-04\n")
        f.write("=" * 80 + "\n")
        f.write(f"Base de datos : {db_params['dbname']}\n")
        f.write(f"Fecha y hora  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -05\n")
        f.write(f"Usuario       : {os.getenv('USERNAME', 'unknown')}\n")
        f.write("=" * 80 + "\n\n")

        f.write("1. RESUMEN DE REGISTROS\n")
        f.write("-" * 50 + "\n")
        max_len = max(len(k) for k in conteo_tablas)
        for tabla, count in conteo_tablas.items():
            f.write(f"{tabla.ljust(max_len)} : {count:,} registro{'s' if count != 1 else ''}\n")
        f.write("\n")

        f.write("2. RESUMEN DE PROPIETARIOS\n")
        f.write("-" * 60 + "\n")
        f.write(f"Interesados únicos (por documento)          : {conteo_propietarios['interesados_unicos']:,}\n")
        f.write(f"Relaciones predio → interesado              : {conteo_propietarios['relaciones_directas']:,}\n")
        f.write(f"Predios con múltiples propietarios          : {conteo_propietarios['predios_multiples']:,}\n")
        f.write(f"Propietarios con múltiples predios          : {conteo_propietarios['propietarios_multiples']:,}\n")
        f.write("-" * 60 + "\n\n")

        bloques = [
            ("ADMINISTRATIVO", "validacion_administrativo"),
            ("JURÍDICO", "validacion_juridico"),
            ("FÍSICO", "validacion_fisico"),
            ("ALFANUMÉRICO GENERAL", "validacion_alfanumerico")
        ]

        for titulo, prefijo in bloques:
            f.write(f"{titulo}\n")
            f.write("=" * 70 + "\n")
            for regla, resultado in resultados.items():
                if regla.startswith(prefijo):
                    nombre_regla = regla.replace(prefijo + '_', '').replace('validacion_', '')
                    f.write(f"{nombre_regla}:\n{resultado}\n\n")
            f.write("\n")

    df = pd.DataFrame([
        {"Categoría": r.split("_", 1)[0] if "_" in r else "GENERAL", "Regla": r, "Resultado": res}
        for r, res in resultados.items()
    ])
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"Reporte completo generado:")
    logger.info(f"   TXT → {txt_path}")
    logger.info(f"   CSV → {csv_path}")

# === CONTEOS ===
def contar_registros(conn):
    cur = conn.cursor()
    tablas = {
        "cr_predio": f"SELECT COUNT(*) FROM {schema}.cr_predio",
        "cr_derecho": f"SELECT COUNT(*) FROM {schema}.cr_derecho",
        "cr_interesado": f"SELECT COUNT(*) FROM {schema}.cr_interesado",
        "cr_terreno": f"SELECT COUNT(*) FROM {schema}.cr_terreno",
        "cr_unidadconstruccion": f"SELECT COUNT(*) FROM {schema}.cr_unidadconstruccion"
    }
    conteo = {}
    for nombre, sql in tablas.items():
        cur.execute(sql)
        conteo[nombre] = cur.fetchone()[0]
    cur.close()
    return conteo

def contar_propietarios(conn):
    cur = conn.cursor()
    conteo = {k: 0 for k in ["interesados_unicos", "relaciones_directas", "predios_multiples", "propietarios_multiples"]}
    
    try:
        cur.execute(f"SELECT COUNT(DISTINCT numero_documento) FROM {schema}.cr_interesado WHERE numero_documento IS NOT NULL")
        conteo["interesados_unicos"] = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {schema}.cr_interesado WHERE predio_t_ili_tid IS NOT NULL")
        conteo["relaciones_directas"] = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT predio_t_ili_tid FROM {schema}.cr_interesado
                WHERE predio_t_ili_tid IS NOT NULL AND numero_documento IS NOT NULL
                GROUP BY predio_t_ili_tid HAVING COUNT(DISTINCT numero_documento) > 1
            ) sub
        """)
        conteo["predios_multiples"] = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT numero_documento FROM {schema}.cr_interesado
                WHERE numero_documento IS NOT NULL AND predio_t_ili_tid IS NOT NULL
                GROUP BY numero_documento HAVING COUNT(DISTINCT predio_t_ili_tid) > 1
            ) sub
        """)
        conteo["propietarios_multiples"] = cur.fetchone()[0]
    finally:
        cur.close()
    return conteo

# === VALIDACIONES COMPLETAS ===
def ejecutar_validaciones(conn):
    cur = conn.cursor()
    resultados = {}

    try:
        # 1. ADMINISTRATIVAS
        cur.execute(f"""
            SELECT numero_predial, SUBSTRING(numero_predial FROM 22 FOR 9)
            FROM {schema}.cr_predio
            WHERE SUBSTRING(numero_predial FROM 22 FOR 9) NOT IN ('000000000','100000000','200000000','300000000')
              AND tipo_predio IN ('NPH', 'Uso_Publico', 'Bien_de_uso_publico', 'Lote_de_engorde')
        """)
        err = cur.fetchall()
        resultados["validacion_administrativo_NPN 22-30 no estandarizado"] = f"{len(err)} errores:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Todos correctos."

        cur.execute(f"SELECT numero_predial FROM {schema}.cr_predio WHERE SUBSTRING(numero_predial FROM 22 FOR 1) IN ('1','5','6')")
        err = cur.fetchall()
        resultados["validacion_administrativo_Campo 22 inválido (1,5,6)"] = f"{len(err)} inválidos:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Todos válidos."

        cur.execute(f"""
            SELECT p.numero_predial FROM {schema}.cr_predio p
            JOIN {schema}.cr_unidadconstruccion u ON u.numero_predial = p.numero_predial
            WHERE p.destinacion_economica = 'Lote_Urbanizado_No_Construido'
        """)
        err = cur.fetchall()
        resultados["validacion_administrativo_Lote urbanizado no construido con UC"] = f"{len(err)} errores:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Correcto."

        # 2. JURÍDICAS
        cur.execute(f"""
            SELECT p.numero_predial, COUNT(d.t_id) AS total_derechos, MAX(d.fraccion_derecho)
            FROM {schema}.cr_derecho d
            JOIN {schema}.cr_predio p ON d.predio_t_ili_tid = p.t_ili_tid
            GROUP BY p.numero_predial
            HAVING 
                (COUNT(d.t_id) = 1 AND MAX(d.fraccion_derecho) < 0.99) OR
                (COUNT(d.t_id) > 1 AND MAX(d.fraccion_derecho) >= 0.99)
        """)
        err = cur.fetchall()
        resultados["validacion_juridico_Dominio pleno ≠ fracción 1"] = \
            f"{len(err)} errores graves (fuera de tolerancia):\n" + \
            "\n".join([f"   → {r[0]} | {r[1]} titular(es) | fracción máx={r[2]}" for r in err]) \
            if err else "Todos correctos (tolerancia aplicada: dominio pleno ≥0.99, múltiples <0.99)."

        cur.execute(f"""
            SELECT p.numero_predial, ROUND(SUM(d.fraccion_derecho)::numeric, 6) AS suma
            FROM {schema}.cr_derecho d
            JOIN {schema}.cr_predio p ON d.predio_t_ili_tid = p.t_ili_tid
            GROUP BY p.numero_predial
            HAVING ABS(SUM(d.fraccion_derecho) - 1.0) > 0.02
        """)
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Fracciones ≠ 1 (tolerancia ±0.02)"] = \
            f"{len(err)} predios fuera de rango (±0.02):\n" + \
            "\n".join([f"   → {r[0]} suma={r[1]}" for r in err]) \
            if err else "Todas las fracciones suman entre 0.98 y 1.02 → ACEPTABLE SEGÚN IGAC."

        cur.execute(f"""
            SELECT p.numero_predial FROM {schema}.cr_derecho d
            JOIN {schema}.cr_predio p ON d.predio_t_ili_tid = p.t_ili_tid
            WHERE d.informalidad_tipo = 'Posesion' AND p.tipo_predio NOT IN ('Privado', 'Privado_Informal')
        """)
        err = cur.fetchall()
        resultados["validacion_juridico_Posesión en predio no privado"] = f"{len(err)} errores:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Correcto."

        cur.execute(f"SELECT numero_documento FROM {schema}.cr_interesado WHERE numero_documento ~ '[^0-9]' OR numero_documento = '0'")
        err = cur.fetchall()
        resultados["validacion_juridico_Documento no numérico o cero"] = f"{len(err)} inválidos:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Todos válidos."

        cur.execute(f"""
            SELECT numero_predial FROM {schema}.cr_predio p
            LEFT JOIN {schema}.cr_interesado i ON i.predio_t_ili_tid = p.t_ili_tid
            WHERE i.t_ili_tid IS NULL
        """)
        err = cur.fetchall()
        resultados["validacion_juridico_Predio sin interesado"] = f"{len(err)} predios sin titular:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Todos tienen interesado."

        # 3. FÍSICAS
        cur.execute(f"""
            SELECT t.numero_predial, t.area, ROUND(ST_Area(t.geometria)::numeric, 2)
            FROM {schema}.cr_terreno t
            WHERE ABS(t.area - ST_Area(t.geometria)) > 1
        """)
        err = cur.fetchall()
        resultados["validacion_fisico_Área terreno ≠ área geométrica (>1m²)"] = \
            f"{len(err)} diferencias:\n" + "\n".join([f"   → {r[0]} | BD={r[1]} | Geom={r[2]}" for r in err]) \
            if err else "Todas coinciden (±1 m²)."

        cur.execute(f"SELECT numero_predial FROM {schema}.cr_unidadconstruccion WHERE planta_ubicacion IS NULL OR planta_ubicacion <= 0")
        err = cur.fetchall()
        resultados["validacion_fisico_Planta ubicación ≤ 0"] = f"{len(err)} inválidas:\n" + "\n".join([f"   → {r[0]}" for r in err]) if err else "Todas válidas."

        # 4. ALFANUMÉRICAS GENERALES
        cur.execute(f"SELECT numero_predial, COUNT(*) FROM {schema}.cr_predio GROUP BY numero_predial HAVING COUNT(*) > 1")
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Duplicados NPN"] = f"{len(err)} duplicados:\n" + "\n".join([f"   → {r[0]} ({r[1]} veces)" for r in err]) if err else "Sin duplicados."

        cur.execute(f"""
            SELECT p.numero_predial, d.fraccion_derecho
            FROM {schema}.cr_derecho d
            JOIN {schema}.cr_predio p ON d.predio_t_ili_tid = p.t_ili_tid
            WHERE d.fraccion_derecho <= 0 OR d.fraccion_derecho > 1
        """)
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Fracción fuera 0-1"] = f"{len(err)} inválidas:\n" + "\n".join([f"   → {r[0]} = {r[1]}" for r in err]) if err else "Todas válidas."

        cur.execute(f"SELECT t_id, departamento FROM {schema}.cr_predio WHERE departamento IS NULL OR LENGTH(departamento) <> 2 OR departamento !~ '^[0-9]+$'")
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Departamento inválido"] = f"{len(err)} errores:\n" + "\n".join([f"   → t_id={r[0]} '{r[1]}'" for r in err]) if err else "Todos válidos."

        cur.execute(f"SELECT t_id, municipio FROM {schema}.cr_predio WHERE municipio IS NULL OR LENGTH(municipio) <> 3 OR municipio !~ '^[0-9]+$'")
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Municipio inválido"] = f"{len(err)} errores:\n" + "\n".join([f"   → t_id={r[0]} '{r[1]}'" for r in err]) if err else "Todos válidos."

        cur.execute(f"SELECT t_id, numero_documento FROM {schema}.cr_interesado WHERE tipo_interesado = 'Persona_Natural' AND razon_social IS NOT NULL")
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Natural con razón social"] = f"{len(err)} errores:\n" + "\n".join([f"   → doc={r[1]}" for r in err]) if err else "Correcto."

        cur.execute(f"SELECT t_id, numero_documento FROM {schema}.cr_interesado WHERE tipo_interesado = 'Persona_Juridica' AND (primer_nombre IS NOT NULL OR primer_apellido IS NOT NULL)")
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Jurídica con nombres"] = f"{len(err)} errores:\n" + "\n".join([f"   → doc={r[1]}" for r in err]) if err else "Correcto."

        cur.execute(f"""
            SELECT p.numero_predial, p.tipo_predio FROM {schema}.cr_predio p
            LEFT JOIN {schema}.cr_prediotipo t ON p.tipo_predio = t.ilicode
            WHERE p.tipo_predio IS NOT NULL AND t.ilicode IS NULL
        """)
        err = cur.fetchall()
        resultados["validacion_alfanumerico_tipo_predio fuera dominio"] = f"{len(err)} inválidos:\n" + "\n".join([f"   → {r[0]} tipo='{r[1]}'" for r in err]) if err else "Todos válidos."

        cur.execute(f"""
            SELECT tipo_documento, numero_documento, COUNT(*)
            FROM {schema}.cr_interesado
            WHERE numero_documento IS NOT NULL
            GROUP BY tipo_documento, numero_documento HAVING COUNT(*) > 1
        """)
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Documentos duplicados"] = f"{len(err)} duplicados:\n" + "\n".join([f"   → {r[0]} {r[1]} ({r[2]} veces)" for r in err]) if err else "Sin duplicados."

        cur.execute(f"""
            SELECT 'terreno' AS tipo, t.numero_predial FROM {schema}.cr_terreno t
            LEFT JOIN {schema}.cr_predio p ON t.numero_predial = p.numero_predial 
            WHERE p.numero_predial IS NULL
            UNION ALL
            SELECT 'unidad_construccion' AS tipo, u.numero_predial FROM {schema}.cr_unidadconstruccion u
            LEFT JOIN {schema}.cr_predio p ON u.numero_predial = p.numero_predial 
            WHERE p.numero_predial IS NULL
            LIMIT 50
        """)
        err = cur.fetchall()
        resultados["validacion_alfanumerico_Huérfanos físico"] = f"{len(err)} registros huérfanos:\n" + "\n".join([f"   → {r[0]}: {r[1]}" for r in err]) if err else "Ninguno."

        logger.info("Todas las validaciones ejecutadas correctamente.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error crítico en validaciones: {e}")
        resultados["ERROR_CRITICO"] = str(e)
    finally:
        cur.close()

    return resultados

# === MAIN ===
def main():
    conn = connect_db()
    if not conn:
        return

    try:
        conteo_tablas = contar_registros(conn)
        conteo_prop = contar_propietarios(conn)
        resultados = ejecutar_validaciones(conn)
        generar_reporte(resultados, conteo_tablas, conteo_prop)
    finally:
        conn.close()
        logger.info("Validación completa finalizada.")

if __name__ == "__main__":
    main()