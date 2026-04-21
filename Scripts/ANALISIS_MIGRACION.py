# ANALISIS_MIGRACION.py
# Script para generar reporte estadístico de la migración LADM-COL en PostgreSQL
# Genera un archivo TXT con tablas y porcentajes clave

import os
import warnings
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

# ==============================
# CONFIGURACIÓN
# ==============================
DB_USER = "postgres"           # Cambia si es necesario
DB_PASSWORD = "postgres123"    # Cambia si es necesario
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bd_ladm_24012026"

# Nombre del archivo de salida con timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"analisis_migracion_{timestamp}.txt"

# Suprimir el warning de pandas + psycopg2 (opcional si usas SQLAlchemy)
warnings.filterwarnings("ignore", category=UserWarning)

def connect_db():
    """Conexión usando SQLAlchemy (recomendado para evitar warnings)"""
    connection_string = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    try:
        engine = create_engine(connection_string)
        print("Conexión SQLAlchemy exitosa a la base de datos.")
        return engine
    except Exception as e:
        print(f"Error al conectar con SQLAlchemy: {e}")
        return None


def execute_query(engine, query, title):
    """Ejecuta una consulta y devuelve DataFrame + título"""
    try:
        df = pd.read_sql_query(query, engine)
        if df.empty:
            df = pd.DataFrame({"Resultado": ["(sin registros)"]})
        return df, title
    except Exception as e:
        print(f"Error ejecutando consulta '{title}': {e}")
        return None, title


def write_report(results):
    """Genera el archivo TXT con formato legible"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("═" * 80 + "\n")
        f.write("       ANÁLISIS ESTADÍSTICO DE LA MIGRACIÓN LADM-COL\n")
        f.write("═" * 80 + "\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Base de datos: {DB_NAME}\n")
        f.write(f"Script: ANALISIS_MIGRACION.py\n")
        f.write("═" * 80 + "\n\n")

        for df, title in results:
            if df is not None:
                f.write(f" {title.upper()} \n")
                f.write("-" * 70 + "\n")
                f.write(df.to_string(index=False) + "\n\n")
            else:
                f.write(f" {title.upper()} \n")
                f.write("-" * 70 + "\n")
                f.write("(Error al ejecutar la consulta)\n\n")

        f.write("═" * 80 + "\n")
        f.write("Fin del reporte\n")
        f.write("═" * 80 + "\n")

    abs_path = os.path.abspath(output_file)
    print(f"\nReporte generado correctamente:")
    print(f"→ {abs_path}")
    print("Puedes abrirlo con Bloc de notas, Word, Excel (importar como texto), etc.")


def main():
    engine = connect_db()
    if not engine:
        print("No se pudo conectar. Terminando ejecución.")
        return

    # Lista de consultas con sus títulos descriptivos
    consultas = [
        (
            """
            SELECT 'cr_predio'                                   AS entidad, COUNT(*) AS registros FROM ladm.cr_predio
            UNION ALL
            SELECT 'cr_interesado'                              , COUNT(*) FROM ladm.cr_interesado
            UNION ALL
            SELECT 'cr_derecho'                                 , COUNT(*) FROM ladm.cr_derecho
            UNION ALL
            SELECT 'cr_terreno'                                 , COUNT(*) FROM ladm.cr_terreno
            UNION ALL
            SELECT 'cr_unidadconstruccion'                      , COUNT(*) FROM ladm.cr_unidadconstruccion
            UNION ALL
            SELECT 'cr_caracteristicasunidadconstruccion'       , COUNT(*) FROM ladm.cr_caracteristicasunidadconstruccion;
            """,
            "Tabla 17 - Distribución de registros migrados por entidad"
        ),
        (
            """
            SELECT 
                destinacion_economica                               AS "Destinación Económica",
                COUNT(*)                                            AS Cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS Porcentaje
            FROM ladm.cr_predio
            WHERE destinacion_economica IS NOT NULL
            GROUP BY destinacion_economica
            ORDER BY Cantidad DESC
            LIMIT 10;
            """,
            "Tabla 18 - Distribución por destinación económica (Top 10)"
        ),
        (
            """
            SELECT 
                tipo_predio                                         AS "Tipo Predio",
                COUNT(*)                                            AS Cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS Porcentaje
            FROM ladm.cr_predio
            GROUP BY tipo_predio
            ORDER BY Cantidad DESC;
            """,
            "Tabla 19 - Predios por Tipo Predio"
        ),
        (
            """
            SELECT 
                condicion_predio                                    AS "Condición Predio",
                COUNT(*)                                            AS Cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS Porcentaje
            FROM ladm.cr_predio
            GROUP BY condicion_predio
            ORDER BY Cantidad DESC;
            """,
            "Tabla 110 - Predios por Condición Predio"
        ),
        (
            """
            SELECT 
                tipo_interesado                                     AS "Tipo Interesado",
                COUNT(*)                                            AS Cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS Porcentaje,
                COUNT(DISTINCT predio_t_ili_tid)                    AS "Predios distintos"
            FROM ladm.cr_interesado
            GROUP BY tipo_interesado;
            """,
            "Tabla 11 - Interesados por Tipo Interesado"
        ),
        (
            """
            SELECT 
                c.uso                                               AS Uso,
                COUNT(*)                                            AS Cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS Porcentaje
            FROM ladm.cr_unidadconstruccion u
            JOIN ladm.cr_caracteristicasunidadconstruccion c 
                ON u.caracteristicas_t_ili_tid = c.t_ili_tid
            GROUP BY c.uso
            ORDER BY Cantidad DESC
            LIMIT 5;
            """,
            "Tabla 12 - Usos de construcción (Top 5)"
        ),
        (
            """
            SELECT 
                estado_conservacion                                 AS "Estado Conservación",
                COUNT(*)                                            AS Cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS Porcentaje
            FROM ladm.cr_caracteristicasunidadconstruccion
            WHERE estado_conservacion IS NOT NULL
            GROUP BY estado_conservacion
            ORDER BY Cantidad DESC;
            """,
            "Tabla 13 - Estado de conservación de construcciones"
        ),
        (
            """
            WITH conteo AS (
                SELECT 
                    predio_t_ili_tid,
                    COUNT(*) AS n_interesados
                FROM ladm.cr_derecho
                GROUP BY predio_t_ili_tid
            )
            SELECT 
                CASE 
                    WHEN n_interesados = 1 THEN 'Propietario único'
                    WHEN n_interesados > 1 THEN 'Varios propietarios'
                    ELSE 'Sin derechos'
                END                                                 AS Categoría,
                COUNT(*)                                            AS "Cantidad predios",
                ROUND(100.0 * COUNT(*) FILTER (WHERE n_interesados = 1) 
                      / COUNT(*), 1)                                AS "% Propietario único",
                SUM(n_interesados)                                  AS "Total derechos"
            FROM conteo
            GROUP BY Categoría;
            """,
            "Tabla 14 - Análisis de derechos (único vs copropiedad)"
        )
    ]

    print("Ejecutando consultas...")

    resultados = []
    for sql, titulo in consultas:
        df, tit = execute_query(engine, sql, titulo)
        resultados.append((df, tit))

    write_report(resultados)

    engine.dispose()
    print("\nProceso finalizado.")


if __name__ == "__main__":
    main()