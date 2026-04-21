import os
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

# Configuración de la conexión
db_params = {
    "host": "localhost",
    "port": "5432",
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres123"),
}

database_name = "bd_ladm_05042026"

def execute_sql(cursor, sql, message=""):
    try:
        cursor.execute(sql)
        if message:
            print(message)
    except Error as e:
        print(f"\nERROR al ejecutar: {message}\n{e}\nSQL: {sql}\n")
        raise

# =============================================
# 1. CREAR LA BASE DE DATOS
# =============================================
conn = None
cursor = None
try:
    conn = psycopg2.connect(dbname="postgres", **db_params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    execute_sql(cursor, "CREATE EXTENSION IF NOT EXISTS postgis;", "Extensión PostGIS habilitada en 'postgres'.")

    execute_sql(cursor, f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{database_name}' AND pid <> pg_backend_pid();
    """, "Conexiones activas terminadas.")
    
    execute_sql(cursor, f"DROP DATABASE IF EXISTS \"{database_name}\";", "Base de datos anterior eliminada.")
    
    execute_sql(cursor, f"""
        CREATE DATABASE "{database_name}"
        WITH ENCODING 'UTF8'
        TEMPLATE template0
        LC_COLLATE 'es_CO.UTF-8'
        LC_CTYPE 'es_CO.UTF-8';
    """, f"Base de datos '{database_name}' creada.")

except Error as e:
    print(f"Error en la creación de la base de datos: {e}")
    raise
finally:
    if cursor: cursor.close()
    if conn: conn.close()
    print("Conexión inicial cerrada.\n")

# =============================================
# ESPERAR A QUE LA BASE ESTÉ 100% LISTA
# =============================================
print("Esperando a que la base de datos esté disponible", end="")
for i in range(30):
    try:
        test_conn = psycopg2.connect(dbname=database_name, **db_params)
        test_conn.close()
        print(" ¡Listo!")
        break
    except:
        time.sleep(0.5)
        print(".", end="", flush=True)
else:
    print("\nERROR: No se pudo conectar a la base de datos después de 15 segundos.")
    exit(1)

# =============================================
# 2. CREAR ESQUEMA LADM-COL (tu script original corregido)
# =============================================
conn = None
cursor = None
try:
    conn = psycopg2.connect(dbname=database_name, **db_params)
    cursor = conn.cursor()

    execute_sql(cursor, "CREATE EXTENSION IF NOT EXISTS postgis;", f"Extensión PostGIS habilitada en '{database_name}'.")
    execute_sql(cursor, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";", "Extensión uuid-ossp habilitada.")
    execute_sql(cursor, "CREATE SCHEMA IF NOT EXISTS ladm;", "Esquema 'ladm' creado.")

    execute_sql(cursor, """
        INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
        VALUES (9377, 'EPSG', 9377,
        'PROJCS["MAGNA-SIRGAS / Origen Nacional",GEOGCS["MAGNA-SIRGAS",DATUM["Sistema_de_Referencia_Geocentrico_para_las_Americas",SPHEROID["GRS 1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",4.596200416666666],PARAMETER["central_meridian",-74.07750791666666],PARAMETER["scale_factor",1],PARAMETER["false_easting",1000000],PARAMETER["false_northing",1000000],UNIT["metre",1]]',
        '+proj=tmerc +lat_0=4.596200416666666 +lon_0=-74.07750791666666 +k=1 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
        ON CONFLICT DO NOTHING;
    """, "Sistema de referencia EPSG:9377 agregado.")

    # TU SCRIPT SQL ORIGINAL, PERO 100% CORREGIDO
    sql_script = """
    BEGIN;

    -- TABLAS DE DOMINIOS
    CREATE TABLE ladm.cr_prediotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_condicionprediotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_destinacioneconomicatipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_estadotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_construccionplantatipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_unidadconstrucciontipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_usoconstrucciontipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(255) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.col_interesadotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.col_documentotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_sexotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_derechotipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_visitatipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.col_fuenteadministrativatipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.col_estadodisponibilidadtipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_fuenteadministrativatipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.cr_informalidadtipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.extdirecciontipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.extclaseviaprincipal (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);
    CREATE TABLE ladm.relacion_supertipo (t_id SERIAL PRIMARY KEY, ilicode VARCHAR(100) NOT NULL UNIQUE, dispname VARCHAR(250) NOT NULL, description TEXT, inactive BOOLEAN DEFAULT false);

    -- TABLAS PRINCIPALES
    CREATE TABLE ladm.cr_caracteristicasunidadconstruccion (
        t_id SERIAL PRIMARY KEY, 
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        identificador VARCHAR(50), 
        tipo_unidad VARCHAR(100), 
        total_plantas INTEGER,
        total_habitaciones INTEGER,
        total_banios INTEGER,
        total_locales INTEGER,
        uso VARCHAR(255), 
        area_construida NUMERIC(15,2), 
        estado_conservacion VARCHAR(50),
        Observaciones TEXT
    );

    CREATE TABLE ladm.cr_predio (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30), departamento VARCHAR(50), municipio VARCHAR(50),
        Codigo_ORIP VARCHAR(50), Matricula_inmobiliaria VARCHAR(50),
        tipo_predio VARCHAR(100), condicion_predio VARCHAR(100), destinacion_economica VARCHAR(100),
        area_catastral NUMERIC(15,2), avaluo_catastral NUMERIC(15,2), estado VARCHAR(100),
        actualizado BOOLEAN DEFAULT false
    );

    CREATE TABLE ladm.cr_terreno (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30),predio_t_ili_tid UUID, geometria GEOMETRY(MULTIPOLYGON, 9377),
        area NUMERIC(15,2), Relacion_Superficie VARCHAR(100),
        CONSTRAINT valid_geometria CHECK (ST_IsValid(geometria))
    );

    CREATE TABLE ladm.cr_unidadconstruccion (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30),predio_t_ili_tid UUID, tipo_planta VARCHAR(100), planta_ubicacion NUMERIC,
        altura NUMERIC, geometria GEOMETRY(MULTIPOLYGON, 9377), area NUMERIC(15,2),
        caracteristicas_t_ili_tid UUID, Relacion_Superficie VARCHAR(100),
        CONSTRAINT valid_geometria CHECK (ST_IsValid(geometria)),
        CONSTRAINT fk_unidad_caracteristicas FOREIGN KEY (caracteristicas_t_ili_tid)
            REFERENCES ladm.cr_caracteristicasunidadconstruccion(t_ili_tid) ON DELETE SET NULL
    );

    CREATE TABLE ladm.cr_interesado (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        tipo_interesado VARCHAR(100), tipo_documento VARCHAR(100), numero_documento VARCHAR(20),
        primer_nombre VARCHAR(50), segundo_nombre VARCHAR(50), primer_apellido VARCHAR(50),
        segundo_apellido VARCHAR(50), razon_social VARCHAR(500), sexo VARCHAR(100),
        predio_t_ili_tid UUID,
        direccion TEXT, telefono VARCHAR(20), email VARCHAR(100),
        CONSTRAINT fk_interesado_predio FOREIGN KEY (predio_t_ili_tid) REFERENCES ladm.cr_predio(t_ili_tid) ON DELETE CASCADE
    );

    CREATE TABLE ladm.cr_derecho (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        predio_t_ili_tid UUID, naturaleza_juridica VARCHAR(50), tipo VARCHAR(100),
        interesado_t_ili_tid UUID NOT NULL, fraccion_derecho NUMERIC(5,2),
        informalidad_tipo VARCHAR(100),
        CONSTRAINT fk_derecho_predio FOREIGN KEY (predio_t_ili_tid) REFERENCES ladm.cr_predio(t_ili_tid) ON DELETE CASCADE,
        CONSTRAINT fk_derecho_interesado FOREIGN KEY (interesado_t_ili_tid) REFERENCES ladm.cr_interesado(t_ili_tid) ON DELETE CASCADE
    );

    CREATE TABLE ladm.cr_fuenteadministrativa (
        t_id SERIAL PRIMARY KEY, 
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        fuente_administrativa_tipo VARCHAR(100),
        tipo VARCHAR(100),
        estado_disponibilidad VARCHAR(100),
        fecha_documento_fuente DATE,
        nombre VARCHAR(50),
        descripcion TEXT,  
        derecho_t_ili_tid UUID,
        ente_emisor VARCHAR(100),
        oficina_origen VARCHAR(50),
        ciudad_origen VARCHAR(50),
        CONSTRAINT fk_fuente_derecho FOREIGN KEY (derecho_t_ili_tid) REFERENCES ladm.cr_derecho(t_ili_tid) ON DELETE CASCADE
    );

    CREATE TABLE ladm.cr_contactovisita (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        predio_t_ili_tid UUID, nombre_contacto VARCHAR(100), tipo_documento VARCHAR(100),
        numero_documento VARCHAR(20), telefono VARCHAR(20), tipo_visita VARCHAR(100),
        observaciones TEXT, fecha_visita DATE, nombre_encuestador VARCHAR(100), foto VARCHAR(255),
        CONSTRAINT fk_contactovisita_predio FOREIGN KEY (predio_t_ili_tid) REFERENCES ladm.cr_predio(t_ili_tid) ON DELETE CASCADE
    );

    CREATE TABLE ladm.c_adjunto (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID NOT NULL UNIQUE, archivo TEXT,
        observaciones TEXT, fuente_t_ili_tid UUID,
        CONSTRAINT fk_adjunto_fuente FOREIGN KEY (fuente_t_ili_tid) REFERENCES ladm.cr_fuenteadministrativa(t_ili_tid) ON DELETE CASCADE
    );

CREATE TABLE ladm.cr_adjuntocaracteristica (
t_id SERIAL PRIMARY KEY,
t_ili_tid UUID NOT NULL UNIQUE,
archivo_cara1 TEXT,
archivo_cara2 TEXT,
archivo_cara3 TEXT,
archivo_cara4 TEXT,
archivo_cara5 TEXT,
observaciones TEXT,
caracteristica_t_ili_tid UUID,
CONSTRAINT fk_adjunto_caracteristica FOREIGN KEY (caracteristica_t_ili_tid) REFERENCES ladm.cr_caracteristicasunidadconstruccion(t_ili_tid) ON DELETE CASCADE
);

    -- EXTDIRECCION CORREGIDA: ahora usa VARCHAR + ilicode
    CREATE TABLE ladm.extdireccion (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        tipo_direccion VARCHAR(100) NOT NULL,
        es_direccion_principal BOOLEAN,
        clase_via_principal VARCHAR(100),
        valor_via_principal INTEGER, letra_via_principal VARCHAR(10),
        valor_via_generadora INTEGER, letra_via_generadora VARCHAR(10),
        numero_predio INTEGER, complemento TEXT, nombre_predio TEXT,
        localizacion GEOMETRY(POINT, 9377), predio_t_ili_tid UUID,
        CONSTRAINT valid_localizacion CHECK (ST_IsValid(localizacion)),
        CONSTRAINT fk_extdireccion_predio FOREIGN KEY (predio_t_ili_tid) REFERENCES ladm.cr_predio(t_ili_tid) ON DELETE CASCADE
    );

    CREATE TABLE ladm.cr_derecho_fuente (
        t_id SERIAL PRIMARY KEY, t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        derecho_t_ili_tid UUID, fuente_t_ili_tid UUID,
        CONSTRAINT fk_derecho_fuente_derecho FOREIGN KEY (derecho_t_ili_tid) REFERENCES ladm.cr_derecho(t_ili_tid) ON DELETE CASCADE,
        CONSTRAINT fk_derecho_fuente_fuente FOREIGN KEY (fuente_t_ili_tid) REFERENCES ladm.cr_fuenteadministrativa(t_ili_tid) ON DELETE CASCADE
    );


    -- TODAS LAS FK A DOMINIOS CORREGIDAS (usando ilicode)
    ALTER TABLE ladm.cr_predio ADD CONSTRAINT fk_predio_tipo_predio FOREIGN KEY (tipo_predio) REFERENCES ladm.cr_prediotipo(ilicode);
    ALTER TABLE ladm.cr_predio ADD CONSTRAINT fk_predio_condicion_predio FOREIGN KEY (condicion_predio) REFERENCES ladm.cr_condicionprediotipo(ilicode);
    ALTER TABLE ladm.cr_predio ADD CONSTRAINT fk_predio_destinacion_economica FOREIGN KEY (destinacion_economica) REFERENCES ladm.cr_destinacioneconomicatipo(ilicode);
    ALTER TABLE ladm.cr_predio ADD CONSTRAINT fk_predio_estado FOREIGN KEY (estado) REFERENCES ladm.cr_estadotipo(ilicode);
    ALTER TABLE ladm.cr_unidadconstruccion ADD CONSTRAINT fk_unidadconstruccion_tipo_planta FOREIGN KEY (tipo_planta) REFERENCES ladm.cr_construccionplantatipo(ilicode);
    ALTER TABLE ladm.cr_unidadconstruccion ADD CONSTRAINT fk_unidadconstruccion_relacion_supertipo FOREIGN KEY (Relacion_Superficie) REFERENCES ladm.relacion_supertipo(ilicode);
    ALTER TABLE ladm.cr_terreno ADD CONSTRAINT fk_terreno_relacion_supertipo FOREIGN KEY (Relacion_Superficie) REFERENCES ladm.relacion_supertipo(ilicode);
    -- RELACIONES ENTRE PREDIO Y TERRENO / UNIDADCONSTRUCCION
    ALTER TABLE ladm.cr_terreno ADD CONSTRAINT fk_terreno_predio FOREIGN KEY (predio_t_ili_tid) REFERENCES ladm.cr_predio(t_ili_tid) ON DELETE CASCADE;
    ALTER TABLE ladm.cr_unidadconstruccion ADD CONSTRAINT fk_unidadconstruccion_predio FOREIGN KEY (predio_t_ili_tid) REFERENCES ladm.cr_predio(t_ili_tid) ON DELETE CASCADE;
    ALTER TABLE ladm.cr_caracteristicasunidadconstruccion ADD CONSTRAINT fk_caracteristicas_tipo_unidad FOREIGN KEY (tipo_unidad) REFERENCES ladm.cr_unidadconstrucciontipo(ilicode);
    ALTER TABLE ladm.cr_caracteristicasunidadconstruccion ADD CONSTRAINT fk_caracteristicas_uso FOREIGN KEY (uso) REFERENCES ladm.cr_usoconstrucciontipo(ilicode);
    ALTER TABLE ladm.cr_interesado ADD CONSTRAINT fk_interesado_tipo_interesado FOREIGN KEY (tipo_interesado) REFERENCES ladm.col_interesadotipo(ilicode);
    ALTER TABLE ladm.cr_interesado ADD CONSTRAINT fk_interesado_tipo_documento FOREIGN KEY (tipo_documento) REFERENCES ladm.col_documentotipo(ilicode);
    ALTER TABLE ladm.cr_interesado ADD CONSTRAINT fk_interesado_sexo FOREIGN KEY (sexo) REFERENCES ladm.cr_sexotipo(ilicode);
    ALTER TABLE ladm.cr_derecho ADD CONSTRAINT fk_derecho_tipo FOREIGN KEY (tipo) REFERENCES ladm.cr_derechotipo(ilicode);
    ALTER TABLE ladm.cr_derecho ADD CONSTRAINT fk_derecho_informalidad_tipo FOREIGN KEY (informalidad_tipo) REFERENCES ladm.cr_informalidadtipo(ilicode);
    ALTER TABLE ladm.cr_fuenteadministrativa ADD CONSTRAINT fk_fuente_tipo FOREIGN KEY (tipo) REFERENCES ladm.cr_fuenteadministrativatipo(ilicode);
    ALTER TABLE ladm.cr_fuenteadministrativa ADD CONSTRAINT fk_fuente_estado_disponibilidad FOREIGN KEY (estado_disponibilidad) REFERENCES ladm.col_estadodisponibilidadtipo(ilicode);
    ALTER TABLE ladm.cr_fuenteadministrativa ADD CONSTRAINT fk_fuente_fuente_administrativa_tipo FOREIGN KEY (fuente_administrativa_tipo) REFERENCES ladm.col_fuenteadministrativatipo(ilicode);
    ALTER TABLE ladm.cr_contactovisita ADD CONSTRAINT fk_contacto_tipo_documento FOREIGN KEY (tipo_documento) REFERENCES ladm.col_documentotipo(ilicode);
    ALTER TABLE ladm.cr_contactovisita ADD CONSTRAINT fk_contacto_tipo_visita FOREIGN KEY (tipo_visita) REFERENCES ladm.cr_visitatipo(ilicode);
    ALTER TABLE ladm.extdireccion ADD CONSTRAINT fk_extdireccion_tipo_direccion FOREIGN KEY (tipo_direccion) REFERENCES ladm.extdirecciontipo(ilicode);
    ALTER TABLE ladm.extdireccion ADD CONSTRAINT fk_extdireccion_clase_via_principal FOREIGN KEY (clase_via_principal) REFERENCES ladm.extclaseviaprincipal(ilicode);


    -- Validaciones de fechas
    ALTER TABLE ladm.cr_fuenteadministrativa ADD CONSTRAINT check_fecha_documento CHECK (fecha_documento_fuente <= CURRENT_DATE);
    ALTER TABLE ladm.cr_contactovisita ADD CONSTRAINT check_fecha_visita CHECK (fecha_visita <= CURRENT_DATE);

    -- TODOS TUS INSERTS (exactamente como los tenías)

    INSERT INTO ladm.relacion_supertipo (ilicode, dispname, description, inactive)
    VALUES 
        ('En_Rasante', 'En Rasante', 'Superficie del predio', false),
        ('En_Vuelo', 'En Vuelo', 'Construcción en el predio', false),
        ('En_Subsuelo', 'En Subsuelo', 'Terreno del predio', false),
        ('Otro', 'Otro', 'Otro', false);

    INSERT INTO ladm.cr_prediotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Predio', 'Predio', 'Predio estándar', false),
        ('Publico', 'Público', 'Predio público', false),
        ('Baldio', 'Baldío', 'Predio baldío', false),
        ('Fiscal_Patrimonial', 'Fiscal Patrimonial', 'Predio fiscal patrimonial', false),
        ('Uso_Publico', 'Uso Público', 'Predio de uso público', false),
        ('Presunto_Baldio', 'Presunto Baldío', 'Predio presunto baldío', false),
        ('Privado', 'Privado', 'Predio privado', false),
        ('Colectivo', 'Colectivo', 'Predio colectivo', false);

    INSERT INTO ladm.cr_condicionprediotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('NPH', 'No Propiedad Horizontal', 'No Propiedad Horizontal', false),
        ('PH_Matriz', 'Propiedad Horizontal Matriz', 'Propiedad Horizontal Matriz', false),
        ('PH_Unidad_Predial', 'Propiedad Horizontal Unidad Predial', 'Propiedad Horizontal Unidad Predial', false),
        ('Condominio_Matriz', 'Condominio Matriz', 'Condominio Matriz', false),
        ('Condominio_Unidad_Predial', 'Condominio Unidad Predial', 'Condominio Unidad Predial', false),
        ('Parque_Cementerio_Matriz', 'Parque Cementerio Matriz', 'Parque Cementerio Matriz', false),
        ('Parque_Cementerio_Unidad_Predial', 'Parque Cementerio Unidad Predial', 'Parque Cementerio Unidad Predial', false),
        ('Via', 'Vía', 'Vía', false),
        ('Informal', 'Informal', 'Predio informal', false),
        ('Bien_Uso_Publico', 'Bien Uso Público', 'Bien de uso público', false),
        ('Resguardo_Indigena', 'Resguardo Indígena', 'Resguardo indígena', false),
        ('Territorio_Colectivo', 'Territorio Colectivo', 'Territorio colectivo', false);

    INSERT INTO ladm.cr_destinacioneconomicatipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Acuicola', 'Acuícola', 'Uso acuícola', false),
        ('Agricola', 'Agrícola', 'Uso agrícola', false),
        ('Agroindustrial', 'Agroindustrial', 'Uso agroindustrial', false),
        ('Agroforestal', 'Agroforestal', 'Uso agroforestal', false),
        ('Agropecuario', 'Agropecuario', 'Uso agropecuario', false),
        ('Comercial', 'Comercial', 'Uso comercial', false),
        ('Conservacion_Proteccion_Ambiental', 'Conservación_Protección_Ambiental', 'Conservación y protección ambiental', false),
        ('Cultural', 'Cultural', 'Uso cultural', false),
        ('Educativo', 'Educativo', 'Uso educativo', false),
        ('Forestal_Productor', 'Forestal_Productor', 'Uso forestal productor', false),
        ('Habitacional', 'Habitacional', 'Uso habitacional', false),
        ('Industrial', 'Industrial', 'Uso industrial', false),
        ('Infraestructura_Asociada_Produccion_Agropecuaria', 'Infraestructura_Asociada_Producción_Agropecuaria', 'Infraestructura para producción agropecuaria', false),
        ('Infraestructura_Hidraulica', 'Infraestructura_Hidráulica', 'Infraestructura hidráulica', false),
        ('Infraestructura_Saneamiento_Basico', 'Infraestructura_Saneamiento_Básico', 'Infraestructura de saneamiento básico', false),
        ('Infraestructura_Seguridad', 'Infraestructura_Seguridad', 'Infraestructura de seguridad', false),
        ('Infraestructura_Transporte', 'Infraestructura_Transporte', 'Infraestructura de transporte', false),
        ('Infraestructura_Energia_Renovable_Electrica', 'Infraestructura_Energía_Renovable_Eléctrica', 'Infraestructura de energía renovable eléctrica', false),
        ('Institucional', 'Institucional', 'Uso institucional', false),
        ('Mineria_Hidrocarburos', 'Minería_Hidrocarburos', 'Minería e hidrocarburos', false),
        ('Lote_Urbanizable_No_Urbanizado', 'Lote_Urbanizable_No_Urbanizado', 'Lote urbanizable no urbanizado', false),
        ('Lote_Urbanizado_No_Construido', 'Lote_Urbanizado_No_Construido', 'Lote urbanizado no construido', false),
        ('Lote_No_Urbanizable', 'Lote_No_Urbanizable', 'Lote no urbanizable', false),
        ('Lote_Rural', 'Lote_Rural', 'Lote rural', false),
        ('Turistico', 'Turístico', 'Uso turístico', false),
        ('Recreacional', 'Recreacional', 'Uso recreativo', false),
        ('Pecuario', 'Pecuario', 'Uso pecuario', false),
        ('Religioso', 'Religioso', 'Uso religioso', false),
        ('Salubridad', 'Salubridad', 'Uso de salubridad', false),
        ('Servicios_Funerarios', 'Servicios_Funerarios', 'Servicios funerarios', false),
        ('Servicios_Sociales', 'Servicios_Sociales', 'Servicios sociales', false),
        ('Uso_Publico', 'Uso_Público', 'Uso público', false);

    INSERT INTO ladm.cr_estadotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Activo', 'Activo', 'Estado activo', false),
        ('Cancelado', 'Cancelado', 'Estado cancelado', false),
        ('Suspendido', 'Suspendido', 'Estado suspendido', false);

    INSERT INTO ladm.cr_unidadconstrucciontipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Residencial', 'Residencial', 'Construcción residencial', false),
        ('Comercial', 'Comercial', 'Construcción comercial', false),
        ('Industrial', 'Industrial', 'Construcción industrial', false),
        ('Institucional', 'Institucional', 'Construcción institucional', false),
        ('Anexo', 'Anexo', 'Construcción anexa', false);

    INSERT INTO ladm.cr_usoconstrucciontipo (ilicode, dispname, description, inactive) VALUES
('Residencial.Apartamentos_4_y_mas_pisos_en_PH', 'Apartamentos 4 y más pisos en PH', 'Apartamentos de 4 o más pisos en propiedad horizontal', false),
('Residencial.Apartamentos_4_y_mas_pisos', 'Apartamentos 4 y más pisos', 'Apartamentos de 4 o más pisos', false),
('Residencial.Barracas', 'Barracas', 'Barracas', false),
('Residencial.Casa_Elbas', 'Casa Elbas', 'Casa tipo Elbas', false),
('Residencial.Depositos_Lockers', 'Depósitos Lockers', 'Depósitos y lockers', false),
('Residencial.Garajes_Cubiertos', 'Garajes Cubiertos', 'Garajes cubiertos', false),
('Residencial.Garajes_En_PH', 'Garajes en PH', 'Garajes en propiedad horizontal', false),
('Residencial.Salon_Comunal', 'Salón Comunal', 'Salón comunal', false),
('Residencial.Secadero_Ropa', 'Secadero Ropa', 'Secadero de ropa', false),
('Residencial.Vivienda_Colonial', 'Vivienda Colonial', 'Vivienda de estilo colonial', false),
('Residencial.Vivienda_Colonial_en_PH', 'Vivienda Colonial en PH', 'Vivienda colonial en propiedad horizontal', false),
('Residencial.Vivienda_Hasta_3_Pisos', 'Vivienda Hasta 3 Pisos', 'Vivienda de hasta 3 pisos', false),
('Residencial.Vivienda_Hasta_3_Pisos_En_PH', 'Vivienda Hasta 3 Pisos en PH', 'Vivienda de hasta 3 pisos en propiedad horizontal', false),
('Residencial.Vivienda_Recreacional', 'Vivienda Recreacional', 'Vivienda para uso recreacional', false),
('Residencial.Vivienda_Recreacional_En_PH', 'Vivienda Recreacional en PH', 'Vivienda recreacional en propiedad horizontal', false),
('Comercial.Bodegas_Comerciales_Grandes_Almacenes', 'Bodegas Comerciales Grandes Almacenes', 'Bodegas comerciales y grandes almacenes', false),
('Comercial.Bodegas_Comerciales', 'Bodegas Comerciales', 'Bodegas comerciales', false),
('Comercial.Bodegas_Comerciales_en_PH', 'Bodegas Comerciales en PH', 'Bodegas comerciales en propiedad horizontal', false),
('Comercial.Centros_Comerciales', 'Centros Comerciales', 'Centros comerciales', false),
('Comercial.Centros_Comerciales_en_PH', 'Centros Comerciales en PH', 'Centros comerciales en propiedad horizontal', false),
('Comercial.Clubes_Casinos', 'Clubes y Casinos', 'Clubes y casinos', false),
('Comercial.Comercio', 'Comercio', 'Uso comercial genérico', false),
('Comercial.Comercio_Colonial', 'Comercio Colonial', 'Comercio en edificaciones coloniales', false),
('Comercial.Comercio_Deposito_Almacenamiento', 'Comercio Depósito Almacenamiento', 'Comercio con depósito y almacenamiento', false),
('Comercial.Comercio_en_PH', 'Comercio en PH', 'Comercio en propiedad horizontal', false),
('Comercial.Hotel_Colonial', 'Hotel Colonial', 'Hotel de estilo colonial', false),
('Comercial.Hoteles', 'Hoteles', 'Hoteles genéricos', false),
('Comercial.Hoteles_en_PH', 'Hoteles en PH', 'Hoteles en propiedad horizontal', false),
('Comercial.Oficinas_Consultorios', 'Oficinas y Consultorios', 'Oficinas y consultorios', false),
('Comercial.Oficinas_Consultorios_Coloniales', 'Oficinas y Consultorios Coloniales', 'Oficinas y consultorios en edificaciones coloniales', false),
('Comercial.Oficinas_Consultorios_en_PH', 'Oficinas y Consultorios en PH', 'Oficinas y consultorios en propiedad horizontal', false),
('Comercial.Parque_Diversiones', 'Parque de Diversiones', 'Parque de diversiones', false),
('Comercial.Parqueaderos', 'Parqueaderos', 'Parqueaderos genéricos', false),
('Comercial.Parqueaderos_en_PH', 'Parqueaderos en PH', 'Parqueaderos en propiedad horizontal', false),
('Comercial.Pensiones_y_Residencias', 'Pensiones y Residencias', 'Pensiones y residencias', false),
('Comercial.Plaza_Mercado', 'Plaza de Mercado', 'Plaza de mercado', false),
('Comercial.Restaurante_Colonial', 'Restaurante Colonial', 'Restaurante de estilo colonial', false),
('Comercial.Restaurantes', 'Restaurantes', 'Restaurantes genéricos', false),
('Comercial.Restaurantes_en_PH', 'Restaurantes en PH', 'Restaurantes en propiedad horizontal', false),
('Comercial.Teatro_Cinemas', 'Teatro y Cinemas', 'Teatros y cinemas genéricos', false),
('Comercial.Teatro_Cinemas_en_PH', 'Teatro y Cinemas en PH', 'Teatros y cinemas en propiedad horizontal', false),
('Industrial.Bodega_Casa_Bomba', 'Bodega Casa Bomba', 'Bodega con casa bomba', false),
('Industrial.Bodegas_Casa_Bomba_en_PH', 'Bodegas Casa Bomba en PH', 'Bodegas con casa bomba en propiedad horizontal', false),
('Industrial.Industrias', 'Industrias', 'Industrias genéricas', false),
('Industrial.Industrias_en_PH', 'Industrias en PH', 'Industrias en propiedad horizontal', false),
('Industrial.Talleres', 'Talleres', 'Talleres', false),
('Institucional.Aulas_de_Clases', 'Aulas de Clases', 'Aulas de clases', false),
('Institucional.Biblioteca', 'Biblioteca', 'Biblioteca', false),
('Institucional.Carceles', 'Cárceles', 'Cárceles', false),
('Institucional.Casas_de_Culto', 'Casas de Culto', 'Casas de culto', false),
('Institucional.Clinicas_Hospitales_Centros_Medicos', 'Clínicas, Hospitales y Centros Médicos', 'Clínicas, hospitales y centros médicos', false),
('Institucional.Colegio_y_Universidades', 'Colegio y Universidades', 'Colegios y universidades', false),
('Institucional.Coliseos', 'Coliseos', 'Coliseos', false),
('Institucional.Entidad_Educativa_Colonial_Colegio_Colonial', 'Entidad Educativa Colonial / Colegio Colonial', 'Entidad educativa en estilo colonial', false),
('Institucional.Estadios', 'Estadios', 'Estadios', false),
('Institucional.Fuertes_y_Castillos', 'Fuertes y Castillos', 'Fuertes y castillos', false),
('Institucional.Iglesia', 'Iglesia', 'Iglesia', false),
('Institucional.Iglesia_en_PH', 'Iglesia en PH', 'Iglesia en propiedad horizontal', false),
('Institucional.Instalaciones_Militares', 'Instalaciones Militares', 'Instalaciones militares', false),
('Institucional.Jardin_Infantil_en_Casa', 'Jardín Infantil en Casa', 'Jardín infantil en vivienda', false),
('Institucional.Parque_Cementerio', 'Parque Cementerio', 'Parque cementerio', false),
('Institucional.Planetario', 'Planetario', 'Planetario', false),
('Institucional.Plaza_de_Toros', 'Plaza de Toros', 'Plaza de toros', false),
('Institucional.Puestos_de_Salud', 'Puestos de Salud', 'Puestos de salud', false),
('Institucional.Museos', 'Museos', 'Museos', false),
('Institucional.Seminarios_Conventos', 'Seminarios y Conventos', 'Seminarios y conventos', false),
('Institucional.Teatro', 'Teatro', 'Teatro', false),
('Institucional.Unidad_Deportiva', 'Unidad Deportiva', 'Unidad deportiva', false),
('Institucional.Velodromo_Patinodromo', 'Velódromo / Patinódromo', 'Velódromo o patinódromo', false),
('Anexo.Albercas_Banaderas', 'Albercas / Bañaderas', 'Albercas y bañaderas', false),
('Anexo.Beneficiaderos', 'Beneficiaderos', 'Beneficiaderos', false),
('Anexo.Camaroneras', 'Camaroneras', 'Camaroneras', false),
('Anexo.Canchas', 'Canchas', 'Canchas deportivas', false),
('Anexo.Canchas_de_Tenis', 'Canchas de Tenis', 'Canchas de tenis', false),
('Anexo.Carretera', 'Carretera', 'Carretera', false),
('Anexo.Cerramiento', 'Cerramiento', 'Cerramiento', false),
('Anexo.Cimientos_Estructura_Muros_y_Placa_Base', 'Cimientos, Estructura, Muros y Placa Base', 'Construcción en cimientos, estructura, muros y placa base', false),
('Anexo.Cocheras_Marraneras_Porquerizas', 'Cocheras, Marraneras, Porquerizas', 'Cocheras, marraneras y porquerizas', false),
('Anexo.Construccion_en_Membrana_Arquitectonica', 'Construcción en Membrana Arquitectónica', 'Construcción en membrana arquitectónica', false),
('Anexo.Contenedor', 'Contenedor', 'Contenedor', false),
('Anexo.Corrales', 'Corrales', 'Corrales', false),
('Anexo.Establos_Pesebreras_Caballerizas', 'Establos, Pesebreras, Caballerizas', 'Establos, pesebreras y caballerizas', false),
('Anexo.Estacion_Bombeo', 'Estación de Bombeo', 'Estación de bombeo', false),
('Anexo.Estacion_Sistema_Transporte', 'Estación Sistema de Transporte', 'Estación de sistema de transporte', false),
('Anexo.Galpones_Gallineros', 'Galpones y Gallineros', 'Galpones y gallineros', false),
('Anexo.Glamping', 'Glamping', 'Glamping', false),
('Anexo.Hangar', 'Hangar', 'Hangar', false),
('Anexo.Kioscos', 'Kioscos', 'Kioscos', false),
('Anexo.Lagunas_de_Oxidacion', 'Lagunas de Oxidación', 'Lagunas de oxidación', false),
('Anexo.Marquesinas_Patios_Cubiertos', 'Marquesinas y Patios Cubiertos', 'Marquesinas y patios cubiertos', false),
('Anexo.Muelles', 'Muelles', 'Muelles', false),
('Anexo.Murallas', 'Murallas', 'Murallas', false),
('Anexo.Pergolas', 'Pérgolas', 'Pérgolas', false),
('Anexo.Piscinas', 'Piscinas', 'Piscinas', false),
('Anexo.Pista_Aeropuerto', 'Pista de Aeropuerto', 'Pista de aeropuerto', false),
('Anexo.Pozos', 'Pozos', 'Pozos', false),
('Anexo.Toboganes', 'Toboganes', 'Toboganes', false),
('Anexo.Ramadas_Cobertizos_Caneyes', 'Ramadas, Cobertizos, Caneyes', 'Ramadas, cobertizos y caneyes', false),
('Anexo.Secaderos', 'Secaderos', 'Secaderos', false),
('Anexo.Silos', 'Silos', 'Silos', false),
('Anexo.Tanques', 'Tanques', 'Tanques', false);



    INSERT INTO ladm.col_interesadotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Persona_Natural', 'Persona Natural', 'Persona natural', false),
        ('Persona_Juridica', 'Persona Jurídica', 'Persona jurídica', false);

    INSERT INTO ladm.col_documentotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Cedula_Ciudadania', 'Cédula de Ciudadanía', 'Cédula de ciudadanía colombiana', false),
        ('NIT', 'NIT', 'Número de Identificación Tributaria', false),
        ('Cedula_Extranjeria', 'Cédula de Extranjería', 'Cédula de extranjería', false),
        ('Tarjeta_Identidad', 'Tarjeta de Identidad', 'Tarjeta de identidad', false),
        ('Pasaporte', 'Pasaporte', 'Pasaporte', false),
        ('Registro_Civil', 'Registro Civil', 'Registro civil de nacimiento', false),
        ('No_Especificado', 'No Especificado', 'Documento no especificado', false);

    INSERT INTO ladm.cr_sexotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Masculino', 'Masculino', 'Sexo masculino', false),
        ('Femenino', 'Femenino', 'Sexo femenino', false),
        ('No_Especificado', 'No Especificado', 'Sexo no especificado', false);

    INSERT INTO ladm.cr_derechotipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Dominio', 'Dominio', 'Derecho de dominio', false),
        ('Posesion', 'Posesión', 'Derecho de posesión', false),
        ('Tenencia', 'Tenencia', 'Derecho de tenencia', false);



    INSERT INTO ladm.cr_visitatipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Efectiva', 'Efectiva', 'Visita efectiva', false),
        ('No_Efectiva', 'No Efectiva', 'Visita no efectiva', false);

    INSERT INTO ladm.col_fuenteadministrativatipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Documento_Publico', 'Documento Público', 'Fuente de documento público', false),
        ('Documento_Privado', 'Documento Privado', 'Fuente de documento privado', false);

    INSERT INTO ladm.col_estadodisponibilidadtipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Convertido', 'Convertido', 'Fuente convertida', false),
        ('Desconocido', 'Desconocido', 'Fuente desconocida', false),
        ('Disponible', 'Disponible', 'Fuente disponible', false);


    INSERT INTO ladm.cr_fuenteadministrativatipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Escritura_Publica', 'Escritura Pública', 'Fuente de escritura pública', false),
        ('Sentencia_Judicial', 'Sentencia Judicial', 'Fuente de sentencia judicial', false),
        ('Acto_Administrativo', 'Acto Administrativo', 'Fuente de acto administrativo', false),
        ('Sin_Documento', 'Sin Documento', 'Fuente sin documento', false);

    INSERT INTO ladm.cr_informalidadtipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Posesion', 'Posesión', 'Posesión informal', false),
        ('Ocupacion', 'Ocupación', 'Ocupación informal', false);


    INSERT INTO ladm.extdirecciontipo (ilicode, dispname, description, inactive)
    VALUES 
        ('Residencial', 'Residencial', 'Dirección residencial', false),
        ('Comercial', 'Comercial', 'Dirección comercial', false),
        ('Institucional', 'Institucional', 'Dirección institucional', false),
        ('Industrial', 'Industrial', 'Dirección industrial', false),
        ('Rural', 'Rural', 'Dirección rural', false),
        ('Urbana', 'Urbana', 'Dirección urbana', false);

    INSERT INTO ladm.extclaseviaprincipal (ilicode, dispname, description, inactive)
    VALUES 
        ('Calle', 'Calle', 'Calle', false),
        ('Carrera', 'Carrera', 'Carrera', false),
        ('Avenida', 'Avenida', 'Avenida', false),
        ('Diagonal', 'Diagonal', 'Diagonal', false),
        ('Transversal', 'Transversal', 'Transversal', false),
        ('Circular', 'Circular', 'Circular', false),
        ('Autopista', 'Autopista', 'Autopista', false),
        ('Kilometro', 'Kilómetro', 'Kilómetro', false),
        ('Via', 'Vía', 'Vía', false);

-- DOMINIO OBLIGATORIO PARA TIPO_PLANTA (TODOS LOS VALORES REALES DE TUS SHPs)
INSERT INTO ladm.cr_construccionplantatipo (ilicode, dispname, description, inactive) VALUES
        ('Piso', 'Piso', 'Planta tipo piso', false),
        ('Mezanine', 'Mezanine', 'Planta tipo mezanine', false),
        ('Sotano', 'Sotano', 'Planta tipo sótano', false),
        ('Semisotano', 'Semisotano', 'Planta tipo semisótano', false),
        ('Subterraneo', 'Subterraneo', 'Planta tipo subterráneo', false),
        ('Cubierta', 'Cubierta', 'Planta tipo cubierta', false)
ON CONFLICT (ilicode) DO NOTHING;


    COMMIT;
    """

    # Ejecutar sentencia por sentencia
    for statement in sql_script.split(';'):
        stmt = statement.strip()
        if stmt:
            execute_sql(cursor, stmt + ';')

    print("\nBASE DE DATOS LADM-COL 4.1 CREADA CON ÉXITO - 100% FUNCIONAL")

except Error as e:
    print(f"\nError en la creación del esquema: {e}")
    if conn:
        try:
            conn.rollback()
            print("ROLLBACK ejecutado correctamente")
        except:
            pass
    raise
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Conexión cerrada.")