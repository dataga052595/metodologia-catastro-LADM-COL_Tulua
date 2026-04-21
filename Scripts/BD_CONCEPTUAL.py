import os
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configuración de la conexión
db_params = {
    "host": "localhost",
    "port": "5432",
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres123"),
}

# Nombre de la base de datos
database_name = "bd_conceptual_01092025"

# Función para ejecutar comandos SQL
def execute_sql(cursor, sql, message):
    try:
        cursor.execute(sql)
        print(message)
    except Error as e:
        print(f"Error al ejecutar {message}: {e}")
        raise

# Crear y configurar la base de datos
try:
    conn = psycopg2.connect(dbname="postgres", **db_params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Habilitar PostGIS en 'postgres'
    execute_sql(cursor, "CREATE EXTENSION IF NOT EXISTS postgis;", "Extensión PostGIS habilitada en 'postgres'.")

    # Terminar conexiones activas y crear la base de datos
    execute_sql(cursor, f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{database_name}' AND pid <> pg_backend_pid();
    """, "Conexiones activas terminadas.")
    
    execute_sql(cursor, f"DROP DATABASE IF EXISTS {database_name};", "Base de datos anterior eliminada.")
    execute_sql(cursor, f"""
        CREATE DATABASE {database_name}
        WITH ENCODING 'UTF8'
        TEMPLATE template0
        LC_COLLATE 'es_CO.UTF-8'
        LC_CTYPE 'es_CO.UTF-8';
    """, f"Base de datos '{database_name}' creada.")

except Error as e:
    print(f"Error en la creación de la base de datos: {e}")
    raise
finally:
    if conn:
        cursor.close()
        conn.close()

# Conectar a la nueva base y crear el esquema
try:
    conn = psycopg2.connect(dbname=database_name, **db_params)
    conn.autocommit = True
    cursor = conn.cursor()

    # Habilitar PostGIS y uuid-ossp
    execute_sql(cursor, "CREATE EXTENSION IF NOT EXISTS postgis;", f"Extensión PostGIS habilitada en '{database_name}'.")
    execute_sql(cursor, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";", "Extensión uuid-ossp habilitada.")
    execute_sql(cursor, "CREATE SCHEMA ladm;", "Esquema 'ladm' creado.")

    # Agregar sistema de referencia EPSG:9377
    execute_sql(cursor, """
        INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
        VALUES (
            9377,
            'EPSG',
            9377,
            'PROJCS["MAGNA-SIRGAS / Origen Nacional",GEOGCS["MAGNA-SIRGAS",DATUM["Sistema de Referencia Geocéntrico para las Américas",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6686"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4686"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",4.596200416666666],PARAMETER["central_meridian",-74.07750791666666],PARAMETER["scale_factor",1],PARAMETER["false_easting",1000000],PARAMETER["false_northing",1000000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","9377"]]',
            '+proj=tmerc +lat_0=4.596200416666666 +lon_0=-74.07750791666666 +k=1 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'
        );
    """, "Sistema de referencia EPSG:9377 agregado.")

    # Script SQL para la base simplificada (solo tablas principales)
    sql_script = """
    BEGIN;

    -- Tablas principales
    CREATE TABLE ladm.cr_predio (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30) UNIQUE,
        departamento VARCHAR(50),
        municipio VARCHAR(50),
        Codigo_ORIP VARCHAR(50),
        Matricula_inmobiliaria VARCHAR(50),
        Estado_FMI VARCHAR(1024),
        Codigo_Homologado VARCHAR(50),
        tipo_predio VARCHAR(1024),
        condicion_predio VARCHAR(1024),
        destinacion_economica VARCHAR(1024),
        area_catastral NUMERIC(15,2),
        area_registral NUMERIC(15,2),
        avaluo_catastral NUMERIC(15,2),
        estado VARCHAR(1024)
    );

    CREATE TABLE ladm.cr_terreno (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30),
        geometria GEOMETRY(MULTIPOLYGON, 9377),
        area NUMERIC(15,2),
        CONSTRAINT valid_geometria CHECK (ST_IsValid(geometria))
    );

    CREATE TABLE ladm.cr_unidadconstruccion (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30),
        tipo_planta VARCHAR(1024),
        planta_ubicacion NUMERIC,
        altura NUMERIC,
        geometria GEOMETRY(MULTIPOLYGON, 9377),
        area NUMERIC(15,2),
        CONSTRAINT valid_geometria CHECK (ST_IsValid(geometria))
    );

    CREATE TABLE ladm.cr_caracteristicasunidadconstruccion (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        t_ili_tid_unidad UUID NOT NULL,
        identificador VARCHAR(50),
        tipo_unidad VARCHAR(1024),
        total_plantas INTEGER,
        uso VARCHAR(1024),
        area_construida NUMERIC(15,2),
        estado_conservacion VARCHAR(50)
    );

    CREATE TABLE ladm.cr_interesado (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30),
        tipo_interesado VARCHAR(1024),
        tipo_documento VARCHAR(1024),
        numero_documento VARCHAR(20),
        primer_nombre VARCHAR(50),
        primer_apellido VARCHAR(50),
        segundo_apellido VARCHAR(50),
        razon_social VARCHAR(100),
        sexo VARCHAR(1024)
    );

    CREATE TABLE ladm.cr_derecho (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        numero_predial VARCHAR(30),
        naturaleza_juridica VARCHAR(50),
        tipo VARCHAR(1024),
        interesado_t_ili_tid UUID,
        fraccion_derecho NUMERIC(5,2),
        informalidad_tipo VARCHAR(1024),
        restriccion_tipo VARCHAR(1024)
    );

    CREATE TABLE ladm.cr_fuenteadministrativa (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4() UNIQUE,
        tipo VARCHAR(1024),
        estado_disponibilidad VARCHAR(1024),
        formato_col VARCHAR(1024),
        nombre VARCHAR(50),
        fecha_documento_fuente DATE,
        descripcion TEXT,
        fuente_administrativa_tipo VARCHAR(1024),
        ente_emisor VARCHAR(100),
        ciudad_origen VARCHAR(50)
    );

    CREATE TABLE ladm.cr_derecho_fuente (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4(),
        derecho_t_id UUID,
        fuente_administrativa_t_id UUID
    );

    CREATE TABLE ladm.cr_contactovisita (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4(),
        numero_predial VARCHAR(30),
        nombre_contacto VARCHAR(100),
        tipo_documento VARCHAR(1024),
        numero_documento VARCHAR(20),
        telefono VARCHAR(20),
        tipo_visita VARCHAR(1024),
        observaciones TEXT,
        fecha_visita DATE,
        foto VARCHAR(255)
    );

    CREATE TABLE ladm.cca_adjunto (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID NOT NULL UNIQUE,
        t_seq INTEGER,
        archivo TEXT,
        observaciones TEXT,
        cca_fuenteadministrativa_ext_archivo UUID
    );

    CREATE TABLE ladm.extdireccion (
        t_id SERIAL PRIMARY KEY,
        t_ili_tid UUID DEFAULT uuid_generate_v4(),
        tipo_direccion INTEGER NOT NULL,
        es_direccion_principal BOOLEAN,
        clase_via_principal INTEGER,
        valor_via_principal INTEGER,
        letra_via_principal VARCHAR(10),
        valor_via_generadora INTEGER,
        letra_via_generadora VARCHAR(10),
        numero_predio INTEGER,
        complemento TEXT,
        nombre_predio TEXT,
        localizacion GEOMETRY(POINT, 9377),
        numero_predial VARCHAR(30),
        CONSTRAINT valid_localizacion CHECK (ST_IsValid(localizacion))
    );

    -- Índices
    CREATE INDEX idx_cr_terreno_geometria ON ladm.cr_terreno USING GIST (geometria);
    CREATE INDEX idx_cr_unidadconstruccion_geometria ON ladm.cr_unidadconstruccion USING GIST (geometria);
    CREATE INDEX idx_cr_predio_numero_predial ON ladm.cr_predio (numero_predial);
    CREATE INDEX idx_cr_terreno_numero_predial ON ladm.cr_terreno (numero_predial);
    CREATE INDEX idx_cr_unidadconstruccion_numero_predial ON ladm.cr_unidadconstruccion (numero_predial);
    CREATE INDEX idx_cr_interesado_numero_predial ON ladm.cr_interesado (numero_predial);
    CREATE INDEX idx_cr_derecho_numero_predial ON ladm.cr_derecho (numero_predial);
    CREATE INDEX idx_cr_contactovisita_numero_predial ON ladm.cr_contactovisita (numero_predial);
    CREATE INDEX idx_cr_caracteristicas_t_ili_tid_unidad ON ladm.cr_caracteristicasunidadconstruccion (t_ili_tid_unidad);
    CREATE INDEX idx_cca_adjunto_fuente ON ladm.cca_adjunto (cca_fuenteadministrativa_ext_archivo);
    CREATE INDEX idx_extdireccion_localizacion ON ladm.extdireccion USING GIST (localizacion);
    CREATE INDEX idx_extdireccion_numero_predial ON ladm.extdireccion (numero_predial);

    -- Agregar llaves foráneas
    ALTER TABLE ladm.cr_terreno ADD CONSTRAINT fk_terreno_predio FOREIGN KEY (numero_predial) REFERENCES ladm.cr_predio (numero_predial);
    ALTER TABLE ladm.cr_unidadconstruccion ADD CONSTRAINT fk_unidadconstruccion_predio FOREIGN KEY (numero_predial) REFERENCES ladm.cr_predio (numero_predial);
    ALTER TABLE ladm.cr_interesado ADD CONSTRAINT fk_interesado_predio FOREIGN KEY (numero_predial) REFERENCES ladm.cr_predio (numero_predial);
    ALTER TABLE ladm.cr_derecho ADD CONSTRAINT fk_derecho_predio FOREIGN KEY (numero_predial) REFERENCES ladm.cr_predio (numero_predial);
    ALTER TABLE ladm.cr_derecho ADD CONSTRAINT fk_derecho_interesado FOREIGN KEY (interesado_t_ili_tid) REFERENCES ladm.cr_interesado (t_ili_tid);
    ALTER TABLE ladm.cr_derecho_fuente ADD CONSTRAINT fk_derecho_fuente_derecho FOREIGN KEY (derecho_t_id) REFERENCES ladm.cr_derecho (t_ili_tid);
    ALTER TABLE ladm.cr_derecho_fuente ADD CONSTRAINT fk_derecho_fuente_fuente FOREIGN KEY (fuente_administrativa_t_id) REFERENCES ladm.cr_fuenteadministrativa (t_ili_tid);
    ALTER TABLE ladm.cr_contactovisita ADD CONSTRAINT fk_contactovisita_predio FOREIGN KEY (numero_predial) REFERENCES ladm.cr_predio (numero_predial);
    ALTER TABLE ladm.cr_caracteristicasunidadconstruccion ADD CONSTRAINT fk_caracteristicas_unidad FOREIGN KEY (t_ili_tid_unidad) REFERENCES ladm.cr_unidadconstruccion (t_ili_tid);
    ALTER TABLE ladm.cca_adjunto ADD CONSTRAINT fk_adjunto_fuente FOREIGN KEY (cca_fuenteadministrativa_ext_archivo) REFERENCES ladm.cr_fuenteadministrativa (t_ili_tid);
    ALTER TABLE ladm.extdireccion ADD CONSTRAINT fk_extdireccion_predio FOREIGN KEY (numero_predial) REFERENCES ladm.cr_predio (numero_predial);

    -- Inserciones de datos de prueba
    INSERT INTO ladm.cr_predio (
        t_ili_tid, numero_predial, departamento, municipio, Codigo_ORIP, Matricula_inmobiliaria, Estado_FMI, Codigo_Homologado, tipo_predio, 
        condicion_predio, destinacion_economica, area_catastral, area_registral, avaluo_catastral, estado
    ) VALUES 
        (uuid_generate_v4(), '123456789012345678901234567890', 'Cundinamarca', 'Bogotá', '001', '01-123456', 'Activo', 'HOM_001',
         'Privado', 'NPH', 'Habitacional', 1000.50, 1000.50, 50000000.00, 'Activo'),
        (uuid_generate_v4(), '987654321098765432109876543210', 'Antioquia', 'Medellín', '002', '02-654321', 'Activo', 'HOM_002',
         'Publico', 'PH_Matriz', 'Comercial', 800.25, 800.25, 40000000.00, 'Activo');

    INSERT INTO ladm.cr_terreno (
        t_ili_tid, numero_predial, geometria, area
    ) VALUES 
        (uuid_generate_v4(), '123456789012345678901234567890',
         ST_GeomFromText('MULTIPOLYGON(((1000000 1000000, 1000100 1000000, 1000100 1000100, 1000000 1000100, 1000000 1000000)))', 9377), 10000.00),
        (uuid_generate_v4(), '987654321098765432109876543210',
         ST_GeomFromText('MULTIPOLYGON(((1000200 1000200, 1000300 1000200, 1000300 1000300, 1000200 1000300, 1000200 1000200)))', 9377), 10000.00);

    INSERT INTO ladm.cr_unidadconstruccion (
        t_ili_tid, numero_predial, tipo_planta, planta_ubicacion, altura, geometria, area
    ) VALUES 
        (uuid_generate_v4(), '123456789012345678901234567890', 'Piso', 1, 3.5,
         ST_GeomFromText('MULTIPOLYGON(((1000050 1000050, 1000060 1000050, 1000060 1000060, 1000050 1000060, 1000050 1000050)))', 9377), 100.00),
        (uuid_generate_v4(), '987654321098765432109876543210', 'Sotano', -1, 2.8,
         ST_GeomFromText('MULTIPOLYGON(((1000250 1000250, 1000260 1000250, 1000260 1000260, 1000250 1000260, 1000250 1000250)))', 9377), 100.00);

    INSERT INTO ladm.cr_caracteristicasunidadconstruccion (
        t_ili_tid, t_ili_tid_unidad, identificador, tipo_unidad, total_plantas, uso, area_construida, estado_conservacion
    ) VALUES 
        (uuid_generate_v4(),
         (SELECT t_ili_tid FROM ladm.cr_unidadconstruccion WHERE numero_predial = '123456789012345678901234567890'),
         'A', 'Residencial', 1, 'Vivienda_Hasta_3_Pisos', 100.00, 'Bueno'),
        (uuid_generate_v4(),
         (SELECT t_ili_tid FROM ladm.cr_unidadconstruccion WHERE numero_predial = '987654321098765432109876543210'),
         'A', 'Comercial', 2, 'Oficinas_Consultorios_EN_PH', 100.00, 'Regular');

    INSERT INTO ladm.cr_interesado (
        t_ili_tid, numero_predial, tipo_interesado, tipo_documento, numero_documento, 
        primer_nombre, primer_apellido, segundo_apellido, razon_social, sexo
    ) VALUES 
        (uuid_generate_v4(), '123456789012345678901234567890',
         'Persona_Natural', 'Cedula_Ciudadania', '1234567890', 
         'Juan', 'Pérez', 'Gamboa', NULL, 'Masculino'),
        (uuid_generate_v4(), '987654321098765432109876543210',
         'Persona_Juridica', 'NIT', '9001234567', 
         NULL, NULL, NULL, 'Empresa Ejemplo S.A.', 'No_Especificado');

    INSERT INTO ladm.cr_derecho (
        t_ili_tid, numero_predial, naturaleza_juridica, tipo, interesado_t_ili_tid, fraccion_derecho, informalidad_tipo, restriccion_tipo
    ) VALUES 
        (uuid_generate_v4(), '123456789012345678901234567890', 'Compraventa', 
         'Dominio', (SELECT t_ili_tid FROM ladm.cr_interesado WHERE numero_documento = '1234567890'), 
         100.00, NULL, NULL),
        (uuid_generate_v4(), '987654321098765432109876543210', 'Donacion', 
         'Tenencia', (SELECT t_ili_tid FROM ladm.cr_interesado WHERE numero_documento = '9001234567'), 
         50.00, 'Posesion', NULL);

    INSERT INTO ladm.cr_fuenteadministrativa (
        t_ili_tid, tipo, estado_disponibilidad, formato_col, nombre, 
        fecha_documento_fuente, descripcion, fuente_administrativa_tipo, ente_emisor, ciudad_origen
    ) VALUES 
        (uuid_generate_v4(), 'Documento_Publico', 'Disponible', 'Documento', 'ESCRITURA_001', 
         '2025-01-01', 'Descripción de la fuente administrativa 1', 'Escritura_Publica', 
         'Notaría Primera Bogotá', 'Bogotá'),
        (uuid_generate_v4(), 'Documento_Publico', 'Disponible', 'Documento', 'RESOLUCION_001', 
         '2025-02-01', 'Descripción de la fuente administrativa 2', 'Acto_Administrativo', 
         'Alcaldía Medellín', 'Medellín');

    INSERT INTO ladm.cr_derecho_fuente (
        t_ili_tid, derecho_t_id, fuente_administrativa_t_id
    ) VALUES 
        (uuid_generate_v4(), 
         (SELECT t_ili_tid FROM ladm.cr_derecho WHERE numero_predial = '123456789012345678901234567890'), 
         (SELECT t_ili_tid FROM ladm.cr_fuenteadministrativa WHERE nombre = 'ESCRITURA_001')),
        (uuid_generate_v4(), 
         (SELECT t_ili_tid FROM ladm.cr_derecho WHERE numero_predial = '987654321098765432109876543210'), 
         (SELECT t_ili_tid FROM ladm.cr_fuenteadministrativa WHERE nombre = 'RESOLUCION_001'));

    INSERT INTO ladm.cr_contactovisita (
        t_ili_tid, numero_predial, nombre_contacto, tipo_documento, numero_documento, 
        telefono, tipo_visita, observaciones, fecha_visita, foto
    ) VALUES 
        (uuid_generate_v4(), '123456789012345678901234567890', 'María Gómez', 
         'Cedula_Ciudadania', '987654321', '3001234567', 'Efectiva', 
         'Contacto disponible, proporcionó información completa', '2025-01-02', 
         '/path/to/foto_visita_1.jpg'),
        (uuid_generate_v4(), '987654321098765432109876543210', 'Carlos López', 
         'Cedula_Ciudadania', '123456789', '3019876543', 'No_Efectiva', 
         'Nadie presente en el predio durante la visita', '2025-02-02', 
         '/path/to/foto_visita_2.jpg');

    INSERT INTO ladm.cca_adjunto (
        t_ili_tid, t_seq, archivo, observaciones, cca_fuenteadministrativa_ext_archivo
    ) VALUES 
        (uuid_generate_v4(), 1, 'escritura_001_pagina_1.pdf', 'Primera página de la escritura pública', 
         (SELECT t_ili_tid FROM ladm.cr_fuenteadministrativa WHERE nombre = 'ESCRITURA_001')),
        (uuid_generate_v4(), 2, 'escritura_001_pagina_2.pdf', 'Segunda página de la escritura pública', 
         (SELECT t_ili_tid FROM ladm.cr_fuenteadministrativa WHERE nombre = 'ESCRITURA_001')),
        (uuid_generate_v4(), 1, 'resolucion_001.pdf', 'Archivo completo de la resolución administrativa', 
         (SELECT t_ili_tid FROM ladm.cr_fuenteadministrativa WHERE nombre = 'RESOLUCION_001'));

    INSERT INTO ladm.extdireccion (
        t_ili_tid, tipo_direccion, es_direccion_principal, clase_via_principal, valor_via_principal, letra_via_principal, 
        valor_via_generadora, letra_via_generadora, numero_predio, complemento, nombre_predio, localizacion, numero_predial
    ) VALUES 
        (uuid_generate_v4(), 1, true, 1, 123, 'A', 456, 'B', 101, 
         'Apto 301', 'Edificio Torres del Parque', 
         ST_GeomFromText('POINT(1000050 1000050)', 9377), 
         '123456789012345678901234567890'),
        (uuid_generate_v4(), 2, true, 2, 789, 'B', 321, 'A', 205, 
         'Local 15', 'Centro Comercial Plaza', 
         ST_GeomFromText('POINT(1000250 1000250)', 9377), 
         '987654321098765432109876543210');

    -- Comentarios en las columnas para documentar la estructura
    COMMENT ON TABLE ladm.extdireccion IS 'Tabla para almacenar direcciones de predios con localización geográfica';
    COMMENT ON COLUMN ladm.extdireccion.t_id IS 'Identificador único de la dirección';
    COMMENT ON COLUMN ladm.extdireccion.t_ili_tid IS 'Identificador UUID para interoperabilidad';
    COMMENT ON COLUMN ladm.extdireccion.tipo_direccion IS 'Tipo de dirección (entero)';
    COMMENT ON COLUMN ladm.extdireccion.es_direccion_principal IS 'Indica si es la dirección principal del predio';
    COMMENT ON COLUMN ladm.extdireccion.clase_via_principal IS 'Clase de vía principal (entero)';
    COMMENT ON COLUMN ladm.extdireccion.valor_via_principal IS 'Número de la vía principal';
    COMMENT ON COLUMN ladm.extdireccion.letra_via_principal IS 'Letra de la vía principal';
    COMMENT ON COLUMN ladm.extdireccion.valor_via_generadora IS 'Número de la vía generadora';
    COMMENT ON COLUMN ladm.extdireccion.letra_via_generadora IS 'Letra de la vía generadora';
    COMMENT ON COLUMN ladm.extdireccion.numero_predio IS 'Número del predio en la dirección';
    COMMENT ON COLUMN ladm.extdireccion.complemento IS 'Complemento de la dirección (apartamento, local, etc.)';
    COMMENT ON COLUMN ladm.extdireccion.nombre_predio IS 'Nombre del predio o edificio';
    COMMENT ON COLUMN ladm.extdireccion.localizacion IS 'Punto geográfico de la dirección';
    COMMENT ON COLUMN ladm.extdireccion.numero_predial IS 'Número predial para relación con cr_predio';

    COMMENT ON COLUMN ladm.cr_fuenteadministrativa.t_ili_tid IS 'Identificador UUID único para la fuente administrativa';
    COMMENT ON COLUMN ladm.cr_derecho_fuente.fuente_administrativa_t_id IS 'Identificador UUID de la fuente administrativa asociada al derecho';
    COMMENT ON COLUMN ladm.cca_adjunto.cca_fuenteadministrativa_ext_archivo IS 'Identificador UUID de la fuente administrativa asociada al adjunto';

    COMMIT;
    """

    # Ejecutar el script
    for statement in sql_script.split(';'):
        if statement.strip():
            execute_sql(cursor, statement + ';', "Sentencia SQL ejecutada.")

    print("Base de datos simplificada LADM-COL creada con éxito, sin tablas de tipo.")

except Error as e:
    print(f"Error en la creación del esquema: {e}")
finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión cerrada.")