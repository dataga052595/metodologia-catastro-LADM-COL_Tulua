# GENERAR_XTF_SIN_INTERLIS.py
# Genera archivo XTF manualmente desde tu BD LADM-COL simplificada
# Autor: Tú + Grok (2025)
# ¡SIN USAR NINGUNA HERRAMIENTA INTERLIS!

import psycopg2
from lxml import etree
from datetime import datetime
import uuid
import os

# === CONFIGURACIÓN ===
db_params = {
    "host": "localhost",
    "port": "5432",
    "dbname": "bd_ladm_13122025",
    "user": "postgres",
    "password": "postgres123"
}

schema = "ladm"
output_file = f"LADM_COL_TULUA_XTF_MANUAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xtf"

# Namespaces necesarios para XTF (INTERLIS 2)
NS = {
    "ili": "http://www.interlis.ch/INTERLIS2.3",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "gml": "http://www.opengis.net/gml/3.2"
}
etree.register_namespace("ili", NS["ili"])
etree.register_namespace("gml", NS["gml"])

# === CONEXIÓN ===
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# === CABECERA XTF ===
root = etree.Element("{" + NS["ili"] + "}TRANSFER", nsmap=NS)
header = etree.SubElement(root, "{" + NS["ili"] + "}HEADERSECTION")
etree.SubElement(header, "{" + NS["ili"] + "}SENDER").text = "Script Python Manual - Grok 2025"
etree.SubElement(header, "{" + NS["ili"] + "}DATE").text = datetime.now().strftime("%Y-%m-%d")
etree.SubElement(header, "{" + NS["ili"] + "}TIME").text = datetime.now().strftime("%H:%M:%S")

models = etree.SubElement(header, "{" + NS["ili"] + "}MODELS")
model = etree.SubElement(models, "{" + NS["ili"] + "}MODEL")
etree.SubElement(model, "{" + NS["ili"] + "}NAME").text = "LADM_COL_Simplificado_V1"
etree.SubElement(model, "{" + NS["ili"] + "}VERSION").text = "1.0"
etree.SubElement(model, "{" + NS["ili"] + "}URI").text = "https://example.com/ladm_col_simplificado"

data_section = etree.SubElement(root, "{" + NS["ili"] + "}DATASECTION")

# === FUNCIÓN PARA GEOMETRÍA GML ===
def geom_to_gml(geom_wkt):
    if not geom_wkt:
        return None
    # Muy básico: solo MultiPolygon (ajusta si necesitas otros tipos)
    gml_geom = etree.Element("{" + NS["gml"] + "}MultiSurface", srsName="EPSG:9377")
    surface_member = etree.SubElement(gml_geom, "{" + NS["gml"] + "}surfaceMember")
    polygon = etree.SubElement(surface_member, "{" + NS["gml"] + "}Polygon")
    exterior = etree.SubElement(polygon, "{" + NS["gml"] + "}exterior")
    linear_ring = etree.SubElement(exterior, "{" + NS["gml"] + "}LinearRing")
    pos_list = etree.SubElement(linear_ring, "{" + NS["gml"] + "}posList")
    
    # Extraer coordenadas del WKT (simplificado para MULTIPOLYGON)
    coords = []
    if geom_wkt.startswith("MULTIPOLYGON"):
        # Extrae coordenadas
        inner = geom_wkt[geom_wkt.find("((")+2 : geom_wkt.rfind("))")]
        for pair in inner.split(","):
            x, y = pair.strip().split()
            coords.extend([x, y])
    pos_list.text = " ".join(coords)
    return gml_geom

# === EXPORTAR PREDIOS, TERRENOS, UNIDADES, INTERESADOS Y DERECHOS ===
bid = str(uuid.uuid4())  # Basket ID único

# PREDIOS
cur.execute(f"SELECT t_ili_tid, numero_predial, departamento, municipio, destinacion_economica FROM {schema}.cr_predio")
for row in cur.fetchall():
    tid, npn, dept, mun, dest = row
    predio = etree.SubElement(data_section, "LADM_COL_Simplificado_V1.Predio", {"BID": bid, "TID": tid})
    etree.SubElement(predio, "numero_predial").text = npn or ""
    etree.SubElement(predio, "departamento").text = dept or ""
    etree.SubElement(predio, "municipio").text = mun or ""
    etree.SubElement(predio, "destinacion_economica").text = dest or ""

# TERRENOS
cur.execute(f"SELECT t_ili_tid, predio_t_ili_tid, ST_AsText(geometria), area FROM {schema}.cr_terreno")
for row in cur.fetchall():
    tid, predio_tid, wkt, area = row
    terreno = etree.SubElement(data_section, "LADM_COL_Simplificado_V1.Terreno", {"BID": bid, "TID": tid})
    etree.SubElement(terreno, "predio").text = predio_tid
    etree.SubElement(terreno, "area").text = str(area) if area else ""
    gml = geom_to_gml(wkt)
    if gml is not None:
        terreno.append(gml)

# UNIDADES CONSTRUCCIÓN
cur.execute(f"SELECT t_ili_tid, predio_t_ili_tid, ST_AsText(geometria), area, tipo_planta FROM {schema}.cr_unidadconstruccion")
for row in cur.fetchall():
    tid, predio_tid, wkt, area, planta = row
    unidad = etree.SubElement(data_section, "LADM_COL_Simplificado_V1.UnidadConstruccion", {"BID": bid, "TID": tid})
    etree.SubElement(unidad, "predio").text = predio_tid
    etree.SubElement(unidad, "area").text = str(area) if area else ""
    etree.SubElement(unidad, "tipo_planta").text = planta or ""
    gml = geom_to_gml(wkt)
    if gml is not None:
        unidad.append(gml)

# INTERESADOS
cur.execute(f"SELECT t_ili_tid, tipo_interesado, tipo_documento, numero_documento, primer_nombre, primer_apellido FROM {schema}.cr_interesado")
for row in cur.fetchall():
    tid, tipo_int, tipo_doc, num_doc, p_nombre, p_apellido = row
    interesado = etree.SubElement(data_section, "LADM_COL_Simplificado_V1.Interesado", {"BID": bid, "TID": tid})
    etree.SubElement(interesado, "tipo_interesado").text = tipo_int or ""
    etree.SubElement(interesado, "tipo_documento").text = tipo_doc or ""
    etree.SubElement(interesado, "numero_documento").text = num_doc or ""
    etree.SubElement(interesado, "primer_nombre").text = p_nombre or ""
    etree.SubElement(interesado, "primer_apellido").text = p_apellido or ""

# DERECHOS
cur.execute(f"SELECT t_ili_tid, predio_t_ili_tid, interesado_t_ili_tid, tipo, fraccion_derecho FROM {schema}.cr_derecho")
for row in cur.fetchall():
    tid, predio_tid, interesado_tid, tipo, fraccion = row
    derecho = etree.SubElement(data_section, "LADM_COL_Simplificado_V1.Derecho", {"BID": bid, "TID": tid})
    etree.SubElement(derecho, "predio").text = predio_tid
    etree.SubElement(derecho, "interesado").text = interesado_tid
    etree.SubElement(derecho, "tipo").text = tipo or "Dominio"
    etree.SubElement(derecho, "fraccion_derecho").text = str(fraccion) if fraccion else "1.0"

# === GUARDAR ARCHIVO ===
tree = etree.ElementTree(root)
tree.write(output_file, pretty_print=True, xml_declaration=True, encoding="UTF-8")

print(f"¡XTF generado exitosamente! → {os.path.abspath(output_file)}")
print("Este archivo es un XTF válido en estructura XML, pero como tu modelo es simplificado,")
print("no pasará validaciones oficiales de INTERLIS. Puedes abrirlo como XML o en visores básicos.")