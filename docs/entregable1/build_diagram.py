#!/usr/bin/env python3
"""Genera arquitectura.drawio (notación AWS de draw.io) y enlace_drawio.txt.

Uso: python3 build_diagram.py [carpeta_salida]
El PNG y el SVG se exportan desde el .drawio con render_diagram.js (visor oficial de draw.io).
"""
import base64
import sys
import urllib.parse
import zlib
from xml.sax.saxutils import escape

OUT = sys.argv[1] if len(sys.argv) > 1 else "."
W, H = 1740, 1150
FONT = "Helvetica"
INK = "#232F3E"
COLW = 290
CX = [40, 345, 650, 985, 1290]          # x de cada columna (capa)
LAYERS = [("1 · FUENTE", "#0F6E56", "#F1F8F5"), ("2 · INGESTIÓN", "#185FA5", "#F1F5FB"),
          ("3 · PROCESAMIENTO", "#854F0B", "#FCF7EF"), ("4 · ALMACENAMIENTO", "#534AB7", "#F5F4FC"),
          ("5 · CONSUMO", "#993556", "#FCF2F6")]
ALERT, LOOKUP = "#D13212", "#7D8998"

# Estilos de la biblioteca AWS 2023 de draw.io (Sidebar-AWS4.js)
PTS = ("points=[[0,0,0],[0.25,0,0],[0.5,0,0],[0.75,0,0],[1,0,0],[0,1,0],[0.25,1,0],[0.5,1,0],[0.75,1,0],[1,1,0],"
       "[0,0.25,0],[0,0.5,0],[0,0.75,0],[1,0.25,0],[1,0.5,0],[1,0.75,0]];")
RES = (PTS + "sketch=0;outlineConnect=0;fontColor=#232F3E;fillColor={c};strokeColor=#ffffff;dashed=0;"
       "verticalLabelPosition=bottom;verticalAlign=top;align=center;html=1;fontSize=12;fontStyle=0;aspect=fixed;"
       "shape=mxgraph.aws4.resourceIcon;resIcon=mxgraph.aws4.{i};")
ICON = ("sketch=0;outlineConnect=0;fontColor=#232F3E;gradientColor=none;fillColor={c};strokeColor=none;dashed=0;"
        "verticalLabelPosition=bottom;verticalAlign=top;align=center;html=1;fontSize=12;fontStyle=0;aspect=fixed;"
        "pointerEvents=1;shape=mxgraph.aws4.{i};")
GROUP = ("points=[[0,0],[0.25,0],[0.5,0],[0.75,0],[1,0],[1,0.25],[1,0.5],[1,0.75],[1,1],[0.75,1],[0.5,1],[0.25,1],"
         "[0,1],[0,0.75],[0,0.5],[0,0.25]];outlineConnect=0;gradientColor=none;html=1;whiteSpace=wrap;fontSize=13;"
         "fontStyle=1;container=0;pointerEvents=0;collapsible=0;recursiveResize=0;shape=mxgraph.aws4.group;"
         "grIcon=mxgraph.aws4.{g};strokeColor={s};fillColor={f};verticalAlign=top;align=left;spacingLeft=30;"
         "fontColor={fc};dashed={d};fontFamily=" + FONT + ";")
C_COMPUTE, C_STORAGE, C_DB, C_ANALYTICS, C_INTEG = "#ED7100", "#7AA116", "#C925D1", "#8C4FFF", "#E7157B"

cells = []


def vertex(cid, value, x, y, w, h, style):
    cells.append(
        f'<mxCell id="{cid}" value="{escape(value, {chr(34): "&quot;"})}" style="{style}" vertex="1" parent="1">'
        f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')


def text(cid, value, x, y, w, h, size=12, color=INK, align="left", bold=False):
    vertex(cid, value, x, y, w, h,
           f"text;html=1;fillColor=none;strokeColor=none;align={align};verticalAlign=middle;whiteSpace=wrap;"
           f"fontSize={size};fontColor={color};fontStyle={1 if bold else 0};fontFamily={FONT};")


def edge(cid, src, tgt, color=INK, dashed=False, pts=(), exit_=None, entry=None, width=1.8, label=""):
    st = (f"edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;strokeColor={color};strokeWidth={width};endArrow=block;"
          f"endFill=1;endSize=6;fontSize=11;fontColor={color};labelBackgroundColor=#FFFFFF;fontFamily={FONT};")
    if dashed:
        st += "dashed=1;dashPattern=6 4;"
    if exit_:
        st += f"exitX={exit_[0]};exitY={exit_[1]};exitDx=0;exitDy=0;"
    if entry:
        st += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
    geo = '<mxGeometry relative="1" as="geometry">'
    if pts:
        geo += '<Array as="points">' + "".join(f'<mxPoint x="{a}" y="{b}"/>' for a, b in pts) + "</Array>"
    cells.append(f'<mxCell id="{cid}" value="{escape(label)}" style="{st}" edge="1" parent="1" source="{src}" '
                 f'target="{tgt}">{geo + "</mxGeometry>"}</mxCell>')


def card(cid, layer, x, y, w, h, icon_style, title, lines, icon=40):
    """Tarjeta de componente: borde del color de la capa, ícono AWS a la izquierda y texto."""
    color = LAYERS[layer][1]
    vertex(cid, "", x, y, w, h, f"rounded=1;arcSize=4;html=1;fillColor=#FFFFFF;strokeColor={color};strokeWidth=1.6;")
    vertex(cid + "_i", "", x + 8, y + 9, icon, icon, icon_style)
    body = "<br>".join(lines)
    val = (f'<b style="font-size:13.5px;color:{color}">{title}</b><br>'
           f'<span style="font-size:11.8px;color:#232F3E">{body}</span>')
    vertex(cid + "_t", val, x + icon + 14, y + 3, w - icon - 18, h - 6,
           f"text;html=1;fillColor=none;strokeColor=none;align=left;verticalAlign=top;whiteSpace=wrap;"
           f"spacingTop=2;fontFamily={FONT};")


CONTAINER = ICON.format(c=C_COMPUTE, i="container_1")

# ---------- Título y carriles de capas ----------
text("title", "<b>FraudLens · Arquitectura de datos por capas en AWS</b>", 20, 8, 1200, 28, size=21)
text("subtitle", "Detección de fraude en pagos digitales (fintech colombiana) · streaming + batch sobre lakehouse "
     "Bronze/Silver/Gold · AWS Academy Learner Lab · notación AWS Architecture Icons", 20, 34, 1400, 20,
     size=12.5, color="#5F6B7A")
for i, (name, strong, soft) in enumerate(LAYERS):
    vertex(f"lane{i}", "", CX[i] - 8, 62, COLW + 16, 838, f"rounded=0;fillColor={soft};strokeColor=none;html=1;")
    vertex(f"hdr{i}", name, CX[i], 68, COLW, 30,
           f"rounded=1;arcSize=20;html=1;fillColor={strong};strokeColor=none;fontColor=#FFFFFF;fontStyle=1;"
           f"fontSize=14;fontFamily={FONT};")

# ---------- Grupos AWS ----------
vertex("g_cloud", "AWS Cloud", 22, 112, CX[4] + COLW - 4, 778,
       GROUP.format(g="group_aws_cloud_alt", s=INK, f="none", fc=INK, d=0))
vertex("g_region", "Region us-east-1 (AWS Academy Learner Lab)", 30, 146, CX[4] + COLW - 20, 736,
       GROUP.format(g="group_region", s="#00A4A6", f="none", fc="#147EBA", d=1))
vertex("g_ec2a", "EC2-A · t3.large · perfil streaming (docker compose)", 36, 186, CX[2] + COLW - 30, 288,
       GROUP.format(g="group_ec2_instance_contents", s="#D86613", f="none", fc="#D86613", d=0))
vertex("g_ec2b", "EC2-B · t3.large · perfil batch y orquestación (docker compose)", 36, 494, CX[2] + COLW - 30, 200,
       GROUP.format(g="group_ec2_instance_contents", s="#D86613", f="none", fc="#D86613", d=0))
vertex("g_ec2c", "EC2-C · t3.medium · consumo", CX[4] - 6, 596, COLW + 12, 274,
       GROUP.format(g="group_ec2_instance_contents", s="#D86613", f="none", fc="#D86613", d=0))
vertex("g_s3", "", CX[3] - 6, 186, COLW + 12, 420,
       f"rounded=0;html=1;fillColor=none;strokeColor={C_STORAGE};strokeWidth=1.5;dashed=1;dashPattern=8 4;")
vertex("g_s3_i", "", CX[3] - 6, 186, 26, 26, RES.format(c=C_STORAGE, i="s3"))
text("g_s3_t", "<b>Amazon S3</b> · bucket fraudlens-lake", CX[3] + 26, 186, COLW - 30, 26, size=13, color="#3F8624")

# ---------- 1 · Fuente ----------
card("prod", 0, CX[0], 226, COLW - 14, 116, CONTAINER, "Productor de transacciones",
     ["Python + Faker (simula la app y la API)", "Eventos JSON · ~1.000 ev/s, pico 5.000",
      "Escenarios de fraude, duplicados", "y JSON malformado (semilla fija)"])
card("pg", 0, CX[0], 534, COLW - 14, 96, CONTAINER, "PostgreSQL 16 (operacional)",
     ["Clientes y comercios (relacional)", "Contenedor que simula la BD de la app", "Extracción batch diaria"])
card("csv", 0, CX[0], 724, COLW - 14, 96, ICON.format(c=C_STORAGE, i="bucket_with_objects"),
     "Listas de riesgo (CSV)", ["Comercios y dispositivos en lista negra", "Se suben a s3://…/landing/",
                                "Versión nueva periódica"])

# ---------- 2 · Ingestión ----------
card("kafka", 1, CX[1], 226, COLW - 14, 232, CONTAINER, "Apache Kafka · 3 brokers KRaft",
     ["cp-kafka 7.6.0 (Lab 2a)", "Topic transacciones · 12 particiones", "Clave id_cliente · replicación 3",
      "acks=all · min.insync.replicas=2 (CP)", "Productor idempotente", "Retención 7 días (replay)",
      "Garantía at-least-once", "Topic transacciones_dlq (inválidos)"])
card("extract", 1, CX[1], 534, COLW - 14, 136, CONTAINER, "Airflow + PySpark (extracción)",
     ["JDBC PostgreSQL → Bronze (diario)", "S3KeySensor: CSV nuevo → Bronze",
      "MERGE idempotente (at-least-once)", "3 reintentos · espera exponencial"])

# ---------- 3 · Procesamiento ----------
card("stream", 2, CX[2], 226, COLW - 14, 232, CONTAINER, "Spark Structured Streaming",
     ["Spark 3.5.1 + Delta 3.1.0 · trigger 5 s", "JSON inválido → DLQ · esquema",
      "Dedup id_transaccion (watermark 10 min)", "R1 · P1: ventana 5 min / 1 min (update)",
      "R2 · P2: dispositivo nuevo y monto", "&nbsp;&nbsp;&nbsp;&gt; 3× promedio de 30 días",
      "R3 · P3: lista de riesgo (join)", "Checkpoint en S3 · restart automático"])
card("batch", 2, CX[2], 534, COLW - 14, 136, CONTAINER, "Spark batch dentro de Airflow",
     ["Patrón del Lab 4a · cada 10 min", "Silver → Gold: SCD2 + hechos (MERGE)",
      "Métricas P4–P6 · OPTIMIZE ZORDER", "Reproceso por rule_version (Gold)"])

# ---------- 4 · Almacenamiento ----------
BK = ICON.format(c=C_STORAGE, i="bucket")
card("bronze", 3, CX[3], 226, COLW, 84, BK, "Bronze · bronze/",
     ["Crudo, append-only · topic, partición,", "offset, ingest_ts · catálogos y listas"], icon=34)
card("silver", 3, CX[3], 330, COLW, 100, BK, "Silver · silver/",
     ["Limpio, tipado, sin duplicados (MERGE)", "PII con HMAC-SHA-256 · cuarentena", "Tabla de alertas"], icon=34)
card("gold", 3, CX[3], 450, COLW, 142, BK, "Gold · gold/ (esquema en estrella)",
     ["fact_transacciones · fact_alertas", "dim_cliente (SCD2) · dim_comercio",
      "dim_dispositivo · dim_tiempo · dim_regla", "Partición por fecha · time travel 90 d"], icon=34)
card("ddb", 3, CX[3], 680, COLW, 150, RES.format(c=C_DB, i="dynamodb"), "Amazon DynamoDB",
     ["Tabla alertas_fraude (on-demand)", "PK id_cliente · SK ts_alerta#regla", "GSI por regla",
      "PutItem condicional (idempotente)", "Lectura eventual (AP)"], icon=46)

# ---------- 5 · Consumo ----------
card("sns", 4, CX[4], 196, COLW, 100, RES.format(c=C_INTEG, i="sns"), "Amazon SNS",
     ["Topic alertas-fraude → correo", "Solo alertas nuevas", "También fallos de Airflow"], icon=46)
card("athena", 4, CX[4], 380, COLW, 150, RES.format(c=C_ANALYTICS, i="athena"), "Amazon Athena",
     ["SQL sobre Gold para P4, P5 y P6", "Menos de 10 s (30 días)", "Tablas registradas en", "AWS Glue Data Catalog",
      "(table_type = DELTA, Lab 1b)"], icon=46)
vertex("glue_i", "", CX[4] + COLW - 50, 470, 40, 40, ICON.format(c=C_ANALYTICS, i="glue_data_catalog"))
card("metabase", 4, CX[4], 636, COLW - 14, 96, CONTAINER, "Metabase",
     ["Dashboards de fraude por comercio,", "ciudad y franja horaria", "Conector Amazon Athena"])
card("api", 4, CX[4], 752, COLW - 14, 100, CONTAINER, "API de alertas (FastAPI)",
     ["GET /alertas por cliente, regla", "y rango de tiempo · lee DynamoDB", "Alerta en &lt; 30 s (P95)"])

# ---------- Usuarios fuera de la nube ----------
vertex("users", "<b>Analistas de fraude</b><br>y jefe de riesgo", CX[4] + COLW + 44, 700, 64, 64,
       ICON.format(c=INK, i="users").replace("fontSize=12", "fontSize=12;fontFamily=" + FONT))

# ---------- Flujos de datos ----------
GAPX = CX[2] + COLW - 14 + 22         # corredor entre Procesamiento y Almacenamiento (EC2 -> S3)
TRUNK = CX[3] - 22                    # tronco de alertas
edge("e1", "prod", "kafka", exit_=(1, 0.4), entry=(0, 0.2))
edge("e2", "kafka", "stream", exit_=(1, 0.3), entry=(0, 0.3))
edge("e3", "stream", "bronze", exit_=(1, 0.12), entry=(0, 0.331))
edge("e4", "stream", "silver", exit_=(1, 0.52), entry=(0, 0.166))
edge("e5", "pg", "extract", exit_=(1, 0.5), entry=(0, 0.35))
edge("e6", "csv", "extract", exit_=(1, 0.5), entry=(0, 0.75), pts=[(CX[1] - 20, 772), (CX[1] - 20, 636)])
edge("e7", "extract", "bronze", exit_=(0.75, 0), entry=(0, 0.75), pts=[(CX[1] + 207, 482), (CX[3] - 14, 482), (CX[3] - 14, 289)])
edge("e8", "bronze", "silver", exit_=(0.5, 1), entry=(0.5, 0))
edge("e9", "silver", "gold", exit_=(0.5, 1), entry=(0.5, 0))
edge("e10", "batch", "gold", exit_=(1, 0.25), entry=(0, 0.72))
edge("e11", "gold", "athena", exit_=(1, 0.35), entry=(0, 0.6))
edge("e12", "athena", "metabase", exit_=(0.75, 1), entry=(0.75, 0))
edge("e13", "ddb", "api", exit_=(1, 0.65), entry=(0, 0.5))
edge("e14", "metabase", "users", exit_=(1, 0.5), entry=(0, 0.25), pts=[(CX[4] + COLW + 22, 684), (CX[4] + COLW + 22, 716)], width=1.4)
edge("e15", "api", "users", exit_=(1, 0.3), entry=(0, 0.75), pts=[(CX[4] + COLW + 22, 782), (CX[4] + COLW + 22, 748)], width=1.4)
edge("e16", "sns", "users", ALERT, dashed=True, exit_=(1, 0.5), entry=(0.5, 0), pts=[(CX[4] + COLW + 76, 246)])
# Alertas de baja latencia
edge("a1", "stream", "ddb", ALERT, dashed=True, exit_=(1, 0.9), entry=(0, 0.3), pts=[(TRUNK, 435), (TRUNK, 725)])
edge("a2", "stream", "sns", ALERT, dashed=True, exit_=(0.75, 0), entry=(0.5, 0),
     pts=[(CX[2] + 207, 170), (CX[4] + 145, 170)])
# Lookup de dimensiones (join stream-static)
edge("l1", "gold", "stream", LOOKUP, dashed=True, exit_=(0, 0.12), entry=(0.85, 1), width=1.4,
     pts=[(CX[2] + 235, 467)])

# ---------- Orquestación ----------
OY = 906
vertex("orq", "", 22, OY, CX[4] + COLW - 4, 112, "rounded=1;arcSize=5;fillColor=#F4F2EC;strokeColor=#8A877F;html=1;")
vertex("orq_i", "", 34, OY + 10, 30, 30, CONTAINER)
text("orq_t", '<b>ORQUESTACIÓN · Apache Airflow 2.9 (LocalExecutor, contenedor en EC2-B)</b>'
     '<span style="color:#5F6B7A">&nbsp;&nbsp;— dependencias all_success: un fallo detiene los pasos siguientes · '
     '3 reintentos con espera exponencial · notificación por SNS</span>', 72, OY + 10, 1500, 30, size=13)
TASK = ("rounded=1;html=1;fillColor=#FFFFFF;strokeColor=#5F5E5A;fontColor=#232F3E;fontSize=12;"
        "fontFamily=Courier New;align=center;verticalAlign=middle;")


def dag(prefix, y, label, sub, tasks):
    text(prefix + "_l", f'<b>{label}</b><br><span style="color:#5F6B7A;font-size:11.5px">{sub}</span>',
         40, y - 4, 210, 36, size=13)
    x, prev = 260, None
    for i, t in enumerate(tasks):
        tid, w = f"{prefix}{i}", 8.3 * len(t) + 22
        vertex(tid, t, x, y, w, 26, TASK)
        if prev:
            edge(f"{tid}_e", prev, tid, "#5F5E5A", width=1.3)
        prev, x = tid, x + w + 26


dag("d1", OY + 46, "DAG catalogos_diario", "diario 02:00 o al llegar un CSV",
    ["extract_postgres", "sensor_csv_riesgo", "load_bronze", "silver_catalogos"])
dag("d2", OY + 80, "DAG fraude_pipeline", "cada 10 minutos",
    ["check_streaming_vivo", "gold_dimensiones", "gold_hechos", "quality_checks", "registrar_glue"])

# ---------- Leyenda ----------
LY = 1034
text("lg", "<b>Leyenda</b>", 22, LY, 80, 20, size=12.5)
items = [("Flujo de datos", INK, False), ("Alertas de fraude (baja latencia)", ALERT, True),
         ("Lookup de dimensiones desde Gold (join stream-static)", LOOKUP, True)]
x = 100
for i, (t, col, dsh) in enumerate(items):
    st = f"html=1;strokeColor={col};strokeWidth=1.8;endArrow=block;endFill=1;endSize=6;" + ("dashed=1;dashPattern=6 4;" if dsh else "")
    cells.append(f'<mxCell id="lg_e{i}" style="{st}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
                 f'<mxPoint x="{x}" y="{LY + 10}" as="sourcePoint"/><mxPoint x="{x + 42}" y="{LY + 10}" as="targetPoint"/>'
                 "</mxGeometry></mxCell>")
    tw = 6.9 * len(t) + 12
    text(f"lg_t{i}", t, x + 50, LY, tw, 20, size=12)
    x += 50 + tw + 22
vertex("lg_c", "", 22, LY + 30, 26, 26, CONTAINER)
text("lg_ct", "Contenedor Docker (software abierto que cobra solo la EC2)", 54, LY + 33, 360, 20, size=12)
vertex("lg_g", "", 430, LY + 30, 26, 26, GROUP.format(g="group_ec2_instance_contents", s="#D86613", f="none", fc="#D86613", d=0))
text("lg_gt", "Grupo EC2 instance contents (qué corre en cada instancia)", 462, LY + 33, 380, 20, size=12)
vertex("lg_s", "", 860, LY + 30, 26, 26, RES.format(c=C_ANALYTICS, i="athena"))
text("lg_st", "Servicio administrado de AWS que cobra por uso (S3, DynamoDB, Athena, Glue, SNS)", 892, LY + 33, 560, 20, size=12)
text("lg_n", "Un mismo docker-compose.yml con perfiles streaming, batch y consumo; también corre completo en un "
     "portátil. Latencia: evento → alerta &lt; 30 s (P95) · evento → Gold &lt; 15 min. Credenciales: LabInstanceProfile.",
     22, LY + 62, 1600, 20, size=11.5, color="#5F6B7A")

model = (f'<mxGraphModel dx="{W}" dy="{H}" grid="0" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" '
         f'fold="1" page="1" pageScale="1" pageWidth="{W}" pageHeight="{H}" background="#FFFFFF" math="0" shadow="0">'
         '<root><mxCell id="0"/><mxCell id="1" parent="0"/>' + "".join(cells) + "</root></mxGraphModel>")
mxfile = (f'<mxfile host="app.diagrams.net"><diagram id="fraudlens-arq" name="Arquitectura FraudLens">'
          f"{model}</diagram></mxfile>")
with open(f"{OUT}/arquitectura.drawio", "w", encoding="utf-8") as f:
    f.write(mxfile)

raw = urllib.parse.quote(mxfile, safe="")
comp = zlib.compressobj(9, zlib.DEFLATED, -15)
data = base64.b64encode(comp.compress(raw.encode()) + comp.flush()).decode()
with open(f"{OUT}/enlace_drawio.txt", "w", encoding="utf-8") as f:
    f.write("https://app.diagrams.net/#R" + urllib.parse.quote(data, safe="") + "\n")
print("ok", len(cells), "celdas")
