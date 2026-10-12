#!/usr/bin/env python3
"""Arma e1_fraudlens.tex a partir de e1_fraudlens.src.tex (enlace de draw.io y bitácora) y compila el PDF."""
import subprocess

BITACORA = [
    ("Definición del problema y preguntas P1--P6 (Sección 1)", "Parcial",
     "Redactada por el equipo. El agente (Claude) buscó cifras de mercado con su fuente (Banco de la República, Asobancaria, Nequi) y las incorporó."),
    ("Requisitos funcionales y no funcionales (Sección 2)", "Parcial",
     "Redactados por el equipo. El agente señaló inconsistencias (RF-04 frente a RNF-03, CAP por componente, RF faltante para la API) y los adaptó al despliegue en AWS."),
    ("Elección de tecnologías (Kafka, Spark, Delta Lake, Airflow)", "No",
     "Decisión de diseño central tomada por el equipo antes de usar el agente."),
    ("Despliegue en AWS Learner Lab (contenedores en EC2 y servicios por uso) y cambio de MongoDB a DynamoDB", "Parcial",
     "Propuestos por el agente tras revisar los laboratorios del curso y el límite de instancias del Learner Lab; el equipo los aprobó."),
    ("Redacción de los ADR (Sección 4)", "Parcial",
     "El equipo definió contexto, alternativas y decisión; el agente ayudó a redactar y a citar los conceptos del curso."),
    ("Plan de implementación (Sección 5)", "Parcial",
     "Reparto de bloques acordado por el equipo; el agente redactó el cronograma y la tabla de riesgos."),
    ("Dibujo del diagrama en draw.io (notación AWS) y exportación", "Sí",
     "El agente generó el .drawio con los íconos oficiales de AWS a partir del diseño del equipo (\\texttt{build\\_diagram.py}). Bajo valor pedagógico."),
    ("Maquetación en LaTeX, referencias APA y anexos", "Sí", "Tarea de formato; bajo valor pedagógico."),
]


def esc_url(u):
    return u.replace("%", "\\%").replace("#", "\\#")


src = open("e1_fraudlens.src.tex", encoding="utf-8").read()
url = open("enlace_drawio.txt", encoding="utf-8").read().strip()
rows = "\n".join(f"{t} & {d} & {j}\\\\[1pt]" for t, d, j in BITACORA)
out = src.replace("@@DRAWIO_URL@@", esc_url(url)).replace("@@BITACORA@@", rows)
open("e1_fraudlens.tex", "w", encoding="utf-8").write(out)
subprocess.run(["latexmk", "-pdf", "-interaction=nonstopmode", "-halt-on-error", "e1_fraudlens.tex"],
               check=True, stdout=subprocess.DEVNULL)
subprocess.run(["latexmk", "-c"], stdout=subprocess.DEVNULL)
print("PDF listo")
