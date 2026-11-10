"""Carga de los catálogos reales (IEEE-CIS) en PostgreSQL: lectura del CSV de scripts/preparar_ieee.py."""
from producer import catalogo


def test_lee_el_csv_de_ieee_con_campos_vacios_como_null(tmp_path):
    (tmp_path / "clientes.csv").write_text(
        "id_cliente,nombre,documento,telefono,correo,ciudad,segmento,fecha_alta\n"
        "U13926_315_-13,Cliente U13926_315_-13,IEEE13926,,,R315,credit,2026-05-01\n", encoding="utf-8")
    filas = list(catalogo.leer_csv(str(tmp_path / "clientes.csv"), catalogo.COLS_CLIENTES))
    assert filas == [("U13926_315_-13", "Cliente U13926_315_-13", "IEEE13926", None, None, "R315", "credit",
                      "2026-05-01")]
