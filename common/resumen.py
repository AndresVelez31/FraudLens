"""Texto del correo resumen de alertas. Python puro (sin Spark ni boto3): lo usan el streaming (common/alerts.py) y la
Lambda notificadora (lambdas/notificador), que lo empaqueta tal cual.

Las alertas son dicts como los de DynamoDB: regla, id_cliente, id_transaccion, id_comercio, monto_total, ts_evento,
ts_alerta y detalle (JSON; en R4 trae el puntaje)."""
import json
from datetime import datetime, timedelta, timezone

MAX_DETALLE = 100            # alertas que se listan (~13 KB; el límite de un mensaje de SNS es 256 KB)
COLOMBIA = timezone(timedelta(hours=-5), "COT")   # Colombia no tiene horario de verano


def puntaje(a):
    if a.get("regla") != "R4":
        return None
    try:
        return float(json.loads(a.get("detalle") or "{}")["puntaje"])
    except (ValueError, KeyError, TypeError):
        return None


def riesgo(a):
    """Orden del correo: primero R4 por puntaje; luego las demás por monto."""
    p = puntaje(a)
    return (0, -p) if p is not None else (1, -float(a.get("monto_total") or 0))


def hora_colombia(ts):
    """'2026-11-10T03:42:34.738Z' -> '2026-11-09 22:42:34 (hora de Colombia)'. Si no se puede leer, lo deja igual."""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(COLOMBIA).strftime("%Y-%m-%d %H:%M:%S") + " (hora de Colombia)"
    except ValueError:
        return str(ts)


def _plural(n, palabra):
    return "%d %s%s" % (n, palabra, "" if n == 1 else "s")


def resumen(conteo, alertas, desde=None, tabla="alertas_fraude"):
    """(asunto, cuerpo) del correo para `conteo` {regla: n} y las alertas de mayor riesgo."""
    total = sum(conteo.values())
    por_regla = ", ".join("%s: %d" % (r, conteo[r]) for r in sorted(conteo))
    nuevas = "%s nueva%s" % (_plural(total, "alerta"), "" if total == 1 else "s")
    asunto = ("FraudLens: %s (%s)" % (nuevas, por_regla))[:100]
    lineas = ["FraudLens detectó %s%s." % (nuevas, " desde %s" % hora_colombia(desde) if desde else ""), "",
              "Por regla: " + por_regla, "",
              ("Todas, de mayor a menor riesgo:" if total <= MAX_DETALLE else
               "Las %d de mayor riesgo (de %d):" % (min(MAX_DETALLE, len(alertas)), total)),
              "regla | cliente | transacción | tipo | monto | hora del pago (UTC) | puntaje"]
    for a in sorted(alertas, key=riesgo)[:MAX_DETALLE]:
        p = puntaje(a)
        lineas.append("- %s | %s | %s | %s | %s | %s | %s" % (
            a.get("regla"), a.get("id_cliente"), a.get("id_transaccion") or "(ventana de 5 min)",
            a.get("id_comercio") or "-", a.get("monto_total"), a.get("ts_evento"), "%.2f" % p if p is not None else "-"))
    if total > MAX_DETALLE:
        lineas.append("... y %d más." % (total - MAX_DETALLE))
    filtro = "&desde=%s" % desde if desde else ""
    lineas += ["", "Cada alerta tiene todos sus campos en la tabla %s de DynamoDB y en la API:" % tabla,
               "  todas las de este correo:   GET /alertas?limite=500%s" % filtro,
               "  solo las del modelo (R4):   GET /alertas?regla=R4%s" % filtro,
               "  las de un cliente:          GET /alertas?id_cliente=<cliente>"]
    return asunto, "\n".join(lineas)
