# dashboards

**Responsable:** Hellen Yanes Doria · **Par revisor:** Andrés Felipe Vélez Álvarez

Consumo de la capa Gold (RF-14): consultas de Athena para P4–P6 y el tablero de Power BI sobre Athena.

| Carpeta | Qué tiene |
|---|---|
| `athena/` | SQL de P4, P5 y P6 (los mismos que usa `batch/consultas.py` con Spark SQL en local) |
| `powerbi/` | Tablero `FraudLens.pbip` (proyecto de Power BI en archivos de texto: modelo en TMDL y reporte en PBIR) |

## Tablero de Power BI

Página **FraudLens**: tarjetas (transacciones, alertas, % de transacciones sospechosas y latencia P95), **P4** (matriz de
tasa de fraude por ciudad del comercio y franja, con degradado de riesgo), **P5** (alertas y latencia por regla) y **P6**
(top 10 de comercios por monto sospechoso). Usa el tema `FraudLens-tema.json` y un modelo en estrella:
`fact_transacciones` y `fact_alertas` con `dim_comercio`, `dim_tiempo`, `dim_cliente` (solo filas vigentes de la SCD2)
y `dim_regla`.

Los datos se importan desde Athena (modo **Import**): Athena escanea Gold una vez por actualización y los clics en el
tablero no cuestan. Las EC2 pueden estar apagadas, porque Athena lee directo de S3.

### Cómo abrirlo en otro computador

1. Instalar **Power BI Desktop** (Microsoft Store) y el **driver ODBC 2.x de Amazon Athena** (64 bits).
2. Crear el perfil de AWS con la llave del usuario IAM de solo lectura `powerbi-athena` (pedirla al dueño del
   proyecto; nunca va en el repositorio):
   ```bash
   aws configure --profile powerbi-athena      # región us-east-2
   ```
3. Abrir `powerbi/FraudLens.pbip` y hacer **Refresh**.

Las consultas usan esta cadena ODBC, sin DSN (el conector "Amazon Athena" de Power BI no leía bien el DSN del driver 2.5):

```
Driver={Amazon Athena ODBC (x64)};AwsRegion=us-east-2;Catalog=AwsDataCatalog;Schema=fraudlens;Workgroup=fraudlens;S3OutputLocation=s3://fraudlens-lake-216408407230/athena-results/;S3OutputEncOption=SSE_S3;AuthenticationType=IAM Profile;AWSProfile=powerbi-athena
```

El usuario `powerbi-athena` solo puede leer `gold/`, escribir en `athena-results/` y consultar el workgroup
`fraudlens`, que tiene un límite de 1 GB escaneado por consulta. Al final del semestre se borran su llave y el usuario.

`.pbi/localSettings.json` y `.pbi/cache.abf` (caché de datos) son de cada máquina y están en `.gitignore`.
