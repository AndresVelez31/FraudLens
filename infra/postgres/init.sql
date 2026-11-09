-- Catálogos transaccionales (fuente de catalogos_diario). Se ejecuta una sola vez al crear el contenedor.
CREATE TABLE IF NOT EXISTS clientes (
    id_cliente     TEXT PRIMARY KEY,
    nombre         TEXT NOT NULL,
    documento      TEXT NOT NULL,
    telefono       TEXT,
    correo         TEXT,
    ciudad         TEXT NOT NULL,
    segmento       TEXT NOT NULL,
    fecha_alta     DATE NOT NULL,
    actualizado_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS comercios (
    id_comercio TEXT PRIMARY KEY,
    nombre      TEXT NOT NULL,
    ciudad      TEXT NOT NULL,
    categoria   TEXT NOT NULL
);
-- Base aparte para Airflow y Metabase
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE metabase' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec
