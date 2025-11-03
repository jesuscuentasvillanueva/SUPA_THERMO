import os
import sys
import argparse
import ssl
import json
import csv
import time
from datetime import datetime, timezone
import pg8000.dbapi as pg
import snap7
from snap7.util import get_bool, get_int, get_dint, get_real, get_word, get_dword


def get_arg_or_env(parser):
    p = parser.parse_args()
    p.db_host = p.db_host or os.getenv("DB_HOST") or "aws-0-us-east-1.pooler.supabase.com"
    p.db_port = p.db_port or int(os.getenv("DB_PORT") or 5432)
    p.db_name = p.db_name or os.getenv("DB_NAME") or "postgres"
    p.db_user = p.db_user or os.getenv("DB_USER") or "postgres.tnlbuupmkvqbqcdanldh"
    p.db_password = p.db_password or os.getenv("DB_PASSWORD") or "Migiva2025_2026"
    p.sslmode = (p.sslmode or os.getenv("DB_SSLMODE") or "require").lower()
    p.schema = p.schema or os.getenv("DB_SCHEMA") or "thermo"
    p.plc_ip = p.plc_ip or os.getenv("PLC_IP") or "195.168.1.10"
    p.rack = p.rack if p.rack is not None else int(os.getenv("PLC_RACK") or 0)
    p.slot = p.slot if p.slot is not None else int(os.getenv("PLC_SLOT") or 0)
    p.config = getattr(p, "config", None) or os.getenv("PLC_CONFIG") or "plc_config.json"
    p.interval = p.interval if p.interval is not None else int(os.getenv("INGEST_INTERVAL_SEC") or 120)
    missing = [k for k in ["db_host", "db_port", "db_name", "db_user", "db_password"] if getattr(p, k, None) in (None, "")]
    if missing:
        print("Faltan parámetros: " + ", ".join(missing), file=sys.stderr)
        sys.exit(1)
    return p


def connect_db(p):
    mode = (p.sslmode or "require").lower()
    ssl_ctx = None
    if mode != "disable":
        ssl_ctx = ssl.create_default_context()
        if mode in ("require", "prefer", "allow"):
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        elif mode == "verify-ca":
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        elif mode == "verify-full":
            ssl_ctx.check_hostname = True
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        else:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
    conn = pg.connect(user=p.db_user, password=p.db_password, host=p.db_host, port=p.db_port, database=p.db_name, ssl_context=ssl_ctx)
    try:
        cur = conn.cursor()
        try:
            cur.execute("SET statement_timeout TO 30000")
        finally:
            cur.close()
    except Exception:
        pass
    return conn


def insert_sensor_valor(conn, schema, id_fundo, id_sensorlocalizacion, id_metrica, valor, fecha):
    q = f"INSERT INTO {schema}.sensor_valor (id_fundo, id_sensorlocalizacion, id_metrica, valor, fecha) VALUES (%s, %s, %s, %s, %s)"
    cur = conn.cursor()
    try:
        cur.execute(q, (id_fundo, id_sensorlocalizacion, id_metrica, valor, fecha))
        conn.commit()
    finally:
        cur.close()


def connect_plc(ip, rack, slot):
    c = snap7.client.Client()
    c.connect(ip, rack, slot)
    return c


def parse_value(t, data, bit=None):
    tt = t.upper()
    if tt == "REAL":
        return float(get_real(data, 0))
    if tt == "INT":
        return int(get_int(data, 0))
    if tt == "DINT":
        return int(get_dint(data, 0))
    if tt == "WORD":
        return int(get_word(data, 0))
    if tt == "DWORD":
        return int(get_dword(data, 0))
    if tt == "BOOL":
        b = int(bit or 0)
        return 1.0 if get_bool(data, 0, b) else 0.0
    raise ValueError("Tipo no soportado: " + t)


def type_size(t):
    tt = t.upper()
    if tt in ("REAL", "DINT", "DWORD"):
        return 4
    if tt in ("INT", "WORD"):
        return 2
    if tt == "BOOL":
        return 1
    raise ValueError("Tipo no soportado: " + t)


def load_config(path):
    if path.lower().endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            vars_list = cfg.get("variables") or []
            return vars_list
    if path.lower().endswith(".csv"):
        out = []
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                out.append({
                    "name": row.get("name"),
                    "db": int(row.get("db") or row.get("db_number") or 1),
                    "offset": int(row.get("offset") or 0),
                    "type": row.get("type") or "REAL",
                    "bit": int(row.get("bit") or 0),
                    "id_fundo": int(row.get("id_fundo") or 0),
                    "id_sensorlocalizacion": int(row.get("id_sensorlocalizacion") or 0),
                    "id_metrica": int(row.get("id_metrica") or 0),
                    "scale": float(row.get("scale") or 1.0),
                    "bias": float(row.get("bias") or 0.0),
                })
        return out
    raise ValueError("Extensión de archivo no soportada")


def read_and_ingest_once(args, conn, plc, variables):
    schema = args.schema or "thermo"
    now = datetime.now(timezone.utc)
    for v in variables:
        try:
            dbn = int(v.get("db") or v.get("db_number") or 1)
            off = int(v.get("offset") or 0)
            t = v.get("type") or "REAL"
            sz = type_size(t)
            data = plc.db_read(dbn, off, sz)
            val = parse_value(t, data, v.get("bit"))
            sc = float(v.get("scale") or 1.0)
            bs = float(v.get("bias") or 0.0)
            val = float(val) * sc + bs
            id_fundo = int(v.get("id_fundo"))
            id_sensorlocalizacion = int(v.get("id_sensorlocalizacion"))
            id_metrica = int(v.get("id_metrica"))
            insert_sensor_valor(conn, schema, id_fundo, id_sensorlocalizacion, id_metrica, val, now)
            print(f"OK {v.get('name') or ''} -> {schema}.sensor_valor {id_fundo},{id_sensorlocalizacion},{id_metrica}={val} @ {now.isoformat()}")
        except Exception as e:
            print(f"ERROR {v.get('name') or ''}: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host")
    parser.add_argument("--db-port", type=int)
    parser.add_argument("--db-name")
    parser.add_argument("--db-user")
    parser.add_argument("--db-password")
    parser.add_argument("--sslmode")
    parser.add_argument("--schema")
    parser.add_argument("--plc-ip")
    parser.add_argument("--rack", type=int)
    parser.add_argument("--slot", type=int)
    parser.add_argument("--config")
    parser.add_argument("--interval", type=int)
    args = get_arg_or_env(parser)
    try:
        conn = connect_db(args)
    except Exception as e:
        print(f"Error de conexión DB: {e}", file=sys.stderr)
        sys.exit(2)
    try:
        plc = connect_plc(args.plc_ip, args.rack, args.slot)
    except Exception as e:
        print(f"Error de conexión PLC: {e}", file=sys.stderr)
        try:
            conn.close()
        except Exception:
            pass
        sys.exit(2)
    try:
        variables = load_config(args.config)
        if args.interval and args.interval > 0:
            while True:
                read_and_ingest_once(args, conn, plc, variables)
                time.sleep(args.interval)
        else:
            read_and_ingest_once(args, conn, plc, variables)
    finally:
        try:
            plc.disconnect()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
