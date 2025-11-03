import os
import sys
import argparse
import ssl
import pg8000.dbapi as pg
from datetime import datetime, timezone


def get_arg_or_env(parser):
    p = parser.parse_args()
    p.db_host = p.db_host or os.getenv('DB_HOST')
    p.db_port = p.db_port or int(os.getenv('DB_PORT') or 5432)
    p.db_name = p.db_name or os.getenv('DB_NAME')
    p.db_user = p.db_user or os.getenv('DB_USER')
    p.db_password = p.db_password or os.getenv('DB_PASSWORD')
    p.sslmode = p.sslmode or os.getenv('DB_SSLMODE') or 'require'
    missing = [k for k in ['db_host', 'db_port', 'db_name', 'db_user', 'db_password'] if getattr(p, k) in (None, '')]
    if missing:
        print("Faltan parámetros de conexión: " + ", ".join(missing), file=sys.stderr)
        sys.exit(1)
    return p


def connect(p):
    mode = (p.sslmode or 'require').lower()
    ssl_ctx = None
    if mode != 'disable':
        ssl_ctx = ssl.create_default_context()
        if mode in ('require', 'prefer', 'allow'):
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        elif mode == 'verify-ca':
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        elif mode == 'verify-full':
            ssl_ctx.check_hostname = True
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        else:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
    conn = pg.connect(user=p.db_user, password=p.db_password, host=p.db_host, port=p.db_port, database=p.db_name, ssl_context=ssl_ctx)
    try:
        cur = conn.cursor()
        try:
            if not getattr(p, 'write', False):
                cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            cur.execute("SET statement_timeout TO 30000")
        finally:
            cur.close()
    except Exception:
        # En modo pooler (pgbouncer transaction pooling) puede fallar; continuar sin SET de sesión
        pass
    return conn


def fetch_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def list_schemas(conn, only=None):
    q = """
    SELECT nspname AS schema_name
    FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
      AND nspname NOT LIKE 'pg_toast_temp_%'
      AND nspname NOT LIKE 'pg_temp_%'
      AND nspname = COALESCE(%s::name, nspname)
    ORDER BY nspname
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (only,))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()


def list_tables(conn, schema):
    q = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema,))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()


def list_views(conn, schema):
    q = """
    SELECT table_name
    FROM information_schema.views
    WHERE table_schema = %s
    ORDER BY table_name
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema,))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()


def list_columns(conn, schema, table):
    q = """
    SELECT column_name, data_type, is_nullable, column_default, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, table))
        rows = fetch_dicts(cur)
        return rows
    finally:
        cur.close()


def list_pk_columns(conn, schema, table):
    q = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
      AND tc.table_name = kcu.table_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %s
      AND tc.table_name = %s
    ORDER BY kcu.ordinal_position
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, table))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()


def list_fks(conn, schema, table):
    q = """
    SELECT
        tc.constraint_name,
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        rc.update_rule,
        rc.delete_rule
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
      AND tc.table_name = kcu.table_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    JOIN information_schema.referential_constraints AS rc
      ON rc.constraint_name = tc.constraint_name
      AND rc.constraint_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = %s
      AND tc.table_name = %s
    ORDER BY tc.constraint_name, kcu.ordinal_position
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, table))
        return fetch_dicts(cur)
    finally:
        cur.close()


def list_indexes(conn, schema, table):
    q = """
    SELECT
        i.relname AS index_name,
        idx.indisunique AS is_unique,
        idx.indisprimary AS is_primary,
        array_to_string(ARRAY(
          SELECT pg_get_indexdef(idx.indexrelid, k, TRUE)
          FROM generate_subscripts(idx.indkey::smallint[], 1) AS k
          ORDER BY k
        ), ', ') AS index_columns
    FROM pg_index idx
    JOIN pg_class i ON i.oid = idx.indexrelid
    JOIN pg_class t ON t.oid = idx.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = %s AND t.relname = %s
    ORDER BY index_name
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, table))
        return fetch_dicts(cur)
    finally:
        cur.close()


def list_triggers(conn, schema, table):
    q = """
    select
      tg.tgname as trigger_name,
      case when tg.tgenabled = 'O' then 'ENABLED' else tg.tgenabled end as enabled,
      nsf.nspname as function_schema,
      p.proname as function_name,
      pg_get_triggerdef(tg.oid, true) as trigger_def
    from pg_trigger tg
    join pg_class c on c.oid = tg.tgrelid
    join pg_namespace ns on ns.oid = c.relnamespace
    join pg_proc p on p.oid = tg.tgfoid
    join pg_namespace nsf on nsf.oid = p.pronamespace
    where ns.nspname = %s and c.relname = %s
      and not tg.tgisinternal
    order by tg.tgname
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, table))
        return fetch_dicts(cur)
    finally:
        cur.close()


def list_trigger_function_defs(conn, schema, func_name):
    q = """
    SELECT ns.nspname AS function_schema,
           p.proname  AS function_name,
           pg_get_functiondef(p.oid) AS function_def
    FROM pg_proc p
    JOIN pg_namespace ns ON ns.oid = p.pronamespace
    WHERE ns.nspname = %s AND p.proname = %s
    ORDER BY 1,2
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, func_name))
        return fetch_dicts(cur)
    finally:
        cur.close()


def estimate_rows(conn, schema, table):
    q = """
    SELECT reltuples::bigint
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(q, (schema, table))
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else None
    finally:
        cur.close()


def insert_sensor_valor(conn, schema, id_fundo, id_sensorlocalizacion, id_metrica, valor, fecha):
    q = f"INSERT INTO {schema}.sensor_valor (id_fundo, id_sensorlocalizacion, id_metrica, valor, fecha) VALUES (%s, %s, %s, %s, %s)"
    cur = conn.cursor()
    try:
        cur.execute(q, (id_fundo, id_sensorlocalizacion, id_metrica, valor, fecha))
        conn.commit()
    finally:
        cur.close()


def verify_sensor_valor(conn, schema, id_fundo, id_sensorlocalizacion, id_metrica, fecha):
    cur = conn.cursor()
    try:
        cur.execute(
            f"select id_fundo,id_sensorlocalizacion,id_metrica,valor,fecha,statusid from {schema}.sensor_valor where id_fundo=%s and id_sensorlocalizacion=%s and id_metrica=%s and fecha=%s",
            (id_fundo, id_sensorlocalizacion, id_metrica, fecha),
        )
        sv = cur.fetchall()
        print("sensor_valor:", sv)

        md = []
        try:
            med_cols = {c["column_name"] for c in list_columns(conn, schema, "medicion")}
            if {"sensorid", "metricaid"}.issubset(med_cols):
                cur.execute(
                    f"select localizacionsensorid,sensorid,metricaid,fecha,valor from {schema}.medicion where localizacionsensorid=%s and metricaid=%s and fecha=%s",
                    (id_sensorlocalizacion, id_metrica, fecha),
                )
            else:
                cur.execute(
                    f"select localizacionsensorid,fecha,valor from {schema}.medicion where localizacionsensorid=%s and fecha=%s",
                    (id_sensorlocalizacion, fecha),
                )
            md = cur.fetchall()
        except Exception:
            md = []
        print("medicion:", md)

        cur.execute(
            f"select error,id_fundo,id_sensorlocalizacion,id_metrica,valor,fecha,statusid from {schema}.sensor_valor_error where id_fundo=%s and id_sensorlocalizacion=%s and id_metrica=%s and fecha=%s",
            (id_fundo, id_sensorlocalizacion, id_metrica, fecha),
        )
        sve = cur.fetchall()
        print("sensor_valor_error:", sve)
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host")
    parser.add_argument("--db-port", type=int)
    parser.add_argument("--db-name")
    parser.add_argument("--db-user")
    parser.add_argument("--db-password")
    parser.add_argument("--sslmode")
    parser.add_argument("--schema")
    parser.add_argument("--detail", action="store_true")
    parser.add_argument("--tables", help="Comma-separated table names to include", default=None)
    parser.add_argument("--show-triggers", action="store_true")
    parser.add_argument("--show-trigger-funcs", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--insert-sensor-valor", action="store_true")
    parser.add_argument("--id-fundo", type=int)
    parser.add_argument("--id-sensorlocalizacion", type=int)
    parser.add_argument("--id-metrica", type=int)
    parser.add_argument("--valor", type=float)
    parser.add_argument("--fecha")
    parser.add_argument("--verify-sensor-valor", action="store_true")
    args = get_arg_or_env(parser)
    try:
        conn = connect(args)
    except Exception as e:
        print(f"Error de conexión: {e}", file=sys.stderr)
        sys.exit(2)
    try:
        if args.insert_sensor_valor:
            schema = args.schema or 'thermo'
            if args.id_fundo is None or args.id_sensorlocalizacion is None or args.id_metrica is None or args.valor is None:
                print("Faltan parámetros para inserción", file=sys.stderr)
                sys.exit(4)
            if args.fecha:
                s = args.fecha
                if s.endswith('Z'):
                    s = s[:-1] + '+00:00'
                try:
                    fecha = datetime.fromisoformat(s)
                except Exception:
                    print("Fecha inválida; use ISO 8601", file=sys.stderr)
                    sys.exit(4)
            else:
                fecha = datetime.now(timezone.utc)
            insert_sensor_valor(conn, schema, args.id_fundo, args.id_sensorlocalizacion, args.id_metrica, args.valor, fecha)
            print(f"Insertado en {schema}.sensor_valor: id_fundo={args.id_fundo}, id_sensorlocalizacion={args.id_sensorlocalizacion}, id_metrica={args.id_metrica}, valor={args.valor}, fecha={fecha.isoformat()}")
            conn.close()
            sys.exit(0)
        if args.verify_sensor_valor:
            schema = args.schema or 'thermo'
            if args.id_fundo is None or args.id_sensorlocalizacion is None or args.id_metrica is None or args.fecha is None:
                print("Faltan parámetros para verificación", file=sys.stderr)
                sys.exit(4)
            s = args.fecha
            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            try:
                fecha = datetime.fromisoformat(s)
            except Exception:
                print("Fecha inválida; use ISO 8601", file=sys.stderr)
                sys.exit(4)
            verify_sensor_valor(conn, schema, args.id_fundo, args.id_sensorlocalizacion, args.id_metrica, fecha)
            conn.close()
            sys.exit(0)
        schemas = list_schemas(conn, args.schema)
        if not schemas:
            print("No se encontraron esquemas.")
            sys.exit(0)
        for s in schemas:
            tables = list_tables(conn, s)
            views = list_views(conn, s)
            print(f"Schema: {s}")
            print(f"  Tables: {len(tables)}")
            print(f"  Views: {len(views)}")
            if args.detail:
                if views:
                    print("  Views list:")
                    for v in views:
                        print(f"    - {v}")
            wanted = None
            if args.tables:
                wanted = {x.strip() for x in args.tables.split(',') if x.strip()}
            for t in tables:
                if wanted and t not in wanted:
                    continue
                cols = list_columns(conn, s, t)
                pk = list_pk_columns(conn, s, t)
                fks = list_fks(conn, s, t)
                idx = list_indexes(conn, s, t)
                trigs = list_triggers(conn, s, t) if args.show_triggers else []
                est = estimate_rows(conn, s, t)
                pk_str = "(" + ", ".join(pk) + ")" if pk else "-"
                est_str = str(est) if est is not None else "N/A"
                print(f"  - {t}: columns={len(cols)}, pk={pk_str}, fks={len(fks)}, indexes={len(idx)}, est_rows={est_str}")
                if args.detail:
                    print("      Columns:")
                    for c in cols:
                        dv = c["column_default"] if c["column_default"] is not None else ""
                        print(f"        {c['ordinal_position']:>2}. {c['column_name']} {c['data_type']} null={c['is_nullable']} default={dv}")
                    if pk:
                        print("      Primary key:")
                        print(f"        {', '.join(pk)}")
                    if fks:
                        print("      Foreign keys:")
                        for fk in fks:
                            print(f"        {fk['constraint_name']}: {fk['column_name']} -> {fk['foreign_table_schema']}.{fk['foreign_table_name']}({fk['foreign_column_name']}) on update {fk['update_rule']} on delete {fk['delete_rule']}")
                    if idx:
                        print("      Indexes:")
                        for ix in idx:
                            u = "true" if ix["is_unique"] else "false"
                            p = "true" if ix["is_primary"] else "false"
                            cols_s = ix["index_columns"] or ""
                            print(f"        {ix['index_name']}: unique={u}, primary={p}, cols={cols_s}")
                    if args.show_triggers and trigs:
                        print("      Triggers:")
                        for tg in trigs:
                            print(f"        {tg['trigger_name']} [{tg['enabled']}]: {tg['function_schema']}.{tg['function_name']}")
                            print(f"          {tg['trigger_def']}")
                        if args.show_trigger_funcs:
                            printed = set()
                            print("      Trigger functions:")
                            for tg in trigs:
                                key = (tg['function_schema'], tg['function_name'])
                                if key in printed:
                                    continue
                                printed.add(key)
                                defs = list_trigger_function_defs(conn, tg['function_schema'], tg['function_name'])
                                for d in defs:
                                    print(f"        {d['function_schema']}.{d['function_name']}:")
                                    for line in (d['function_def'] or '').splitlines():
                                        print(f"          {line}")
        conn.close()
    except Exception as e:
        print(f"Error durante el análisis: {e}", file=sys.stderr)
        try:
            conn.close()
        except Exception:
            pass
        sys.exit(3)


if __name__ == "__main__":
    main()
