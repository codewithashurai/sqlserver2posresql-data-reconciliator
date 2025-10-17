"""
Backend validation engine: batch fetch, compare, threaded, CSV/DB output.
"""
import os
import json
import logging
import threading
import hashlib
import datetime
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import sys

class UILogHandler(logging.Handler):
    """
    Custom logging handler to forward all log messages to the UI log callback.
    """
    def __init__(self, ui_log_callback):
        super().__init__()
        self.ui_log_callback = ui_log_callback
    def emit(self, record):
        msg = self.format(record)
        level = record.levelname
        # Map log level to color and success
        success = level in ('INFO', 'DEBUG')
        self.ui_log_callback(f"[{level}] {msg}", success, level)

class ValidationEngine:
    def __init__(self, sql_conn_str, pg_conn_str, config_path='config.json', schema_map=None, ui_log_callback=None):
        self.sql_conn_str = sql_conn_str
        self.pg_conn_str = pg_conn_str
        print(f"[DEBUG] ValidationEngine config_path used: {os.path.abspath(config_path)}")
        self.config = self._load_config(config_path)
        self.batch_size = self.config.get('batch_size', 10000)
        self.output_mode = self.config.get('output_mode', 'CSV').upper()
        config_output_path = self.config.get('output_path', None)
        if not config_output_path or str(config_output_path).strip() == '':
            print('[ERROR] output_path is missing or empty in config. Please set a valid output_path.')
            raise ValueError('output_path is missing or empty in config. Process aborted.')
        # Strictly resolve output_path
        if os.path.isabs(config_output_path) or (':' in str(config_output_path)[:3]):
            resolved_output_path = os.path.normpath(config_output_path)
        else:
            config_dir = os.path.dirname(os.path.abspath(config_path))
            resolved_output_path = os.path.normpath(os.path.join(config_dir, config_output_path))
        self.output_path = resolved_output_path
        print(f"[DEBUG] ValidationEngine output_path resolved to: {self.output_path}")
        self.logs_dir = os.path.join(self.output_path, 'Logs')
        self.reports_dir = os.path.join(self.output_path, 'ValidationReports')
        try:
            os.makedirs(self.output_path, exist_ok=True)
            os.makedirs(self.logs_dir, exist_ok=True)
            os.makedirs(self.reports_dir, exist_ok=True)
        except Exception as e:
            print(f"[ERROR] Could not create output folders: {e}")
            print(f"[ERROR] Config contents: {json.dumps(self.config, indent=2)}")
        self.logger = self._setup_logger(ui_log_callback)
        self.sql_engine = create_engine(self.sql_conn_str)
        self.pg_engine = create_engine(self.pg_conn_str)
        # Use schema_map from argument if provided, else from config
        self.schema_map = schema_map if schema_map is not None else self.config.get('schema_map', [])
        self.summary = {
            'tables_validated': 0,
            'total_rows_compared': 0,
            'mismatched_rows': 0,
            'duration': None,
            'output': self.output_mode,
            'output_path': self.output_path
        }
        self.progress = {}

    def _load_config(self, path):
        if os.path.exists(path):
            with open(path, 'r') as f:
                config = json.load(f)
                # Prepopulate output_path and log_path if missing
                if 'output_path' not in config:
                    config['output_path'] = './ValidationReports'
                if 'log_path' not in config:
                    config['log_path'] = './Logs'
                return config
        # Defaults if config missing
        return {'output_path': './ValidationReports', 'log_path': './Logs'}

    def _setup_logger(self, ui_log_callback=None):
        logger = logging.getLogger('ValidationEngine')
        logger.setLevel(logging.INFO)
        # Save logs in the output_path/Logs folder
        log_file_path = os.path.join(self.logs_dir if hasattr(self, 'logs_dir') else './Logs', 'validation_engine.log')
        handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=3)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        # Add UI log handler if callback provided
        if ui_log_callback:
            ui_handler = UILogHandler(ui_log_callback)
            ui_handler.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(ui_handler)
        return logger

    def _get_output_dirs(self):
        # Always use config values for output and log paths
        output_dir = self.reports_dir if hasattr(self, 'reports_dir') else self.config.get('output_path', './ValidationReports')
        log_dir = self.logs_dir if hasattr(self, 'logs_dir') else self.config.get('log_path', './Logs')
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        return output_dir, log_dir

    def setup_file_logger(self):
        """
        Set up file logger for all details in custom Logs folder from config.
        """
        output_dir, log_dir = self._get_output_dirs()
        import re
        server = re.search(r'host=([^;]+)', self.pg_conn_str)
        db = re.search(r'db(?:name)?=([^;]+)', self.pg_conn_str)
        server_name = server.group(1) if server else 'pgserver'
        db_name = db.group(1) if db else 'pgdb'
        output_format = self.output_mode
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f"{server_name}_{db_name}_{output_format}_{timestamp}.log")
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.info(f"File logger setup: {log_file}")

    def get_tables_and_pk(self, engine, schema=None):
        inspector = inspect(engine)
        # Use schema mapping from config if available
        schema_map = self.config.get('schema_map', [])
        tables = []
        pk_map = {}
        # Get all tables for each schema in schema_map
        schemas = [m['sql'] for m in schema_map if 'sql' in m]
        for sch in schemas:
            for table in inspector.get_table_names(schema=sch):
                tables.append(table)
                pk_cols = inspector.get_pk_constraint(table, schema=sch)['constrained_columns']
                pk_map[table] = pk_cols
        # If no schema_map, fallback to default
        if not tables:
            for table in inspector.get_table_names():
                tables.append(table)
                pk_cols = inspector.get_pk_constraint(table)['constrained_columns']
                pk_map[table] = pk_cols
        return tables, pk_map

    def _get_schema_for_table(self, table, dbtype):
        # dbtype: 'sql' or 'pg'
        schema_map = self.config.get('schema_map', [])
        # Default to first mapping if not found
        for mapping in schema_map:
            # If table is in the selected tables, use mapping
            # (Assume all tables in a schema use the same mapping)
            # You can extend this logic for per-table mapping if needed
            if dbtype in mapping:
                return mapping[dbtype]
        # Fallback: use first mapping or None
        if schema_map and dbtype in schema_map[0]:
            return schema_map[0][dbtype]
        return None

    def _get_schema_table_name(self, table, dbtype):
        schema = self._get_schema_for_table(table, dbtype)
        if dbtype == 'sql':
            if schema:
                return f"[{schema}].[{table}]"
            else:
                return f"[{table}]"
        else:
            if schema:
                return f'"{schema}"."{table}"'
            else:
                return f'"{table}"'

    def _parse_schema_table(self, tbl):
        """
        Parse a schema-qualified table name (schema.table, [schema].[table], etc.) and return (schema, table).
        Handles edge cases and strips whitespace and brackets.
        """
        # Remove brackets and quotes
        tbl = tbl.replace('[', '').replace(']', '').replace('"', '').strip()
        if '.' in tbl:
            parts = tbl.split('.', 1)
            schema = parts[0].strip()
            table = parts[1].strip()
        else:
            schema = None
            table = tbl.strip()
        return schema, table

    def _get_sql_table_name(self, tbl):
        """
        Return SQL Server table name in [schema].[table] format, or [table] if no schema.
        Handles cases where tbl is already schema-qualified or not.
        """
        schema, table = self._parse_schema_table(tbl)
        # Always wrap both schema and table in brackets if schema is present
        if schema:
            return f"[{schema}].[{table}]"
        else:
            return f"[{table}]"

    def _pg_table_name(self, tbl):
        """
        Return PostgreSQL table name in schema.table format, all lowercase, unquoted.
        """
        schema, table = self._parse_schema_table(tbl)
        schema = (schema or 'public').lower()
        table = table.lower()
        return f"{schema}.{table}"

    def fetch_batch(self, engine, tbl, pk_cols, offset, batch_size):
        table_name = self._get_sql_table_name(tbl)
        if pk_cols:
            pk_order = ','.join(pk_cols)
            query = f"SELECT * FROM {table_name} ORDER BY {pk_order} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        else:
            # No PK: use ORDER BY (SELECT NULL) to allow OFFSET/FETCH
            query = f"SELECT * FROM {table_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        try:
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            self.logger.error(f"Error fetching batch from {table_name}: {e}")
            return pd.DataFrame()

    def fetch_pg_batch(self, engine, tbl, pk_cols, offset, batch_size):
        table_name = self._pg_table_name(tbl)
        if pk_cols:
            pk_order = ','.join(pk_cols)
            query = f"SELECT * FROM {table_name} ORDER BY {pk_order} OFFSET {offset} LIMIT {batch_size}"
        else:
            # No PK: omit ORDER BY, use OFFSET/LIMIT for batching
            query = f"SELECT * FROM {table_name} OFFSET {offset} LIMIT {batch_size}"
        try:
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            self.logger.error(f"Error fetching batch from {table_name}: {e}")
            return pd.DataFrame()

    def _normalize_value_for_pg(self, value):
        """Normalize value to canonical PostgreSQL type for robust comparison."""
        if value is None:
            return None
        if isinstance(value, bool):
            return bool(value)
        if isinstance(value, (int, float)):
            return float(value) if isinstance(value, float) else int(value)
        if isinstance(value, bytes):
            try:
                value = value.decode('utf-8')
            except Exception:
                value = str(value)
        if isinstance(value, str):
            return value.strip().lower()
        return str(value).strip().lower()

    def _row_hash(self, row, columns):
        """Hash a row after normalizing all values and sorting columns."""
        norm_row = tuple(self._normalize_value_for_pg(row[col]) for col in sorted(columns))
        return hash(norm_row)

    def hash_row(self, row, columns=None):
        """Alias for _row_hash to support legacy/internal calls."""
        if columns is None:
            columns = row.index if hasattr(row, 'index') else row.keys()
        return self._row_hash(row, columns)

    def validate_table_no_pk(self, sql_rows, pg_rows, columns):
        """Robustly compare rows for tables without PK using normalized hashes."""
        sql_hashes = set(self._row_hash(row, columns) for row in sql_rows)
        pg_hashes = set(self._row_hash(row, columns) for row in pg_rows)
        mismatches = sql_hashes.symmetric_difference(pg_hashes)
        return len(mismatches), mismatches

    def run_all(self, tables, progress_callback=None, ui_log_callback=None):
        self.setup_output_tables()
        self.setup_file_logger()
        start = datetime.datetime.now()
        sql_tables, pk_map = self.get_tables_and_pk(self.sql_engine)
        if tables is None:
            tables = sql_tables
        valid_tables = self.get_valid_tables(tables, ui_log_callback)
        # Estimate total rows for progress bar
        total_rows_estimated = self.estimate_total_rows(valid_tables, ui_log_callback)
        self.summary['total_rows_estimated'] = total_rows_estimated
        results = []
        processed_tables = 0
        processed_rows = 0
        # --- Prepare output folder in custom app directory ---
        output_dir, _ = self._get_output_dirs()
        if not valid_tables:
            self.logger.error("No valid tables found in both SQL Server and PostgreSQL.")
            if ui_log_callback:
                self._ui_log("No valid tables found in both SQL Server and PostgreSQL.", ui_log_callback, False, 'ERROR')
            self.summary['duration'] = str(datetime.datetime.now() - start)
            return self.summary
        for table in valid_tables:
            # Prepare CSV for this table
            table_csv_path = os.path.join(output_dir, f"{table}.csv")
            write_header = not os.path.exists(table_csv_path) or os.path.getsize(table_csv_path) == 0
            table_csv_file = open(table_csv_path, 'a', newline='', encoding='utf-8')
            import csv
            table_csv_writer = csv.writer(table_csv_file)
            if write_header:
                table_csv_writer.writerow(['TableName', 'SqlPKId', 'SqlColumnValue', 'PgColumnValue', 'Timestamp', 'Status'])
            count, mismatches, processed_rows = self.validate_table(
                table, progress_callback, processed_tables, processed_rows,
                csv_writer=table_csv_writer
            )
            table_csv_file.close()
            processed_tables += 1
            # Minimal UI log: only table summary
            if ui_log_callback:
                summary_msg = f"Table: {table} | Rows: {count} | Mismatched: {mismatches} | Status: {'✅' if mismatches == 0 else '❌'}"
                self._ui_log(summary_msg, ui_log_callback, mismatches == 0, 'INFO' if mismatches == 0 else 'WARNING')
            results.append((table, count, mismatches))
        self.summary['duration'] = str(datetime.datetime.now() - start)
        # Final UI log summary
        if ui_log_callback:
            final_msg = f"Total Tables: {len(valid_tables)} | Total Rows Processed: {processed_rows} | Total Mismatched: {self.summary['mismatched_rows']} | Duration: {self.summary['duration']}"
            self._ui_log(final_msg, ui_log_callback, True, 'INFO')
        return self.summary

    def log_cmd_invocation(self):
        """
        Log the command to run this validation engine with all current parameter values.
        """
        cmd = (
            f"python validation_engine.py --config '{self.config.get('config_path', 'config.json')}' "
            f"--sql '{self.sql_conn_str}' --pg '{self.pg_conn_str}'"
        )
        self.logger.debug(f"[CMD] To run validation engine: {cmd}")
        print(f"[CMD] To run validation engine: {cmd}")

    def _ui_log(self, msg, ui_log_callback=None, success=True, level='INFO'):
        """
        Helper to format and send log messages to the UI log callback.
        """
        formatted = f"[{level}] {msg}"
        if ui_log_callback:
            ui_log_callback(formatted, success, level)
        else:
            print(formatted)

    def setup_output_tables(self):
        """
        Ensure datareconciliator_summary and datareconciliator_details exist in PG, truncate if exist.
        """
        create_summary = '''
        CREATE TABLE IF NOT EXISTS datareconciliator_summary (
            pk_summary_id SERIAL PRIMARY KEY,
            table_name TEXT,
            total_rows INT,
            mismatched INT,
            validation_timestamp TIMESTAMP,
            status TEXT
        );'''
        create_details = '''
        CREATE TABLE IF NOT EXISTS datareconciliator_details (
            pk_detail_id SERIAL PRIMARY KEY,
            fk_summary_id INT,
            table_name TEXT,
            sqlpkid TEXT,
            sqlcolumnvalue TEXT,
            pgcolumnvalue TEXT,
            validation_timestamp TIMESTAMP,
            status TEXT
        );'''
        with self.pg_engine.begin() as conn:
            conn.execute(text(create_summary))
            conn.execute(text(create_details))
            conn.execute(text('TRUNCATE TABLE datareconciliator_summary;'))
            conn.execute(text('TRUNCATE TABLE datareconciliator_details;'))
        self.logger.info('Output tables ensured and truncated.')

    def write_summary_db(self, table_name, total_rows, mismatched, status):
        """
        Write table-wise summary to datareconciliator_summary.
        """
        insert_sql = text('''
            INSERT INTO datareconciliator_summary (table_name, total_rows, mismatched, validation_timestamp, status)
            VALUES (:table_name, :total_rows, :mismatched, :validation_timestamp, :status)
            RETURNING pk_summary_id
        ''')
        with self.pg_engine.begin() as conn:
            result = conn.execute(insert_sql, {
                'table_name': table_name,
                'total_rows': total_rows,
                'mismatched': mismatched,
                'validation_timestamp': datetime.datetime.now(),
                'status': status
            })
            pk_summary_id = result.scalar() if result else None
        return pk_summary_id

    def write_details_db(self, fk_summary_id, table_name, sqlpkid, sqlcolumnvalue, pgcolumnvalue, status):
        """
        Write row-level details to datareconciliator_details.
        """
        insert_sql = text('''
            INSERT INTO datareconciliator_details (fk_summary_id, table_name, sqlpkid, sqlcolumnvalue, pgcolumnvalue, validation_timestamp, status)
            VALUES (:fk_summary_id, :table_name, :sqlpkid, :sqlcolumnvalue, :pgcolumnvalue, :validation_timestamp, :status)
        ''')
        with self.pg_engine.begin() as conn:
            conn.execute(insert_sql, {
                'fk_summary_id': fk_summary_id,
                'table_name': table_name,
                'sqlpkid': sqlpkid,
                'sqlcolumnvalue': sqlcolumnvalue,
                'pgcolumnvalue': pgcolumnvalue,
                'validation_timestamp': datetime.datetime.now(),
                'status': status
            })

    def write_details_csv(self, table_name, sqlpkid, sqlcolumnvalue, pgcolumnvalue, status, csv_writer=None):
        """
        Write row-level details to a single CSV file (append mode) using provided csv_writer.
        """
        if csv_writer is None:
            # Fallback: do nothing if no writer provided
            return
        csv_writer.writerow([
            table_name,
            sqlpkid,
            sqlcolumnvalue,
            pgcolumnvalue,
            datetime.datetime.now().isoformat(),
            status
        ])

    def _get_row_count(self, engine, tbl, dbtype):
        """
        Return row count for a table from SQL Server or PostgreSQL.
        dbtype: 'sql' or 'pg'
        """
        schema, table = self._parse_schema_table(tbl)
        if dbtype == 'sql':
            table_name = self._get_sql_table_name(tbl)
        else:
            table_name = self._pg_table_name(tbl)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar() if result else 0
            return count
        except Exception as e:
            self.logger.error(f"Error getting row count for {dbtype} table {tbl}: {e}")
            return 0

    def get_valid_tables(self, tables, ui_log_callback=None):
        """Return tables that exist in both SQL Server and PostgreSQL."""
        valid_tables = []
        sql_tables, _ = self.get_tables_and_pk(self.sql_engine)
        pg_tables, _ = self.get_tables_and_pk(self.pg_engine)
        sql_set = set([t.lower() for t in sql_tables])
        pg_set = set([t.lower() for t in pg_tables])
        for tbl in tables:
            schema, table = self._parse_schema_table(tbl)
            tbl_name = table.lower()
            if tbl_name in sql_set and tbl_name in pg_set:
                valid_tables.append(tbl)
            else:
                if ui_log_callback:
                    self._ui_log(f"Table '{tbl}' not found in both SQL Server and PostgreSQL.", ui_log_callback, False, 'WARNING')
        return valid_tables

    def estimate_total_rows(self, tables, ui_log_callback=None):
        """Estimate total rows for progress bar by summing SQL row counts for all tables."""
        total = 0
        for tbl in tables:
            count = self._get_row_count(self.sql_engine, tbl, 'sql')
            total += count
            if ui_log_callback:
                self._ui_log(f"Estimated rows for table '{tbl}': {count}", ui_log_callback, True, 'DEBUG')
        return total

    def _find_pk_cols_case_insensitive(self, df, pk_cols):
        """Return actual DataFrame column names matching pk_cols, case-insensitive."""
        df_cols = [col.lower() for col in df.columns]
        result = []
        for pk in pk_cols:
            pk_lower = pk.lower()
            for i, col in enumerate(df.columns):
                if col.lower() == pk_lower:
                    result.append(col)
                    break
        return result if result else pk_cols

    # SQL to PG datatype compatibility mapping
    _datatype_compat_map = {
        'varchar': ['character varying', 'varchar'],
        'nvarchar': ['character varying', 'nvarchar'],
        'int': ['integer', 'int', 'int4'],
        'bigint': ['bigint', 'int8'],
        'smallint': ['smallint', 'int2'],
        'decimal': ['decimal', 'numeric'],
        'float': ['float', 'float8', 'double precision'],
        'bit': ['boolean', 'bit'],
        'datetime': ['timestamp', 'datetime'],
        'date': ['date'],
        'text': ['text'],
        'char': ['char', 'character'],
        'uniqueidentifier': ['uuid'],
        # Add more as needed
    }

    def _values_equal(self, val1, val2):
        """Robust equality check for SQL/PG values, allowing datatype compatibility."""
        # Handle nulls and NaN
        def is_null(val):
            if val is None:
                return True
            if isinstance(val, float) and pd.isna(val):
                return True
            if isinstance(val, str) and val.strip().lower() in ('nan', 'none', ''):
                return True
            return False
        if is_null(val1) and is_null(val2):
            return True
        # Datatype compatibility
        v1 = str(val1).strip().lower() if isinstance(val1, str) else val1
        v2 = str(val2).strip().lower() if isinstance(val2, str) else val2
        for sql_type, pg_types in self._datatype_compat_map.items():
            if v1 == sql_type and v2 in pg_types:
                return True
            if v2 == sql_type and v1 in pg_types:
                return True
        # Numeric compatibility
        try:
            if isinstance(val1, (int, float)) or (isinstance(val1, str) and v1.replace('.', '', 1).isdigit()):
                if isinstance(val2, (int, float)) or (isinstance(val2, str) and v2.replace('.', '', 1).isdigit()):
                    return float(val1) == float(val2)
        except Exception:
            pass
        # String compatibility
        if isinstance(val1, str) or isinstance(val2, str):
            return v1 == v2
        # Fallback to normalized comparison
        norm1 = self._normalize_value_for_pg(val1)
        norm2 = self._normalize_value_for_pg(val2)
        return norm1 == norm2

    def validate_table(self, tbl, progress_callback=None, processed_tables=0, processed_rows=0, csv_writer=None):
        # Clear stop_event at the start of each validation run to allow re-execution
        stop_event = getattr(self, 'stop_event', None)
        if stop_event:
            stop_event.clear()
        start = datetime.datetime.now()
        schema, table = self._parse_schema_table(tbl)
        pk_cols = None
        inspector = inspect(self.sql_engine)
        try:
            if schema:
                pk_cols = inspector.get_pk_constraint(table, schema=schema)['constrained_columns']
            else:
                pk_cols = inspector.get_pk_constraint(table)['constrained_columns']
        except Exception as e:
            self.logger.error(f"Error getting PK for table {table} in schema {schema}: {e}")
        pk_exists = bool(pk_cols)
        self.summary.setdefault('pk_status', {})[tbl] = 'Exists' if pk_exists else 'Missing'
        sql_count = self._get_row_count(self.sql_engine, tbl, 'sql')
        pg_count = self._get_row_count(self.pg_engine, tbl, 'pg')
        self.summary.setdefault('row_counts', {})[tbl] = {'sql': sql_count, 'pg': pg_count}
        if sql_count != pg_count:
            self.logger.warning(f"Row count mismatch for table {tbl}: SQL={sql_count}, PG={pg_count}")
        mismatches_total = 0
        missing_total = 0
        extra_total = 0
        offset = 0
        batch_size = self.batch_size
        output_mode = self.output_mode
        pk_summary_id = self.write_summary_db(tbl, sql_count, 0, 'Started')
        stop_event = getattr(self, 'stop_event', None)
        if pk_exists:
            while offset < min(sql_count, pg_count):
                if stop_event and stop_event.is_set():
                    self.logger.info('[DEBUG] Validation cancelled by user inside PK loop. Exiting loop.')
                    break
                sql_df = self.fetch_batch(self.sql_engine, tbl, pk_cols, offset, batch_size)
                pg_df = self.fetch_pg_batch(self.pg_engine, tbl, pk_cols, offset, batch_size)
                if sql_df.empty and pg_df.empty:
                    self.logger.warning(f"Both SQL and PG batches empty for table {tbl} at offset {offset}")
                    break
                sql_pk = self._find_pk_cols_case_insensitive(sql_df, pk_cols)
                pg_pk = self._find_pk_cols_case_insensitive(pg_df, pk_cols)
                # Normalize PK values for both SQL and PG before setting index
                def normalize_pk_index(df, pk_cols):
                    df_index = df[pk_cols].apply(lambda row: tuple(str(row[col]).strip().lower() if row[col] is not None else '' for col in pk_cols), axis=1)
                    df = df.copy()
                    # If single PK column, flatten tuple to value
                    if len(pk_cols) == 1:
                        df.index = [idx[0] for idx in df_index]
                    else:
                        df.index = pd.MultiIndex.from_tuples(df_index)
                    return df
                sql_df = normalize_pk_index(sql_df, sql_pk)
                pg_df = normalize_pk_index(pg_df, pg_pk)
                for pk in sql_df.index:
                    if stop_event and stop_event.is_set():
                        self.logger.info('[DEBUG] Validation cancelled by user inside PK for-row. Exiting loop.')
                        break
                    if pk not in pg_df.index:
                        missing_total += 1
                        sqlpkid = ','.join(pk)
                        if output_mode == 'CSV':
                            self.write_details_csv(tbl, sqlpkid, str(sql_df.loc[pk].to_dict()), '', 'Missing', csv_writer=csv_writer)
                        else:
                            self.write_details_db(pk_summary_id, tbl, sqlpkid, str(sql_df.loc[pk].to_dict()), '', 'Missing')
                        continue
                    sql_row = sql_df.loc[pk]
                    pg_row = pg_df.loc[pk]
                    sql_hash = self.hash_row(sql_row)
                    pg_hash = self.hash_row(pg_row)
                    if sql_hash != pg_hash:
                        for col in sql_df.columns:
                            if stop_event and stop_event.is_set():
                                self.logger.info('[DEBUG] Validation cancelled by user inside PK for-col. Exiting loop.')
                                break
                            sql_val = sql_row[col]
                            if col in pg_row:
                                pg_val = pg_row[col]
                            elif col.lower() in pg_row:
                                pg_val = pg_row[col.lower()]
                            else:
                                pg_val = None
                            if not self._values_equal(sql_val, pg_val):
                                mismatches_total += 1
                                if output_mode == 'CSV':
                                    self.write_details_csv(tbl, sqlpkid, str(sql_val), str(pg_val), 'Mismatch', csv_writer=csv_writer)
                                else:
                                    self.write_details_db(pk_summary_id, tbl, pk, str(sql_val), str(pg_val), 'Mismatch')
                for pk in pg_df.index:
                    if stop_event and stop_event.is_set():
                        self.logger.info('[DEBUG] Validation cancelled by user inside PK for-extra. Exiting loop.')
                        break
                    if pk not in sql_df.index:
                        extra_total += 1
                        sqlpkid = ','.join(pk)
                        if output_mode == 'CSV':
                            self.write_details_csv(tbl, sqlpkid, '', str(pg_df.loc[pk].to_dict()), 'Extra', csv_writer=csv_writer)
                        else:
                            self.write_details_db(pk_summary_id, tbl, sqlpkid, '', str(pg_df.loc[pk].to_dict()), 'Extra')
                offset += batch_size
                processed_rows += len(sql_df)
                if progress_callback:
                    percent = int(100 * processed_rows / max(sql_count, 1))
                    progress_callback(processed_tables, processed_rows, sql_count)
                if hasattr(self, 'stop_event') and self.stop_event.is_set():
                    self.logger.info('[DEBUG] Validation cancelled by user after PK batch. Exiting loop.')
                    break
        else:
            # Robust non-PK row matching with column name normalization
            while offset < max(sql_count, pg_count):
                if stop_event and stop_event.is_set():
                    self.logger.info('[DEBUG] Validation cancelled by user inside no-PK loop. Exiting loop.')
                    break
                sql_df = self.fetch_batch(self.sql_engine, tbl, [], offset, batch_size)
                pg_df = self.fetch_pg_batch(self.pg_engine, tbl, [], offset, batch_size)
                if sql_df.empty and pg_df.empty:
                    self.logger.warning(f"Both SQL and PG batches empty for table {tbl} at offset {offset}")
                    break
                # Normalize column names to lowercase for both SQL and PG
                sql_df.columns = [str(col).lower() for col in sql_df.columns]
                pg_df.columns = [str(col).lower() for col in pg_df.columns]
                # Find intersection of columns
                common_cols = sorted(list(set(sql_df.columns) & set(pg_df.columns)))
                if not common_cols:
                    self.logger.warning(f"No common columns found for table {tbl} at offset {offset}")
                    break
                # Reorder both DataFrames to common columns
                sql_common = sql_df[common_cols]
                pg_common = pg_df[common_cols]
                # Composite key: tuple of all normalized, lowercased, string values for each column
                def normalize_for_key(val):
                    if val is None:
                        return ''
                    if isinstance(val, bytes):
                        try:
                            val = val.decode('utf-8')
                        except Exception:
                            val = str(val)
                    if isinstance(val, (int, float, bool)):
                        val = str(val)
                    return str(val).strip().lower()
                def composite_key(row, columns):
                    return tuple(normalize_for_key(row[col]) for col in columns)
                # Build key maps for SQL and PG
                sql_keys = {composite_key(sql_common.iloc[i], common_cols): sql_common.iloc[i] for i in range(len(sql_common))}
                pg_keys = {composite_key(pg_common.iloc[i], common_cols): pg_common.iloc[i] for i in range(len(pg_common))}
                # Debug: log all composite keys for this batch
                self.logger.info(f"[DEBUG] SQL composite keys for table {tbl} at offset {offset}: {list(sql_keys.keys())}")
                self.logger.info(f"[DEBUG] PG composite keys for table {tbl} at offset {offset}: {list(pg_keys.keys())}")
                # Missing: in SQL, not in PG
                for k in sql_keys:
                    if stop_event and stop_event.is_set():
                        self.logger.info('[DEBUG] Validation cancelled by user inside no-PK for-missing. Exiting loop.')
                        break
                    if k not in pg_keys:
                        missing_total += 1
                        sqlpkid = ','.join(k)
                        if output_mode == 'CSV':
                            self.write_details_csv(tbl, sqlpkid, str(sql_keys[k].to_dict()), '', 'Missing', csv_writer=csv_writer)
                        else:
                            self.write_details_db(pk_summary_id, tbl, sqlpkid, str(sql_keys[k].to_dict()), '', 'Missing')
                # Extra: in PG, not in SQL
                for k in pg_keys:
                    if stop_event and stop_event.is_set():
                        self.logger.info('[DEBUG] Validation cancelled by user inside no-PK for-extra. Exiting loop.')
                        break
                    if k not in sql_keys:
                        extra_total += 1
                        sqlpkid = ','.join(k)
                        if output_mode == 'CSV':
                            self.write_details_csv(tbl, sqlpkid, '', str(pg_keys[k].to_dict()), 'Extra', csv_writer=csv_writer)
                        else:
                            self.write_details_db(pk_summary_id, tbl, sqlpkid, '', str(pg_keys[k].to_dict()), 'Extra')
                # Mismatch: same key, but any column differs
                for k in set(sql_keys.keys()) & set(pg_keys.keys()):
                    sql_row = sql_keys[k]
                    pg_row = pg_keys[k]
                    mismatch_found = False
                    mismatched_cols = []
                    for col in common_cols:
                        sql_val = normalize_for_key(sql_row[col])
                        pg_val = normalize_for_key(pg_row[col])
                        if sql_val != pg_val:
                            mismatch_found = True
                            mismatched_cols.append(col)
                    if mismatch_found:
                        sqlpkid = ','.join(k)
                        if output_mode == 'CSV':
                            self.write_details_csv(tbl, sqlpkid, str(sql_row.to_dict()), str(pg_row.to_dict()), 'Mismatch', csv_writer=csv_writer)
                        else:
                            self.write_details_db(pk_summary_id, tbl, sqlpkid, str(sql_row.to_dict()), str(pg_row.to_dict()), 'Mismatch')
                        mismatches_total += 1
                offset += batch_size
                processed_rows += len(sql_df)
                if progress_callback:
                    percent = int(100 * processed_rows / max(sql_count, 1))
                    progress_callback(processed_tables, processed_rows, sql_count)
                if hasattr(self, 'stop_event') and self.stop_event.is_set():
                    self.logger.info('[DEBUG] Validation cancelled by user after no-PK batch. Exiting loop.')
                    break
        # Update summary
        status = '✅' if missing_total == 0 and extra_total == 0 and mismatches_total == 0 else '❌'
        self.summary['tables_validated'] += 1
        self.summary.setdefault('detail', {})[tbl] = {
            'sql_count': sql_count,
            'pg_count': pg_count,
            'missing': missing_total,
            'extra': extra_total,
            'mismatched': mismatches_total,
            'status': status,
            'pk_status': 'Exists' if pk_exists else 'Missing'
        }
        self.summary['mismatched_rows'] += mismatches_total
        # Update summary table with final counts
        with self.pg_engine.begin() as conn:
            conn.execute(text('''
                UPDATE datareconciliator_summary SET mismatched=:mismatched, validation_timestamp=:ts, status=:status WHERE pk_summary_id=:pkid
            '''), {
                'mismatched': mismatches_total,
                'ts': datetime.datetime.now(),
                'status': status,
                'pkid': pk_summary_id
            })
        self.logger.info(f"Validated table {tbl}: SQL={sql_count}, PG={pg_count}, Missing={missing_total}, Extra={extra_total}, Mismatched={mismatches_total}")
        return sql_count, mismatches_total, processed_rows
