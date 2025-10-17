"""
Configuration management for DB connections, batch size, table selection.
"""
import json
import os
import sqlalchemy
import urllib.parse

class AppConfig:
    def __init__(self, config_path=None, log_callback=None):
        # Always use database.config for all config operations
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'database.config')
        self.log_callback = log_callback  # UI log callback
        self.sqlserver = {
            'server': '', 'db': '', 'user': '', 'pwd': '', 'driver': 'ODBC Driver 17 for SQL Server', 'win_auth': False
        }
        self.postgres = {
            'server': '', 'db': '', 'user': '', 'pwd': '', 'port': '5432'
        }
        self.tables = []
        self.batch_size = 1000
        self.output_type = 'CSV'
        self.output_path = './mismatches'
        # schema_map: [{"sql": schema, "pg": schema}, ...] (multiple mappings)
        self.schema_map = []
        self.load()

    def log(self, msg, success=True):
        if self.log_callback:
            self.log_callback(msg, success)
        else:
            print(msg)

    def load(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                data = json.load(f)
                self.sqlserver.update(data.get('sqlserver', {}))
                self.postgres.update(data.get('postgres', {}))
                self.tables = data.get('tables', [])
                self.batch_size = data.get('batch_size', 1000)
                self.output_type = data.get('output_type', 'CSV')
                self.output_path = data.get('output_path', './mismatches')
                # Always fill schema_map from database.config
                self.schema_map = data.get('schema_map', [])

    def save(self, log_tables=False):
        data = {
            'sqlserver': self.sqlserver,
            'postgres': self.postgres,
            'tables': self.tables,
            'batch_size': self.batch_size,
            'output_type': self.output_type,
            'output_path': self.output_path,
            # schema_map: [{"sql": schema, "pg": schema}, ...] (multiple mappings)
            'schema_map': self.schema_map
        }
        with open(self.config_path, 'w') as f:
            json.dump(data, f, indent=2)
        if log_tables:
            self.log(f"[INFO] Tables saved to config: {self.tables}", success=True)

    def build_sql_conn_str(self):
        # Build SQL Server connection string for SQLAlchemy (pyodbc)
        driver = self.sqlserver['driver'] or 'ODBC Driver 17 for SQL Server'
        driver_enc = urllib.parse.quote_plus(driver)
        if self.sqlserver['win_auth']:
            # Windows Auth
            return (
                f"mssql+pyodbc://@{self.sqlserver['server']}/{self.sqlserver['db']}?driver={driver_enc}&trusted_connection=yes"
            )
        else:
            # SQL Auth
            return (
                f"mssql+pyodbc://{self.sqlserver['user']}:{self.sqlserver['pwd']}@{self.sqlserver['server']}/{self.sqlserver['db']}?driver={driver_enc}"
            )

    def build_pg_conn_str(self):
        # Build PostgreSQL connection string for SQLAlchemy (psycopg2)
        import urllib.parse
        host = self.postgres['server']
        if '@' in host:
            host = host.split('@')[-1].strip()
        port = self.postgres.get('port') or '5432'
        try:
            int(port)
        except (TypeError, ValueError):
            port = '5432'
        user = self.postgres['user']
        pwd = self.postgres['pwd']
        db = self.postgres['db']
        # URL-encode user and password to handle special characters
        user_enc = urllib.parse.quote_plus(user)
        pwd_enc = urllib.parse.quote_plus(pwd)
        self.log(f"[DEBUG] build_pg_conn_str: user='{user_enc}', pwd='{pwd_enc}', host='{host}', port='{port}', db='{db}'", success=True)
        if '@' in user:
            user_enc = urllib.parse.quote_plus(user.split('@')[0].strip())
        return (
            f"postgresql+psycopg2://{user_enc}:{pwd_enc}@{host}:{port}/{db}"
        )

    def test_sql_connection(self):
        try:
            engine = sqlalchemy.create_engine(self.build_sql_conn_str())
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text('SELECT 1'))
            # Only log once per test, not on every call
            return True, 'SQL Server connection successful.'
        except Exception as e:
            return False, f'SQL Server connection error: {e}'

    def test_pg_connection(self):
        host = self.postgres['server']
        if '@' in host:
            host = host.split('@')[-1]
            self.postgres['server'] = host
            self.save()
        try:
            engine = sqlalchemy.create_engine(self.build_pg_conn_str())
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text('SELECT 1'))
            # Only log once per test, not on every call
            return True, 'PostgreSQL connection successful.'
        except Exception as e:
            return False, f'PostgreSQL connection error: {e}'

    def get_schema_map(self):
        """
        Return the schema_map loaded from database.config
        """
        return self.schema_map
