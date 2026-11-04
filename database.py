"""PostgreSQL database operations with Polars for fast bulk loading."""

import io
import logging
import time
from typing import List, Set
from urllib.parse import urlparse

import polars as pl
import psycopg2

logger = logging.getLogger(__name__)

# Tabelas que o modo replace pode TRUNCATE. Lista branca dura: dados RFB apenas.
# NUNCA incluir cnpj_exports / cnpj_export_cursor / ddd_regions — são rastro de
# entrega e seed de região, não snapshot da Receita.
ALLOWED_TRUNCATE = frozenset({
    "estabelecimentos",
    "empresas",
    "cnaes",
    "motivos",
    "municipios",
    "naturezas_juridicas",
    "paises",
    "qualificacoes_socios",
    "top_companies_2m",
})


class Database:
    """PostgreSQL database handler with temp table upsert."""

    def __init__(self, database_url: str, load_replace: bool = False):
        self.database_url = database_url
        self.load_replace = load_replace
        self._pk_cache: dict = {}
        self.conn = None

    def _parse_url(self) -> dict:
        """Parse DATABASE_URL into connection parameters."""
        parsed = urlparse(self.database_url)
        return {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "database": parsed.path[1:],
            "user": parsed.username,
            "password": parsed.password,
        }

    def connect(self):
        """Establish database connection with retry."""
        if self.conn is not None:
            return

        params = self._parse_url()
        for attempt in range(4):
            try:
                self.conn = psycopg2.connect(**params)
                self.conn.autocommit = False
                return
            except psycopg2.OperationalError:
                if attempt == 3:
                    raise
                time.sleep(2**attempt)

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get all processed filenames for a directory."""
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT filename FROM processed_files WHERE directory = %s",
                        (directory,),
                    )
                    return {row[0] for row in cur.fetchall()}
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão em get_processed_files (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
            except Exception:
                return set()
        return set()

    def mark_processed(self, directory: str, filename: str):
        """Mark a file as processed."""
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO processed_files (directory, filename)
                           VALUES (%s, %s)
                           ON CONFLICT (directory, filename) DO NOTHING""",
                        (directory, filename),
                    )
                    self.conn.commit()
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão em mark_processed (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
        raise RuntimeError("Falha persistente de conexão em mark_processed")

    def clear_processed_files(self, directory: str):
        """Clear all processed file records for a directory (for force re-processing)."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_files WHERE directory = %s",
                (directory,),
            )
            self.conn.commit()

    def truncate(self, table_name: str):
        """TRUNCATE de tabela de dados (modo replace — snapshot mensal é estado completo)."""
        if table_name not in ALLOWED_TRUNCATE:
            raise RuntimeError(
                f"TRUNCATE bloqueado: '{table_name}' não está na lista branca de dados RFB "
                f"(ALLOWED_TRUNCATE). Rastro de entrega (cnpj_exports/cnpj_export_cursor) "
                f"e ddd_regions nunca são truncados por carga."
            )
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(f'TRUNCATE TABLE {table_name}')
                self.conn.commit()
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão no TRUNCATE de {table_name} (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
        raise RuntimeError(f"Falha persistente de conexão no TRUNCATE de {table_name}")

    def truncated_in_directory(self, directory: str) -> Set[str]:
        """Tabelas já truncadas para este diretório (controle persistente de retomada).

        Um segundo run do mesmo mês (após queda de conexão/timeout) NÃO pode
        retruncar a tabela: os arquivos já carregados estão em processed_files e
        seriam pulados, deixando a tabela com apenas parte dos dados e status
        'completo'. O registro fica no banco, não no processo.
        """
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(
                        """CREATE TABLE IF NOT EXISTS load_truncated (
                               directory  VARCHAR(50)  NOT NULL,
                               table_name VARCHAR(64)  NOT NULL,
                               truncated_at TIMESTAMP DEFAULT NOW(),
                               PRIMARY KEY (directory, table_name)
                           )"""
                    )
                    self.conn.commit()
                    cur.execute(
                        "SELECT table_name FROM load_truncated WHERE directory = %s",
                        (directory,),
                    )
                    return {row[0] for row in cur.fetchall()}
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão em truncated_in_directory (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
        raise RuntimeError("Falha persistente de conexão em truncated_in_directory")

    def mark_truncated(self, directory: str, table_name: str):
        """Registra o TRUNCATE de uma tabela para este diretório (idempotente)."""
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO load_truncated (directory, table_name)
                           VALUES (%s, %s)
                           ON CONFLICT (directory, table_name) DO NOTHING""",
                        (directory, table_name),
                    )
                    self.conn.commit()
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão em mark_truncated (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
        raise RuntimeError("Falha persistente de conexão em mark_truncated")

    def start_load_run(self, directory: str) -> int:
        """Registra o início da carga (sinal durável para o export não rodar na janela).

        O export consulta load_run antes de selecionar; sem isso a única proteção
        era pg_stat_activity, que fica vazio enquanto o pipeline baixa o próximo ZIP.
        """
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(
                        """CREATE TABLE IF NOT EXISTS load_run (
                               run_id     BIGSERIAL PRIMARY KEY,
                               directory  VARCHAR(50) NOT NULL,
                               started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                               finished_at TIMESTAMPTZ,
                               note       TEXT
                           )"""
                    )
                    cur.execute(
                        "INSERT INTO load_run (directory) VALUES (%s) RETURNING run_id",
                        (directory,),
                    )
                    run_id = cur.fetchone()[0]
                    self.conn.commit()
                    return run_id
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão em start_load_run (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
        raise RuntimeError("Falha persistente de conexão em start_load_run")

    def finish_load_run(self, run_id: int, note: str = ""):
        """Marca a carga como concluída (sucesso ou falha — o note diz qual)."""
        for attempt in range(3):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute(
                        "UPDATE load_run SET finished_at = NOW(), note = %s WHERE run_id = %s",
                        (note[:500], run_id),
                    )
                    self.conn.commit()
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão em finish_load_run (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)
        raise RuntimeError("Falha persistente de conexão em finish_load_run")

    def _reset_conn(self):
        """Fecha e descarta a conexão (ex.: compute do Neon suspendeu e matou a sessão)."""
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def bulk_upsert(self, df: pl.DataFrame, table_name: str, columns: List[str]):
        """Bulk upsert using temp table + COPY (com reconexão no caso de queda — Neon autosuspend)."""
        if df.is_empty():
            return

        for attempt in range(3):
            try:
                self.connect()
                temp_table = f"temp_{table_name}_{id(df)}"

                with self.conn.cursor() as cur:
                    # 1. Create temp table
                    cur.execute(
                        f"CREATE TEMP TABLE {temp_table} "
                        f"(LIKE {table_name} INCLUDING DEFAULTS INCLUDING STORAGE) ON COMMIT DROP"
                    )

                    # 2. COPY to temp
                    self._copy_to_temp(cur, df, temp_table, columns)

                    # 3. Upsert from temp to main
                    primary_keys = self._get_primary_keys(cur, table_name)
                    self._upsert_from_temp(cur, temp_table, table_name, columns, primary_keys)

                    self.conn.commit()
                return

            except psycopg2.OperationalError as e:
                logger.warning(f"Queda de conexão no upsert de {table_name} (tentativa {attempt + 1}/3): {e}")
                self._reset_conn()
                time.sleep(2 ** attempt)

            except Exception as e:
                if self.conn is not None:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                logger.error(f"Error: {table_name}: {e}")
                raise

        raise RuntimeError(f"Falha persistente de conexão no upsert de {table_name}")

    def _copy_to_temp(self, cur, df: pl.DataFrame, temp_table: str, columns: List[str]):
        """COPY DataFrame to temp table using Polars CSV."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        csv_bytes = df.write_csv(include_header=False).encode("utf-8", errors="replace")
        csv_bytes = csv_bytes.replace(b"\x00", b"")

        cur.copy_expert(
            f"COPY {temp_table} ({columns_str}) FROM STDIN WITH CSV ENCODING 'UTF8'",
            io.BytesIO(csv_bytes),
        )

    def _get_primary_keys(self, cur, table_name: str) -> List[str]:
        """Get primary key columns for a table with caching."""
        if table_name in self._pk_cache:
            return self._pk_cache[table_name]

        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (table_name,),
        )

        primary_keys = [row[0] for row in cur.fetchall()]
        self._pk_cache[table_name] = primary_keys
        return primary_keys

    def _upsert_from_temp(self, cur, temp_table: str, target_table: str, columns: List[str], primary_keys: List[str]):
        """Upsert from temp to target table."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        pk_str = ", ".join([f'"{pk}"' for pk in primary_keys])

        update_cols = [c for c in columns if c not in primary_keys]
        update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        if update_clause:
            # tabelas enxutas (Neon/slim) não têm data_atualizacao
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s AND column_name = 'data_atualizacao'",
                (target_table,),
            )
            if cur.fetchone():
                update_clause += ", data_atualizacao = CURRENT_TIMESTAMP"

        sql = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT DISTINCT ON ({pk_str}) {columns_str} FROM {temp_table} ORDER BY {pk_str}
            ON CONFLICT ({pk_str}) {"DO UPDATE SET " + update_clause if update_clause else "DO NOTHING"}
        """
        cur.execute(sql)
