import os
import sys
from typing import Dict, List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker


# ------------------- Custom Exceptions -------------------
class SchemaValueError(ValueError):
    pass


# ------------------- Modelos Pydantic -------------------
class ForeignKey(BaseModel):
    target_table: str
    target_column: str
    constraint_name: str


class Column(BaseModel):
    name: str
    type: str
    nullable: bool
    default: Optional[str] = None
    primary_key: bool = False
    foreign_key: Optional[ForeignKey] = None


class Index(BaseModel):
    name: str
    columns: List[str]
    unique: bool
    type: Optional[str] = "index"  # Campo opcional com valor padrão


class ReferencedBy(BaseModel):
    table: str
    columns: List[str]
    constraint: str


class Table(BaseModel):
    description: Optional[str] = None
    columns: Dict[str, Column] = Field(default_factory=dict)
    indexes: Dict[str, Index] = Field(default_factory=dict)
    referenced_by: Dict[str, ReferencedBy] = Field(default_factory=dict)


class Schema(BaseModel):
    tables: Dict[str, Table] = Field(default_factory=dict)


class DatabaseModel(BaseModel):
    schemas: Dict[str, Schema] = Field(default_factory=dict)


# ------------------- Classe de Extração -------------------
class PostgresSchemaExtractor:
    def __init__(
        self, host: str, port: int, user: str, password: str, database: str, schema: str
    ):
        self.connection_url = URL.create(
            drivername="postgresql+psycopg2",
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )

        self.engine = create_engine(self.connection_url)
        self.session = sessionmaker(bind=self.engine)()
        self.inspector = inspect(self.engine)

    def _get_primary_keys(self, schema: str, table_name: str) -> List[str]:
        query = text(
            """
          SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
                ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = CAST(:table_name AS REGCLASS)
              AND i.indisprimary
        """
        )
        full_table_name = f"{schema}.{table_name}"
        result = self.session.execute(query, {"table_name": full_table_name})
        return [row[0] for row in result]

    def _get_foreign_keys(self, schema: str, table_name: str) -> Dict[str, ForeignKey]:
        query = text(
            """
            SELECT
                kcu.column_name,
                con.confrelid::regclass::text AS target_table,
                a.attname AS target_column,
                con.conname AS constraint_name
            FROM pg_constraint con
            JOIN pg_namespace nsp ON nsp.oid = con.connamespace
            JOIN pg_class cl ON cl.oid = con.conrelid
            JOIN pg_attribute a
                ON a.attnum = ANY(con.confkey)
                AND a.attrelid = con.confrelid
            JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_name = con.conname
                AND kcu.table_schema = nsp.nspname
            WHERE con.contype = 'f'
            AND nsp.nspname = :schema
            AND cl.relname = :table_name
        """
        )
        result = self.session.execute(
            query, {"schema": schema, "table_name": table_name}
        )
        return {
            row[0]: ForeignKey(
                target_table=row[1], target_column=row[2], constraint_name=row[3]
            )
            for row in result
        }

    def _get_referenced_by(
        self, schema: str, table_name: str
    ) -> Dict[str, ReferencedBy]:
        query = text(
            """
            SELECT
                con.conrelid::regclass::text AS source_table,
                a.attname AS source_column,
                con.conname AS constraint_name
            FROM pg_constraint con
            JOIN pg_namespace nsp ON nsp.oid = con.connamespace
            JOIN pg_class cl ON cl.oid = con.confrelid
            JOIN pg_attribute a ON a.attnum = ANY(con.confkey) AND a.attrelid = con.confrelid
            WHERE con.contype = 'f'
            AND nsp.nspname = :schema
            AND cl.relname = :table_name
        """
        )
        result = self.session.execute(
            query, {"schema": schema, "table_name": table_name}
        )
        return {
            row[0]: ReferencedBy(table=row[0], columns=[row[1]], constraint=row[2])
            for row in result
        }

    def extract_schema(self) -> DatabaseModel:
        database_model = DatabaseModel()

        # Listar todos os schemas (exceto system schemas)
        schemas = [
            s
            for s in self.inspector.get_schema_names()
            if s not in ("information_schema", "pg_catalog")
        ]

        for schema_name in schemas:
            print(f"schema = {schema_name}", flush=True)
            schema_model = Schema()
            tables = self.inspector.get_table_names(schema=schema_name)

            for table_name in tables:
                table_model = Table()

                # Colunas
                columns = self.inspector.get_columns(table_name, schema=schema_name)
                primary_keys = self._get_primary_keys(schema_name, table_name)
                foreign_keys = self._get_foreign_keys(schema_name, table_name)

                for col in columns:
                    column = Column(
                        name=col["name"],
                        type=str(col["type"]),
                        nullable=col["nullable"],
                        default=col["default"],
                        primary_key=col["name"] in primary_keys,
                    )

                    # Foreign Keys
                    if col["name"] in foreign_keys:
                        column.foreign_key = foreign_keys[col["name"]]

                    table_model.columns[col["name"]] = column

                print(f"table_name = {table_name}", flush=True)
                # print(f"columns = {table_model.columns}", flush=True)
                # print(f"primary_keys = {primary_keys}, foreign_keys = {foreign_keys}", flush=True)

                # Índices
                print(
                    f"Obtendo os indices para a tabela '{schema_name}.{table_name}'",
                    flush=True,
                )
                try:
                    indexes = self.inspector.get_indexes(table_name, schema=schema_name)
                    for idx in indexes:
                        index_type = idx.get(
                            "type", "btree"
                        ).lower()  # Valor padrão para PostgreSQL
                        if idx["name"] != "PRIMARY":
                            index = Index(
                                name=idx["name"],
                                columns=idx["column_names"],
                                unique=idx["unique"],
                                type=index_type,
                            )
                            table_model.indexes[idx["name"]] = index

                except Exception as e:
                    msg = (
                        "Erro ao tentar obter os indices no catálogo para '{schema_name}.{table_name}'. "
                        f"Exception: {e}"
                    )
                    raise SchemaValueError(msg)

                # Referenced By
                try:
                    table_model.referenced_by = self._get_referenced_by(
                        schema_name, table_name
                    )
                except Exception as e:
                    msg = (
                        "Erro ao tentar obter restrições de integridade referencial "
                        f"no catálogo para '{schema_name}.{table_name}'. Exception: {e}"
                    )
                    raise SchemaValueError(msg)

                schema_model.tables[table_name] = table_model

            database_model.schemas[schema_name] = schema_model
            # print(
            #     f"\n\n\ntype(schema_model) = {type(schema_model)}\n"
            #     f"schema_model = \n{schema_model}\n"
            # )

        return database_model

    def close(self):
        print("Fechando conexão ao banco de dados", flush=True)
        self.session.close()
        self.engine.dispose()


# ------------------- Uso -------------------
MY_DB_HOST = os.environ.get("MY_DB_HOST", "NONE")
MY_DB_PORT = os.environ.get("MY_DB_PORT", "NONE")
MY_DB_USER = os.environ.get("MY_DB_USER", "NONE")
MY_DB_PSW = os.environ.get("MY_DB_PSW", "NONE")
MY_DB_DB_NAME = os.environ.get("MY_DB_DB_NAME", "NONE")
MY_DB_SCHEMA_NAME = os.environ.get("MY_DB_SCHEMA_NAME", "NONE")

JSON_DESTINATION_FILENAME = "tmp/schema_documentation.json"


def main():
    # Configuração de conexão
    config = {
        "host": MY_DB_HOST,
        "port": MY_DB_PORT,
        "user": MY_DB_USER,
        "password": MY_DB_PSW,
        "database": MY_DB_DB_NAME,
        "schema": MY_DB_SCHEMA_NAME,
    }

    # Extrair schema
    extractor = PostgresSchemaExtractor(**config)
    print(f"type(extractor) = {type(extractor)}, extractor = {extractor}", flush=True)
    try:
        database_model = extractor.extract_schema()

        # Converter para JSON
        json_output = database_model.model_dump_json(
            indent=2,  # Indentação
            exclude_none=True,  # Opcional: remove campos com valor None
            by_alias=False,  # Mantém os nomes originais dos campos
        )

        # Salvar em arquivo
        with open(JSON_DESTINATION_FILENAME, "w") as f:
            f.write(json_output)

    except Exception as e:
        print(f"Erro durante a extração: {e}", flush=True)
        sys.exit(1)
    finally:
        extractor.close()

    print(
        f"\n\nDocumentação gerada com sucesso em {JSON_DESTINATION_FILENAME}\n",
        flush=True,
    )


if __name__ == "__main__":
    main()
