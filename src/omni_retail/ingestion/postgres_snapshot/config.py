"""Connection configuration for the PostgreSQL OLTP source."""

import os
from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432


@dataclass(frozen=True)
class PostgresSourceConfig:
    """Connection settings of the OLTP source; secrets come from the environment."""

    host: str
    port: int
    user: str
    password: str
    dbname: str

    def __repr__(self) -> str:
        return (
            f"PostgresSourceConfig(host={self.host!r}, port={self.port}, "
            f"user={self.user!r}, password=***masked***, dbname={self.dbname!r})"
        )

    def conninfo(self) -> str:
        return make_conninfo(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
        )

    @classmethod
    def from_env(cls) -> "PostgresSourceConfig":
        required = {
            "POSTGRES_USER": os.environ.get("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB"),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"missing required environment variables: {', '.join(missing)}")
        return cls(
            host=os.environ.get("POSTGRES_HOST", DEFAULT_HOST),
            port=int(os.environ.get("POSTGRES_PORT", DEFAULT_PORT)),
            user=required["POSTGRES_USER"],  # type: ignore[arg-type]
            password=required["POSTGRES_PASSWORD"],  # type: ignore[arg-type]
            dbname=required["POSTGRES_DB"],  # type: ignore[arg-type]
        )
