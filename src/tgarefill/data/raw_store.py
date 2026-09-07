from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

import zstandard as zstd


SCHEMA_VERSION = "tgarefill_raw_payload_store_v1"
COMPRESSION_MIN_BYTES = 4096
ZSTD_LEVEL = 3


class RawPayloadStoreError(ValueError):
    """Raised when a stored raw payload cannot be recovered exactly."""


class RawPayloadStore:
    """Small exact-byte store for a related family of raw source payloads."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def put_bytes(self, key: str, payload: bytes, *, source: str = "") -> None:
        sha256 = hashlib.sha256(payload).hexdigest()
        codec, stored = _encode(payload)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO payloads (key, source, sha256, byte_count, codec, payload)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    source = excluded.source,
                    sha256 = excluded.sha256,
                    byte_count = excluded.byte_count,
                    codec = excluded.codec,
                    payload = excluded.payload
                """,
                (key, source, sha256, len(payload), codec, sqlite3.Binary(stored)),
            )

    def put_json(self, key: str, payload: Any, *, source: str = "") -> None:
        encoded = json.dumps(payload, indent=2, default=str).encode("utf-8")
        self.put_bytes(key, encoded, source=source)

    def get_bytes(self, key: str) -> bytes:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT sha256, byte_count, codec, payload FROM payloads WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            raise KeyError(key)
        sha256, byte_count, codec, raw = row
        return _decode(
            bytes(raw),
            codec=str(codec),
            expected_bytes=int(byte_count),
            expected_sha256=str(sha256),
            key=key,
        )

    def get_json(self, key: str) -> Any:
        return json.loads(self.get_bytes(key).decode("utf-8"))

    def __contains__(self, key: str) -> bool:
        with self._connect() as conn:
            return conn.execute("SELECT 1 FROM payloads WHERE key = ?", (key,)).fetchone() is not None

    def compact(self) -> None:
        with self._connect() as conn:
            conn.execute("VACUUM")

    def stats(self) -> dict[str, int]:
        with self._connect() as conn:
            count, raw_bytes, stored_bytes = conn.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(byte_count), 0),
                       COALESCE(SUM(length(payload)), 0)
                FROM payloads
                """
            ).fetchone()
        return {
            "payload_count": int(count),
            "raw_bytes": int(raw_bytes),
            "stored_bytes": int(stored_bytes),
            "saved_bytes": int(raw_bytes) - int(stored_bytes),
            "database_bytes": self.path.stat().st_size,
        }

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS payloads (
                    key TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
                    codec TEXT NOT NULL CHECK (codec IN ('identity', 'zstd')),
                    payload BLOB NOT NULL
                );
                """
            )
            row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
            elif str(row[0]) != SCHEMA_VERSION:
                raise RawPayloadStoreError(f"unsupported raw payload schema: {row[0]}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn


def _encode(payload: bytes) -> tuple[str, bytes]:
    if len(payload) < COMPRESSION_MIN_BYTES:
        return "identity", payload
    compressed = zstd.ZstdCompressor(level=ZSTD_LEVEL).compress(payload)
    if len(compressed) >= len(payload):
        return "identity", payload
    return "zstd", compressed


def _decode(
    stored: bytes,
    *,
    codec: str,
    expected_bytes: int,
    expected_sha256: str,
    key: str,
) -> bytes:
    if codec == "identity":
        payload = stored
    elif codec == "zstd":
        try:
            payload = zstd.ZstdDecompressor().decompress(stored, max_output_size=expected_bytes)
        except zstd.ZstdError as exc:
            raise RawPayloadStoreError(f"stored payload decompression failed: {key}") from exc
    else:
        raise RawPayloadStoreError(f"unsupported payload codec {codec!r}: {key}")
    if len(payload) != expected_bytes or hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise RawPayloadStoreError(f"stored payload hash mismatch: {key}")
    return payload


__all__ = ["RawPayloadStore", "RawPayloadStoreError", "SCHEMA_VERSION"]
