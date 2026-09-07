from pathlib import Path

from tgarefill.data.raw_store import RawPayloadStore


def test_raw_payload_store_compresses_and_roundtrips_exact_bytes(tmp_path: Path) -> None:
    payload = (b"record_date,value\n" + b"2024-01-03,1000000\n" * 10_000)
    store = RawPayloadStore(tmp_path / "raw.sqlite")

    store.put_bytes("large", payload, source="/example")

    assert store.get_bytes("large") == payload
    assert "large" in store
    stats = store.stats()
    assert stats["payload_count"] == 1
    assert stats["stored_bytes"] < stats["raw_bytes"]
    assert stats["saved_bytes"] > 0
    assert not (tmp_path / "raw.sqlite-wal").exists()
    assert not (tmp_path / "raw.sqlite-shm").exists()
