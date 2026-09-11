"""Chunked sha256 checksums for object payloads."""

import hashlib

DEFAULT_CHUNK_SIZE = 64 * 1024


def compute_checksum(payload: bytes, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Return the hex sha256 digest of ``payload``, hashing in fixed-size chunks."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    digest = hashlib.sha256()
    view = memoryview(payload)
    for start in range(0, len(view), chunk_size):
        digest.update(view[start : start + chunk_size])
    return digest.hexdigest()
