# tests/test_chunked_document_encryption_stress.py
"""
Concurrency & RAM Stress Test for Sub-Phase 8.7:
Chunked Authenticated Envelope (STREAM / 64 KB Blocks) Encryption Engine.

Mandate from docs/QUOTATION_SYSTEM_ROADMAP.md:
"Must pass an automated concurrency test: 20 simultaneous downloads of a 50 MB
 document with server RAM delta strictly remaining under 60 MB."
"""

import io
import os
import sys
import time
import hashlib
import secrets
import tempfile
import tracemalloc
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.core.security_crypto import (
    encrypt_file_stream,
    decrypt_stream_generator,
    DEFAULT_STREAM_CHUNK_SIZE,
)


class RepeatedPatternStream(io.RawIOBase):
    """
    Virtual streaming file-like object generating arbitrary payload sizes (e.g. 50 MB)
    from a small seed pattern with zero memory buffering (< 1 KB RAM).
    """

    def __init__(self, pattern: bytes, total_size: int):
        self.pattern = pattern
        self.total_size = total_size
        self.bytes_read = 0
        self.pat_len = len(pattern)

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        if self.bytes_read >= self.total_size:
            return 0
        
        remaining = self.total_size - self.bytes_read
        to_read = min(len(b), remaining)
        
        offset = self.bytes_read % self.pat_len
        written = 0
        while written < to_read:
            chunk = min(to_read - written, self.pat_len - offset)
            b[written:written + chunk] = self.pattern[offset:offset + chunk]
            written += chunk
            offset = 0
            
        self.bytes_read += to_read
        return to_read

    def read(self, size=-1) -> bytes:
        if size is None or size < 0:
            size = self.total_size - self.bytes_read
        buf = bytearray(min(size, self.total_size - self.bytes_read))
        n = self.readinto(buf)
        return bytes(buf[:n])


def compute_pattern_sha256(pattern: bytes, total_size: int) -> str:
    """Computes reference SHA-256 for the virtual stream."""
    stream = RepeatedPatternStream(pattern, total_size)
    hasher = hashlib.sha256()
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()


def worker_decrypt_and_hash(file_path: str, dek: bytes, expected_hash: str, worker_id: int) -> bool:
    """Worker simulating a bank dealer downloading and streaming decryption."""
    hasher = hashlib.sha256()
    total_bytes = 0

    with open(file_path, "rb") as f:
        for chunk in decrypt_stream_generator(f, dek, context="stress_doc"):
            hasher.update(chunk)
            total_bytes += len(chunk)

    digest = hasher.hexdigest()
    if digest != expected_hash:
        raise ValueError(f"Worker {worker_id} hash mismatch! Expected {expected_hash}, got {digest}")
    return True


def run_stress_test():
    FILE_SIZE_MB = 50
    TOTAL_BYTES = FILE_SIZE_MB * 1024 * 1024  # 50 MB = 52,428,800 bytes
    CONCURRENCY = 20
    RAM_CEILING_MB = 60.0

    print("=" * 80)
    print(f"SUB-PHASE 8.7 CONCURRENCY & RAM STRESS BENCHMARK")
    print(f"Payload Size: {FILE_SIZE_MB} MB | Concurrent Workers: {CONCURRENCY}")
    print(f"Total Transferred Volume: {FILE_SIZE_MB * CONCURRENCY} MB (1.0 GB)")
    print(f"Mandated Python Heap Memory Ceiling: < {RAM_CEILING_MB} MB")
    print("=" * 80)

    dek = secrets.token_bytes(32)
    seed_pattern = b"GROW_TREASURY_INSTITUTIONAL_ENCRYPTED_FACILITY_CONTRACT_STREAM_2026_"

    # Step 1: Compute expected SHA-256 hash
    print("[1/4] Calculating reference SHA-256 digest for virtual 50 MB document...")
    expected_hash = compute_pattern_sha256(seed_pattern, TOTAL_BYTES)
    print(f"      Reference Digest: {expected_hash}")

    # Step 2: Stream-encrypt 50 MB into a temporary file on disk
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".enc")
    temp_path = temp_file.name
    temp_file.close()

    try:
        print(f"[2/4] Stream-encrypting 50 MB document to disk ({temp_path})...")
        t0 = time.time()
        source_stream = RepeatedPatternStream(seed_pattern, TOTAL_BYTES)
        with open(temp_path, "wb") as dest:
            bytes_written = encrypt_file_stream(source_stream, dest, dek, context="stress_doc")
        enc_time = time.time() - t0
        print(f"      Encrypted {bytes_written / (1024*1024):.2f} MB in {enc_time:.2f}s ({bytes_written / (1024*1024) / enc_time:.1f} MB/s)")

        # Step 3: Run 20 concurrent download workers under tracemalloc
        print(f"[3/4] Launching {CONCURRENCY} simultaneous streaming downloads under heap tracking...")
        tracemalloc.start()
        tracemalloc.reset_peak()

        t_start = time.time()
        results = []
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = [
                executor.submit(worker_decrypt_and_hash, temp_path, dek, expected_hash, i)
                for i in range(CONCURRENCY)
            ]
            for f in as_completed(futures):
                results.append(f.result())

        total_duration = time.time() - t_start
        current_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_ram_mb = peak_mem / (1024 * 1024)
        throughput_mb_s = (FILE_SIZE_MB * CONCURRENCY) / total_duration

        # Step 4: Verification and assertions
        print("[4/4] Verification & Analysis:")
        print(f"      Workers Completed: {len(results)}/{CONCURRENCY} (100% SHA-256 match)")
        print(f"      Total Duration: {total_duration:.2f}s")
        print(f"      Aggregate Throughput: {throughput_mb_s:.1f} MB/s")
        print(f"      Peak Python Heap Allocation: {peak_ram_mb:.2f} MB")
        print(f"      Memory Ceiling Target: < {RAM_CEILING_MB} MB")

        assert len(results) == CONCURRENCY, "All workers must succeed"
        assert peak_ram_mb < RAM_CEILING_MB, f"FAIL: Peak RAM {peak_ram_mb:.2f} MB exceeded ceiling of {RAM_CEILING_MB} MB!"

        print("=" * 80)
        print(f"STRESS TEST RESULT: 100% SUCCESSFUL PASS! (RAM: {peak_ram_mb:.2f} MB << {RAM_CEILING_MB} MB)")
        print("=" * 80)

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


if __name__ == "__main__":
    run_stress_test()
