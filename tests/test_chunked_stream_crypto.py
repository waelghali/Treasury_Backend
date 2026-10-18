# tests/test_chunked_stream_crypto.py
"""
Unit Test Suite for Sub-Phase 8.7:
Chunked Authenticated Envelope (STREAM / 64 KB Blocks) Encryption Engine.
"""

import io
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import secrets
from app.core.security_crypto import (
    encrypt_stream_generator,
    decrypt_stream_generator,
    encrypt_file_bytes,
    decrypt_file_bytes,
    encrypt_file_stream,
    is_encrypted_file,
    STREAM_MAGIC,
    RAW_FILE_MAGIC,
    TamperDetectedError,
    InvalidKeyError,
    DEFAULT_STREAM_CHUNK_SIZE,
)


def get_dek():
    return secrets.token_bytes(32)


def test_small_payload_roundtrip():
    dek = get_dek()
    raw_data = b"CONFIDENTIAL_TREASURY_TERM_SHEET_DATA" * 5
    encrypted = encrypt_file_bytes(raw_data, dek, context="term_sheet")

    assert is_encrypted_file(encrypted), "Must identify as encrypted file"
    assert encrypted.startswith(STREAM_MAGIC), "Must start with STREAM_MAGIC"
    assert raw_data not in encrypted, "Zero knowledge: raw data must not be in ciphertext"

    decrypted = decrypt_file_bytes(encrypted, dek, context="term_sheet")
    assert decrypted == raw_data, "Decrypted data must match original"
    print("[PASS] test_small_payload_roundtrip")


def test_exact_chunk_size_payload():
    dek = get_dek()
    raw_data = b"A" * DEFAULT_STREAM_CHUNK_SIZE
    encrypted = encrypt_file_bytes(raw_data, dek, context="exact_chunk")

    decrypted = decrypt_file_bytes(encrypted, dek, context="exact_chunk")
    assert decrypted == raw_data
    assert len(decrypted) == DEFAULT_STREAM_CHUNK_SIZE
    print("[PASS] test_exact_chunk_size_payload")


def test_multi_chunk_payload_streaming():
    dek = get_dek()
    # 2.5 chunks = 160 KB
    raw_data = os.urandom(int(DEFAULT_STREAM_CHUNK_SIZE * 2.5))
    
    stream_in = io.BytesIO(raw_data)
    stream_out = io.BytesIO()
    
    bytes_written = encrypt_file_stream(stream_in, stream_out, dek, context="multi_chunk")
    assert bytes_written > len(raw_data)
    
    encrypted_bytes = stream_out.getvalue()
    assert is_encrypted_file(encrypted_bytes)
    
    # Decrypt via generator
    decrypt_in = io.BytesIO(encrypted_bytes)
    decrypted_chunks = list(decrypt_stream_generator(decrypt_in, dek, context="multi_chunk"))
    
    decrypted_full = b"".join(decrypted_chunks)
    assert decrypted_full == raw_data
    assert len(decrypted_chunks) == 3  # 2 full 64KB blocks + 1 remainder block
    print("[PASS] test_multi_chunk_payload_streaming")


def test_empty_payload():
    dek = get_dek()
    raw_data = b""
    encrypted = encrypt_file_bytes(raw_data, dek, context="empty")
    assert is_encrypted_file(encrypted)

    decrypted = decrypt_file_bytes(encrypted, dek, context="empty")
    assert decrypted == b""
    print("[PASS] test_empty_payload")


def test_nonce_uniqueness_across_encryptions():
    dek = get_dek()
    raw_data = b"IDENTICAL_PLAINTEXT_DATA_REPEATED"
    enc1 = encrypt_file_bytes(raw_data, dek, context="test")
    enc2 = encrypt_file_bytes(raw_data, dek, context="test")

    assert enc1 != enc2
    # Salts should differ
    salt1 = enc1[len(STREAM_MAGIC) + 4 : len(STREAM_MAGIC) + 4 + 7]
    salt2 = enc2[len(STREAM_MAGIC) + 4 : len(STREAM_MAGIC) + 4 + 7]
    assert salt1 != salt2
    print("[PASS] test_nonce_uniqueness_across_encryptions")


def test_tamper_detection_bit_flip():
    dek = get_dek()
    raw_data = b"TAMPER_TEST_DATA" * 50
    encrypted = bytearray(encrypt_file_bytes(raw_data, dek, context="tamper"))

    # Flip a byte inside ciphertext payload
    target_idx = 35
    encrypted[target_idx] ^= 0xFF

    failed = False
    try:
        decrypt_file_bytes(bytes(encrypted), dek, context="tamper")
    except TamperDetectedError:
        failed = True
    assert failed, "Must raise TamperDetectedError on bit flip"
    print("[PASS] test_tamper_detection_bit_flip")


def test_truncation_attack_detection():
    dek = get_dek()
    # 2 full chunks
    raw_data = b"X" * (DEFAULT_STREAM_CHUNK_SIZE * 2)
    encrypted = encrypt_file_bytes(raw_data, dek, context="truncation")

    # Cut off the last 100 bytes (truncating the final block)
    truncated = encrypted[:-100]

    failed = False
    try:
        decrypt_file_bytes(truncated, dek, context="truncation")
    except TamperDetectedError:
        failed = True
    assert failed, "Must raise TamperDetectedError on truncated stream"
    print("[PASS] test_truncation_attack_detection")


def test_foreign_dek_rejection():
    dek = get_dek()
    raw_data = b"HIGHLY_SECRET_CORPORATE_TREASURY_DATA"
    encrypted = encrypt_file_bytes(raw_data, dek, context="isolation")

    foreign_dek = secrets.token_bytes(32)
    failed = False
    try:
        decrypt_file_bytes(encrypted, foreign_dek, context="isolation")
    except TamperDetectedError:
        failed = True
    assert failed, "Must raise TamperDetectedError on foreign tenant key"
    print("[PASS] test_foreign_dek_rejection")


def test_context_binding_aad():
    dek = get_dek()
    raw_data = b"AAD_BINDING_TEST"
    encrypted = encrypt_file_bytes(raw_data, dek, context="leg_1_doc")

    failed = False
    try:
        decrypt_file_bytes(encrypted, dek, context="leg_2_doc")
    except TamperDetectedError:
        failed = True
    assert failed, "Must raise TamperDetectedError on mismatched context AAD"
    print("[PASS] test_context_binding_aad")


def test_dual_read_legacy_unencrypted():
    dek = get_dek()
    raw_pdf = b"%PDF-1.7\nSample unencrypted legacy corporate guarantee contract"
    decrypted = decrypt_file_bytes(raw_pdf, dek, context="any")
    assert decrypted == raw_pdf
    assert not is_encrypted_file(raw_pdf)
    print("[PASS] test_dual_read_legacy_unencrypted")


def test_dual_read_legacy_v1_monolithic():
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    dek = get_dek()
    raw_data = b"LEGACY_V1_DOCUMENT_DATA"
    nonce = secrets.token_bytes(12)
    aesgcm = AESGCM(dek)
    ciphertext = aesgcm.encrypt(nonce, raw_data, b"legacy_context")
    v1_payload = RAW_FILE_MAGIC + nonce + ciphertext

    assert is_encrypted_file(v1_payload)
    decrypted = decrypt_file_bytes(v1_payload, dek, context="legacy_context")
    assert decrypted == raw_data
    print("[PASS] test_dual_read_legacy_v1_monolithic")


def test_invalid_key_length():
    failed = False
    try:
        encrypt_file_bytes(b"data", b"short_key_16_byte")
    except InvalidKeyError:
        failed = True
    assert failed, "Must raise InvalidKeyError on non-32 byte key"
    print("[PASS] test_invalid_key_length")


if __name__ == "__main__":
    tests = [
        test_small_payload_roundtrip,
        test_exact_chunk_size_payload,
        test_multi_chunk_payload_streaming,
        test_empty_payload,
        test_nonce_uniqueness_across_encryptions,
        test_tamper_detection_bit_flip,
        test_truncation_attack_detection,
        test_foreign_dek_rejection,
        test_context_binding_aad,
        test_dual_read_legacy_unencrypted,
        test_dual_read_legacy_v1_monolithic,
        test_invalid_key_length,
    ]
    print(f"Running {len(tests)} Sub-Phase 8.7 Cryptographic Unit Tests...")
    for t in tests:
        t()
    print("ALL 12 TESTS PASSED PERFECTLY!")
