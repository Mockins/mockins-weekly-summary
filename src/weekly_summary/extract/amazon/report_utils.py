from __future__ import annotations

import base64
import gzip
import io
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from sp_api.api import Reports
from sp_api.base.exceptions import SellingApiRequestThrottledException


@dataclass(frozen=True)
class ReportWaitConfig:
    poll_seconds: int = 20
    max_minutes: int = 30


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def wait_for_report(reports: Reports, report_id: str, *, cfg: ReportWaitConfig = ReportWaitConfig()) -> str:
    """
    Poll get_report until DONE and return reportDocumentId.
    Raises on FATAL/CANCELLED. Times out after cfg.max_minutes.
    """
    deadline = _utc_now().timestamp() + (cfg.max_minutes * 60)
    attempt = 0

    while True:
        attempt += 1
        if _utc_now().timestamp() > deadline:
            raise TimeoutError(f"Timed out waiting for reportId={report_id} after {cfg.max_minutes} minutes")

        try:
            res = reports.get_report(reportId=report_id)
        except SellingApiRequestThrottledException:
            sleep_s = min(cfg.poll_seconds * attempt, 120)
            time.sleep(sleep_s)
            continue

        payload = res.payload or {}
        status = payload.get("processingStatus")
        doc_id = payload.get("reportDocumentId")

        if status == "DONE":
            if not doc_id:
                raise RuntimeError(f"Report DONE but missing reportDocumentId. payload={payload}")
            return doc_id

        if status in {"FATAL", "CANCELLED"}:
            raise RuntimeError(f"Report failed: reportId={report_id} status={status} payload={payload}")

        time.sleep(cfg.poll_seconds)


def download_report_document(doc_payload: dict[str, Any], *, timeout_s: int = 90) -> bytes:
    """
    Download report bytes from the presigned URL.

    Handles optional:
      - encryptionDetails (AES-256-CBC)
      - compressionAlgorithm (GZIP)

    Returns bytes ready to parse.
    """
    url = doc_payload.get("url")
    if not url:
        raise ValueError(f"Report document payload missing 'url': {doc_payload}")

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    content = resp.content

    enc = doc_payload.get("encryptionDetails")
    if enc:
        content = _decrypt_document(content, enc)

    compression = doc_payload.get("compressionAlgorithm")
    if compression:
        comp = str(compression).upper()
        if comp == "GZIP":
            content = _gunzip(content)
        else:
            raise ValueError(f"Unsupported compressionAlgorithm: {compression}")

    # Defensive: sometimes compressionAlgorithm is omitted but data is gzipped
    if len(content) >= 2 and content[0] == 0x1F and content[1] == 0x8B:
        content = _gunzip(content)

    return content


def _gunzip(data: bytes) -> bytes:
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
        return gz.read()


def _decrypt_document(data: bytes, encryption_details: dict[str, Any]) -> bytes:
    """
    Decrypt encrypted report content using AES CBC with PKCS7 padding.
    Requires `cryptography`.
    """
    key_b64 = encryption_details.get("key")
    iv_b64 = encryption_details.get("initializationVector")
    if not key_b64 or not iv_b64:
        raise ValueError(f"Invalid encryptionDetails payload: {encryption_details}")

    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives.padding import PKCS7
    except Exception as e:
        raise RuntimeError(
            "Report document is encrypted but 'cryptography' is not installed. "
            "Install it with: pip install cryptography"
        ) from e

    key = base64.b64decode(key_b64)
    iv = base64.b64decode(iv_b64)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    padded = decryptor.update(data) + decryptor.finalize()

    unpadder = PKCS7(algorithms.AES.block_size).unpadder()
    return unpadder.update(padded) + unpadder.finalize()