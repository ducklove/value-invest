from __future__ import annotations

from urllib.parse import urlparse


def is_allowed_report_pdf_url(url: str) -> bool:
    """Allow only known Naver broker-research PDF CDN paths."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.path.endswith(".pdf"):
        return False
    if parsed.netloc == "stock.pstatic.net" and parsed.path.startswith("/stock-research/"):
        return True
    if parsed.netloc == "ssl.pstatic.net" and parsed.path.startswith("/imgstock/upload/research/"):
        return True
    return False
