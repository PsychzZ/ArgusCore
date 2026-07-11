from dataclasses import dataclass
from typing import Any

from lxml import etree

from argus.common.http import make_client, with_retry
from argus.common.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class Form4Data:
    ticker: str
    filer_name: str
    filer_role: str
    transaction_code: str
    shares: int
    price_per_share: float
    value_usd: float


def parse_form4(xml_bytes: bytes) -> Form4Data:
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as e:
        raise ValueError(f"invalid XML: {e}") from e

    def _find(path: str) -> str | None:
        node = root.find(f".//{path}")
        return node.text if node is not None and node.text else None

    ticker = _find("issuerTradingSymbol")
    filer_name = _find("rptOwnerName")
    filer_role = _find("officerTitle") or (
        "Director" if (_find("isDirector") or "").lower() == "true" else ""
    )
    transaction_code = _find("transactionCode")
    shares_str = _find("transactionShares/value")
    price_str = _find("transactionPricePerShare/value")

    if not all([ticker, filer_name, transaction_code, shares_str, price_str]):
        raise ValueError("missing required fields in Form 4 XML")

    shares = int(shares_str)  # type: ignore[arg-type]
    price = float(price_str)  # type: ignore[arg-type]
    return Form4Data(
        ticker=ticker or "",
        filer_name=filer_name or "",
        filer_role=filer_role or "",
        transaction_code=transaction_code or "",
        shares=shares,
        price_per_share=price,
        value_usd=shares * price,
    )


class EdgarClient:
    SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._client = make_client()
        self._client.headers["User-Agent"] = user_agent

    @with_retry()
    async def list_recent_form4_urls(self, since: str) -> list[str]:
        res = await self._client.get(
            self.SEARCH_URL,
            params={"forms": "4", "dateRange": "custom", "startdt": since},
        )
        res.raise_for_status()
        data = res.json()
        urls = []
        for hit in data.get("hits", {}).get("hits", []):
            url = self._hit_to_url(hit)
            if url is None:
                log.warning("sec.malformed_hit", hit_id=hit.get("_id"))
                continue
            urls.append(url)
        return urls

    @staticmethod
    def _hit_to_url(hit: dict[str, Any]) -> str | None:
        # EFTS hit ids look like "0001769628-26-000318:form4.xml"; the archive
        # path needs a CIK (any of the filing's CIKs works) plus the accession
        # number without dashes.
        hit_id: str = hit.get("_id", "")
        if ":" not in hit_id:
            return None
        adsh, _, filename = hit_id.partition(":")
        ciks = hit.get("_source", {}).get("ciks") or []
        if not ciks or not filename:
            return None
        cik = ciks[0].lstrip("0")
        return f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh.replace('-', '')}/{filename}"

    @with_retry()
    async def fetch_filing_xml(self, url: str) -> bytes:
        res = await self._client.get(url)
        res.raise_for_status()
        return res.content

    async def close(self) -> None:
        await self._client.aclose()
