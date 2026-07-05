from dataclasses import dataclass
from lxml import etree


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
    filer_role = _find("officerTitle") or _find("isDirector")
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
