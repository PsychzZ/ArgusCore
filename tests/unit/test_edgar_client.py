import pytest
import respx

from argus.sec_poller.edgar import EdgarClient


@pytest.mark.asyncio
async def test_client_fetches_filings():
    with respx.mock:
        respx.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"q": '"form type":"4"', "dateRange": "custom", "startDt": "2026-07-04"},
        ).respond(
            200,
            json={
                "hits": {
                    "hits": [
                        {
                            "_id": "abc",
                            "_source": {
                                "_id": "https://www.sec.gov/Archives/edgar/data/123/abc.xml"
                            },
                        },
                        {
                            "_id": "def",
                            "_source": {
                                "_id": "https://www.sec.gov/Archives/edgar/data/456/def.xml"
                            },
                        },
                    ]
                }
            },
        )
        respx.get("https://www.sec.gov/Archives/edgar/data/123/abc.xml").respond(
            200, content=b"<xml/>"
        )
        respx.get("https://www.sec.gov/Archives/edgar/data/456/def.xml").respond(
            200, content=b"<xml/>"
        )

        client = EdgarClient(user_agent="Test/1.0 (test@example.com)")
        urls = await client.list_recent_form4_urls(since="2026-07-04")
        assert len(urls) == 2

        xml = await client.fetch_filing_xml(urls[0])
        assert xml == b"<xml/>"
        await client.close()


@pytest.mark.asyncio
async def test_client_sends_user_agent():
    with respx.mock as mock:
        route = mock.get("https://efts.sec.gov/LATEST/search-index").respond(
            200, json={"hits": {"hits": []}}
        )
        client = EdgarClient(user_agent="MyAgent/1.0 (test@example.com)")
        await client.list_recent_form4_urls(since="2026-07-04")
        request = route.calls.last.request
        assert request.headers["user-agent"] == "MyAgent/1.0 (test@example.com)"
        await client.close()
