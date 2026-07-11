import pytest
import respx

from argus.sec_poller.edgar import EdgarClient


@pytest.mark.asyncio
async def test_client_fetches_filings():
    with respx.mock:
        respx.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"forms": "4", "dateRange": "custom", "startdt": "2026-07-04"},
        ).respond(
            200,
            json={
                "hits": {
                    "hits": [
                        {
                            "_id": "0001769628-26-000318:form4.xml",
                            "_source": {"ciks": ["0002058037", "0001769628"]},
                        },
                        {
                            "_id": "0002144805-26-000003:wk-form4_1783722760.xml",
                            "_source": {"ciks": ["0002144805"]},
                        },
                    ]
                }
            },
        )
        expected_url = (
            "https://www.sec.gov/Archives/edgar/data/2058037/000176962826000318/form4.xml"
        )
        respx.get(expected_url).respond(200, content=b"<xml/>")

        client = EdgarClient(user_agent="Test/1.0 (test@example.com)")
        urls = await client.list_recent_form4_urls(since="2026-07-04")
        assert urls == [
            expected_url,
            (
                "https://www.sec.gov/Archives/edgar/data/2144805/000214480526000003/"
                "wk-form4_1783722760.xml"
            ),
        ]

        xml = await client.fetch_filing_xml(urls[0])
        assert xml == b"<xml/>"
        await client.close()


@pytest.mark.asyncio
async def test_client_skips_malformed_hits():
    with respx.mock:
        respx.get("https://efts.sec.gov/LATEST/search-index").respond(
            200,
            json={
                "hits": {
                    "hits": [
                        {"_id": "no-colon-here", "_source": {"ciks": ["0000000001"]}},
                        {"_id": "0001-26-0001:f.xml", "_source": {"ciks": []}},
                    ]
                }
            },
        )
        client = EdgarClient(user_agent="Test/1.0 (test@example.com)")
        urls = await client.list_recent_form4_urls(since="2026-07-04")
        assert urls == []
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
