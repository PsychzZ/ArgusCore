from argus.processor.dedup import normalize_title, titles_similar


def test_normalize_title_strips_punctuation_case_whitespace():
    assert normalize_title("  NVIDIA: CEO buys  50,000 shares!  ") == (
        "nvidia ceo buys 50000 shares"
    )


def test_titles_similar_rephrased_story():
    a = "NVIDIA CEO Jensen Huang buys 50,000 shares"
    b = "Nvidia CEO Huang buys 50000 shares!"
    assert titles_similar(a, b) is True


def test_titles_similar_different_story_same_ticker():
    a = "NVIDIA CEO buys 50,000 shares"
    b = "NVIDIA announces new datacenter GPU lineup for 2027"
    assert titles_similar(a, b) is False
