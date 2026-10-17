from app.main import format_inr, format_usd, format_currency


def test_format_inr():
    assert format_inr(1234567.89).startswith("₹1")
    assert "12,34,567" in format_inr(1234567.89)


def test_format_usd():
    assert format_usd(1234567.89).startswith("$")
    assert "," in format_usd(1234567.89)


def test_format_currency():
    assert format_currency(100, 'INR').startswith('₹')
    assert format_currency(100, 'USD').startswith('$')
