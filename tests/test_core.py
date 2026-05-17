import pytest
from pybutt.core import validate_identifier, quote_identifier


# ------------------------------------------------------------
# ✅ IDENTIFIER VALIDATION
# ------------------------------------------------------------

@pytest.mark.parametrize("valid", [
    "table1", "my_table", "TableName", "t123"
])
def test_validate_identifier_valid(valid):
    assert validate_identifier(valid) == valid


@pytest.mark.parametrize("invalid", [
    "table-name", "table name", "123table", "table;DROP"
])
def test_validate_identifier_invalid(invalid):
    with pytest.raises(ValueError):
        validate_identifier(invalid)


# ------------------------------------------------------------
# ✅ QUOTING
# ------------------------------------------------------------

def test_quote_identifier_basic():
    assert quote_identifier("dbo") == "[dbo]"


def test_quote_identifier_escape():
    assert quote_identifier("a]b") == "[a]]b]"
