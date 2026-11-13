"""Shared test secrets. This is a fixture key, not a production key."""

TEST_PII_KEY = "ab" * 32
TEST_KEY_BYTES = bytes.fromhex(TEST_PII_KEY)
