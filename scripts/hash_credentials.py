"""
Utility to generate bcrypt hashes for device secrets and researcher passwords.

Usage:
    python scripts/hash_credentials.py <plaintext>

The output hash can be pasted directly into device_registry.json
(device_secret_hash) or researcher_registry.json (password_hash).
"""
import sys

import bcrypt


def hash_credential(plaintext: str) -> str:
    return bcrypt.hashpw(plaintext.encode(), bcrypt.gensalt(12)).decode()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/hash_credentials.py <plaintext>")
        sys.exit(1)
    print(hash_credential(sys.argv[1]))
