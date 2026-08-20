"""Production secret-storage adapters."""

from infrastructure.secrets.google_secret_manager import GoogleSecretManagerSecretStore

__all__ = ["GoogleSecretManagerSecretStore"]
