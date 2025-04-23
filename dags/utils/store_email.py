import re

def generate_store_email(store_name):
    """
    Generates a commercial email address for a given store name.

    This function takes the store name, normalizes it by converting spaces to underscores and removing
    special characters, then constructs an email address in the format
    'contato@normalized_store_name.com.br'. If the store name is empty or invalid, returns None.

    Args:
        store_name (str): The name of the store (assumed to have no accents).

    Returns:
        str or None: The generated email address (e.g., 'contato@loja_legal.com.br') or None if the input is invalid.
    """
    if not store_name or not isinstance(store_name, str) or store_name.strip() == "":
        return None

    normalized_name = store_name.lower().strip()
    normalized_name = re.sub(r'[^a-z0-9]', '_', normalized_name)
    normalized_name = re.sub(r'_+', '_', normalized_name)
    normalized_name = normalized_name.strip('_')

    if not normalized_name:
        return None

    email = f"contato@{normalized_name}.com.br"
    return email