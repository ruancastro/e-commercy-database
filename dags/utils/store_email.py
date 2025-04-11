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
    # Validate the input
    if not store_name or not isinstance(store_name, str) or store_name.strip() == "":
        return None

    # Normalize the store name
    # Convert to lowercase and remove leading/trailing spaces
    normalized_name = store_name.lower().strip()

    # Replace spaces with underscores and remove special characters, keeping only alphanumeric and underscores
    normalized_name = re.sub(r'[^a-z0-9]', '_', normalized_name)
    # Replace multiple underscores with a single one
    normalized_name = re.sub(r'_+', '_', normalized_name)
    # Remove leading/trailing underscores
    normalized_name = normalized_name.strip('_')

    # If the normalized name is empty after processing (e.g., only special characters), return None
    if not normalized_name:
        return None

    # Construct the email address
    email = f"contato@{normalized_name}.com.br"
    return email