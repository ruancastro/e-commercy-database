import re

def normalize_name(name):
    """
    Normalizes the provided name by converting it to lowercase and removing non-alphabetic characters.
    """
    name = name.lower()
    name = re.sub(r'[^a-z]', '', name)  # Removes non-alphabetic characters
    return name

def generate_customer_email(full_name):
    """
    Generates a customer email address based on the provided full name.

    This function takes the full name, normalizes it, and constructs an email address in the format
    'first.last@example.com'. It handles names with multiple parts and removes common articles and
    prepositions (e.g., 'da', 'de', 'dos'). If the input is invalid, returns None.

    Args:
        full_name (str): The full name of the user (e.g., "João Silva").

    Returns:
        str or None: The generated email address (e.g., "joao.silva@example.com") or None if the input is invalid.
    """
    # Input validation
    if not full_name or not isinstance(full_name, str) or full_name.strip() == "":
        return None

    # Split the full name into parts
    parts = full_name.split()

    # Remove common articles and prepositions (e.g., "da", "de", "dos", "das")
    parts = [p for p in parts if p.lower() not in ['da', 'de', 'dos', 'das']]

    if len(parts) < 2:
        # If there's only one name, use it alone
        email = normalize_name(parts[0])
    else:
        # Use the first and last name
        first = normalize_name(parts[0])
        last = normalize_name(parts[-1])
        email = f"{first}.{last}"

    # Add the domain
    email += "@example.com"
    return email