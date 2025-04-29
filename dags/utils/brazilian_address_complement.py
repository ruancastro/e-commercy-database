import random


def generate_brazilian_address_complement():
    """
    Generates a random address complement in Brazilian format or None.

    This function simulates the generation of a typical address complement found in Brazilian addresses.
    There is a 30% chance that there will be no complement (returns None). If a complement is present,
    there is a 50% chance it will include a type (e.g., "Apto 101") and a 50% chance it will be just a number (e.g., "101").

    Returns:
        str or None: A string representing the address complement, or None if no complement is generated.
    """
    if random.random() <= 0.4:
        return None
    else:
        types = ["Apto", "Sala", "Bloco", "Casa", "Loja"]
        type_ = random.choice(types)
        number = random.randint(1, 999)
        return f"{type_} {number}"
