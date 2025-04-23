import random
from typing import Optional

def generate_random_phone_number(forced_type: Optional[str] = None) -> tuple[str, str]:
    """
    Generates a random Brazilian phone number with its corresponding phone type.
    
    - Valid DDDs only.
    - Random formats: no separators, with hyphen, with space, or with parentheses.
    - If forced_type is provided ("Residential", "Mobile", or "Commercial"), the number 
      will be generated accordingly.
    - If no type is forced, type will be selected randomly, with bias towards Residential.

    Args:
        forced_type (Optional[str]): Optional phone type to force. Must be "Residential", "Mobile", or "Commercial".

    Returns:
        dict[phone_type, number]: A dictionary containing the phone type ("Mobile", "Residential", or "Commercial") and its number (str).
    """
    
    valid_ddds = [
        *range(11, 20), 21, 22, 24, 27, 28,
        *range(31, 39), *range(41, 47),
        47, 48, 49, 51, 53, 54, 55,
        61, 62, 63, 64, 65, 66, 67, 68, 69,
        71, 73, 74, 75, 77, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89,
        91, 92, 93, 94, 95, 96, 97, 98, 99
    ]
    
    ddd = str(random.choice(valid_ddds))
    
    allowed_types = ["Residential", "Mobile", "Commercial"]
    if forced_type:
        forced_type = forced_type.capitalize()
        if forced_type not in allowed_types:
            raise ValueError(f"Invalid forced_type. Must be one of: {allowed_types}")
        phone_type = forced_type
    else:
        phone_type = random.choices(allowed_types, weights=[0.5, 0.3, 0.2])[0]

    if phone_type == "Mobile":
        first_part = '9' + str(random.randint(1000, 9999))
    else:
        first_digit = str(random.randint(2, 8))
        first_part = first_digit + str(random.randint(100, 9999)).zfill(4)
    
    second_part = str(random.randint(1000, 9999))

    format_type = random.choice(["plain", "hyphen", "space", "parentheses"])

    if format_type == "plain":
        number = f"{ddd}{first_part}{second_part}"
    elif format_type == "hyphen":
        number = f"{ddd}-{first_part}{second_part}"
    elif format_type == "space":
        number = f"{ddd} {first_part}{second_part}"
    elif format_type == "parentheses":
        separator = random.choice([" ", "-"])
        number = f"({ddd}){separator}{first_part}{second_part}"

    return {"phone_type": phone_type, "number": number}
