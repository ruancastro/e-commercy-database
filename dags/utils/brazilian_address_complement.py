import random

def generate_brazilian_address_complement():
    """
    Gera um complemento de endereço aleatório no formato brasileiro ou None.

    Esta função simula a geração de um complemento de endereço comum em endereços brasileiros.
    Há 30% de chance de não haver complemento (retorna None). Se houver complemento,
    há 50% de chance de ser um complemento com tipo (ex.: "Apto 101") e 50% de chance
    de ser apenas um número (ex.: "101").

    Retorna:
        str ou None: Uma string representando o complemento do endereço ou None se não houver complemento.
    """
    if random.random() <= 0.4:
        return None
    else:
            types = ["Apto", "Sala", "Bloco", "Casa", "Loja"]
            type_ = random.choice(types)
            number = random.randint(1, 999)
            return f"{type_} {number}"