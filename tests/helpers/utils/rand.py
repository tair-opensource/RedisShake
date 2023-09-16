import random
import string


def random_string(length=8) -> str:
    chars = string.ascii_letters + string.digits
    random_str = ''.join(random.choices(chars, k=length))
    return random_str
