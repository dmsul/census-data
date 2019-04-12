from .bg_2010 import bg_2010
from .bg_2005 import bg_2005
from .bg_2000 import bg_2000


def codebooks(geography: str, year: int) -> dict:
    if geography == 'bg':
        if year == 2010: return bg_2010
        elif year == 2005: return bg_2005
        elif year == 2000: return bg_2000
        else: pass
    elif geography == 'block':
        pass
    else:
        pass

    raise ValueError(f"Geography '{geography}' and year '{year}' invalid.")
