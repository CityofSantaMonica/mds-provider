"""
Generating random data of miscellaneous types.
"""

import datetime
import random
import string


def random_date_from(date, min_td=None, max_td=None):
    """
    Produces a datetime at a random offset from date.

    Parameters:
        date: datetime
            The reference datetime.

        min_td: int, timedelta, optional
            Number of seconds or timedelta of the minimum offset from the reference datetime (could be negative).

        max_td: int, timedelta, optional
            Number of seconds or timedelta of the maximum offset from the reference datetime (could be negative).

    Return:
        datetime
            A new_date such that (date + min_td) <= new_date < (date + max_td).
    """
    if isinstance(min_td, datetime.timedelta):
        min_td = min_td.total_seconds()
    if isinstance(max_td, datetime.timedelta):
        max_td = max_td.total_seconds()

    if min_td is None and max_td is None:
        min_td, max_td = random.randint(-3600, 3600), random.randint(-3600, 3600)
    elif min_td is not None and max_td is None:
        max_td = random.randint(min_td, 3600)
    elif min_td is None and max_td is not None:
        min_td = random.randint(0, max_td)

    offset = random.uniform(min_td, max_td)
    return date + datetime.timedelta(seconds=offset)


def random_string(k, chars=None):
    """
    Create a random string from the set of uppercase letters and numbers.

    Parameters:
        k: int
            The length of the generated string.

        chars: iterable, optional
            The alphabet of characters from which to generate the string. By default, [A-Z0-9]

    Return:
        str
            The generated string.
    """
    if chars is None:
        chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=k))


def random_file_url(company):
    """
    Generate a random image url string.

    Parameters:
        company: str
            The company name for the hostname.

    Return:
        str
            A generated url for an image on the company's host.
    """
    url = "-".join(company.split())
    return f"https://{url}.co/{random_string(7)}.jpg".lower()
