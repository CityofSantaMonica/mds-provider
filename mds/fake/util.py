"""
Generating random data of miscellaneous types.
"""

import datetime
import random
import string


def random_date_from(date,
                     min_td=datetime.timedelta(seconds=0),
                     max_td=datetime.timedelta(seconds=0)):
    """
    Produces a datetime at a random offset from date.

    Parameters:
        date: datetime
            The reference datetime.

        min_td: timedelta, optional
            The minimum offset from the reference datetime (could be negative).

        max_td: timedelta, optional
            The maximum offset from the reference datetime (could be negative).

    Return:
        datetime
            A new_date such that (date + min_td) <= new_date < (date + max_td).
    """
    min_s = min(min_td.total_seconds(), max_td.total_seconds())
    max_s = max(min_td.total_seconds(), max_td.total_seconds())
    offset = random.uniform(min_s, max_s)
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
