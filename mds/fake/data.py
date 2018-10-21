"""
Generating random data of miscellaneous types.
"""

from datetime import datetime, timedelta
import json
import random
import string
import uuid


def random_date_from(date,
                     min_td=timedelta(seconds=0),
                     max_td=timedelta(seconds=0)):
    """
    Produces a datetime at a random offset from :date:.
    """
    min_s = min(min_td.total_seconds(), max_td.total_seconds())
    max_s = max(min_td.total_seconds(), max_td.total_seconds())
    offset = random.uniform(min_s, max_s)
    return date + timedelta(seconds=offset)

def random_string(k, chars=None):
    """
    Create a random string of length :k: from the set of uppercase letters
    and numbers.

    Optionally use the set of characters :chars:.
    """
    if chars is None:
        chars = string.ascii_uppercase + string.digits 
    return "".join(random.choices(chars, k=k))

def random_file_url(company):
    url = "-".join(company.split())
    return f"https://{url}.co/{random_string(7)}.jpg".lower()
