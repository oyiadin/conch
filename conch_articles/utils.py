# coding=utf-8
from typing import Tuple, List, Iterable, Any, Optional, Dict

import pymongo.database
from simhash import Simhash


MASK_PART1 = 0xffff << 48
MASK_PART2 = 0xffff << 32
MASK_PART3 = 0xffff << 16
MASK_PART4 = 0xffff


def simhash(x: Any) -> int:
    return Simhash(x).value


def _distance(v1: int, v2: int, length: int = 64) -> int:
    """calculate the hamming distance of two integers (hashes)"""
    x = (v1 ^ v2) & ((1 << length) - 1)
    ans = 0
    while x:
        ans += 1
        x &= x - 1
    return ans


def _int_list_to_large_int(ints: Iterable[int], step=16) -> int:
    ans = 0
    bit = 0
    for aint in ints:
        ans += aint << bit
        bit += step
    return ans


def get_hash_parts(value: int) -> Tuple[int, int, int, int]:
    return (
        MASK_PART1 & value,
        MASK_PART2 & value,
        MASK_PART3 & value,
        MASK_PART4 & value
    )


def _get_target_field(document: dict, field_name: str) -> dict:
    field_name_levels = field_name.split('.')
    for name in field_name_levels:
        document = document[name]

    return document


def _find_similar__specific_field(t_article: pymongo.database.Collection,
                                  field_name: str,
                                  hash_parts: Tuple[int, int, int, int],
                                  distance_tolerance: int = 3) -> Optional[Dict]:
    """To find inside the database whether any similar documents can be found.
    The field_name should conform the pattern of mongodb key rule"""

    article = t_article.find_one({
        '$or': [
            { f'{field_name}.simhash{i+1}': hash_parts[i] }
            for i in range(4)
        ]
    })

    if article is None:
        return None
    else:
        target_field = _get_target_field(article, field_name)
        hash1 = _int_list_to_large_int(
            [target_field[f'simhash{i+1}'] for i in range(4)])
        hash2 = _int_list_to_large_int(hash_parts)
        distance = _distance(hash1, hash2)
        if distance > distance_tolerance:
            return None
        return article


def find_similar(t_article: pymongo.database.Collection,
                 title: str) -> Optional[Dict]:
    title_hash_parts = get_hash_parts(simhash(title))
    article_with_similar_title = _find_similar__specific_field(
        t_article, "title", title_hash_parts)

    return article_with_similar_title


def strip_off_blank_values(**kwargs):
    """Strip off the Key-Value pairs with blank values
    >>> assert(strip_off_blank_values(a=1, b=None, c='', d='x') == dict(a=1, d='x')"""

    keys_to_remove = []
    for key, value in kwargs.items():
        if not value:
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del kwargs[key]

    return kwargs
