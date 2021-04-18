# coding=utf-8

import typing

import simhash
from marshmallow import fields


class StringSimhash:
    MASK_PART0 = 0xffff << 48
    MASK_PART1 = 0xffff << 32
    MASK_PART2 = 0xffff << 16
    MASK_PART3 = 0xffff

    def __init__(self, value: str,
                 hash_parts: typing.List[int] = None):
        self.value = value
        if hash_parts is None:
            self.hash = self._simhash(value)
            self.hash_parts = self._split_simhash(self.hash)
        else:
            self.hash = self._combine_hash_parts(hash_parts)
            self.hash_parts = hash_parts

    def __sub__(self, other: 'StringSimhash'):
        return self.distance(self.hash, other.hash)

    def __str__(self):
        return self.value

    @staticmethod
    def load_from_dict(doc: typing.Dict):
        return StringSimhash(value=doc['value'],
                             hash_parts=[int(doc[f'simhash{n}'])
                                         for n in range(4)])

    @staticmethod
    def load_from_string(value: str):
        return StringSimhash(value=value)

    @staticmethod
    def distance(v1: int, v2: int, length: int = 64) -> int:
        """Calculate the hamming distance of two integers (hashes)"""
        x = (v1 ^ v2) & ((1 << length) - 1)
        ans = 0
        while x:
            ans += 1
            x &= x - 1
        return ans

    def to_dict(self):
        ret = {'value': self.value}
        ret.update({f'simhash{n}': self.hash_parts[n] for n in range(4)})
        return ret

    @staticmethod
    def _simhash(value: str) -> int:
        return simhash.Simhash(value).value

    @staticmethod
    def _split_simhash(hash: int) -> typing.List[int]:
        return [
            StringSimhash.MASK_PART0 & hash,
            StringSimhash.MASK_PART1 & hash,
            StringSimhash.MASK_PART2 & hash,
            StringSimhash.MASK_PART3 & hash
        ]

    @staticmethod
    def _combine_hash_parts(ints: typing.Iterable[int], step=16) -> int:
        ans = 0
        bit = 0
        for aint in ints:
            ans += aint << bit
            bit += step
        return ans


class StringSimhashField(fields.String):
    def _serialize(self, value: StringSimhash, attr: str, obj: typing.Any,
                   **kwargs):
        if value is not None:
            if self.context.get('to', '').lower() == 'db':
                return value.to_dict()
            return str(value)
        return None

    def _deserialize(self, value: typing.Any, attr: typing.Optional[str],
                     data: typing.Optional[typing.Mapping[str, typing.Any]],
                     **kwargs):
        if value:
            if isinstance(value, dict):
                return StringSimhash.load_from_dict(value)
            elif isinstance(value, str):
                return StringSimhash.load_from_string(value)
        return None
