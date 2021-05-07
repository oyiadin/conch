# coding=utf-8
import json
import random
from typing import Optional, Dict

from redis import Redis


__all__ = ['session_manager', 'SessionManager']


class DictProxy(dict):
    def __init__(self, sess: 'SessionManager', sess_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sess = sess
        self.sess_key = sess_key
        self.entered = False

    def __enter__(self):
        self.entered = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.entered = False
        self.sess[self.sess_key] = json.dumps(self)

    def __setitem__(self, key, value):
        assert self.entered, "You can set item within `with` statement only"
        super().__setitem__(key, value)


class SessionManager:
    KEY_FORMAT = "session-%s"

    def __init__(self, r: Optional[Redis] = None):
        if r is None:
            from gateways import r as _r
            r = _r
        self.r = r

    def __getitem__(self, key):
        return DictProxy(self, key, json.loads(self.r.get(key)))

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = json.dumps(value)
        self.r.set(key, value)

    @staticmethod
    def get_a_random_key():
        return ''.join(random.sample(
            'ABCDEFGHJKLMNPQRTUVWXYabcdefghjkmnpqrtuvwxy346789-/@#:.', k=40))

    def new_item(self, value: Dict):
        key = self.get_a_random_key()
        self[key] = value
        return key


session_manager = SessionManager()
