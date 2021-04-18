# coding=utf-8

from marshmallow import Schema, pre_load


class StripEmptySchema(Schema):
    @pre_load
    def _strip_off(self, data, **kwargs):
        stripped_keys = []
        for k, v in data.items():
            if v is None:
                stripped_keys.append(k)
            elif isinstance(v, str) and v.strip() == '':
                stripped_keys.append(k)
            elif isinstance(v, (list, tuple, dict, set)) and len(v) == 0:
                stripped_keys.append(k)
        for k in stripped_keys:
            del data[k]
        return data
