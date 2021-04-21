# coding=utf-8
from marshmallow import fields, validate, Schema


class OutputAuthorSchema(Schema):
    names = fields.List(fields.String())
    uname = fields.String()
    affiliations = fields.List(fields.Dict(
        keys=fields.String(validate=validate.OneOf(["label", "text"])),
        values=fields.String()))
    urls = fields.List(fields.Dict(
        keys=fields.String(validate=validate.OneOf(["type", "text"])),
        values=fields.String()))
    awards = fields.List(fields.Dict(
        keys=fields.String(validate=validate.OneOf(["label", "text"])),
        values=fields.String()))
    dblp_homepage = fields.URL()
    is_disambiguation = fields.Boolean()
    orcids = fields.List(fields.Dict(
        keys=fields.String(validate=validate.OneOf([
            "value", "given_names", "family_name", "biography"])),
        values=fields.String()))
