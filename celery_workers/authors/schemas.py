# coding=utf-8
from marshmallow import fields, validate

from celery_workers.base_schema import StripEmptySchema, ObjectIdField


class AuthorSchema(StripEmptySchema):
    _id = ObjectIdField(load_only=True)
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
    datafeeder_keys = fields.List(fields.String())
    orcids = fields.List(fields.Dict(
        keys=fields.String(validate=validate.OneOf([
            "value", "given_names", "family_name", "biography"])),
        values=fields.String()))
