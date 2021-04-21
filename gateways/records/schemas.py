# coding=utf-8
from marshmallow import fields, validate, Schema

from .stringsimhash import StringSimhashField


class OutputAuthorSchema(Schema):
    datafeeder_key = fields.String(load_only=True)
    name = fields.String()
    orcid = fields.String()


class OutputRecordSchema(Schema):
    type = fields.String(
        validate=validate.OneOf(["article", "inproceedings"]))
    title = StringSimhashField()
    authors = fields.List(fields.Nested(OutputAuthorSchema))
    dblp_key = fields.String()
    booktitle = fields.String()
    journal = fields.String()
    volume = fields.String()
    doi = fields.URL()
    ees = fields.List(fields.URL())
    year = fields.String()
    pages = fields.String()
    notes = fields.List(
        fields.Dict(
            keys=fields.String(validate=validate.OneOf(["type", "text"])),
            values=fields.String()),
    )
    abstract = StringSimhashField()
