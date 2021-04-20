# coding=utf-8

from marshmallow import fields, validate

from celery_workers.base_schema import StripEmptySchema, ObjectIdField
from celery_workers.records.stringsimhash import StringSimhashField


class RecordSchema(StripEmptySchema):
    _id = ObjectIdField(load_only=True)
    type = fields.String(validate=validate.OneOf(["article", "inproceedings"]))
    title = StringSimhashField()
    authors = fields.List(
        fields.Dict(
            keys=fields.String(
                validate=validate.OneOf(["datafeeder_key", "name", "orcid"])),
            values=fields.String()))
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
