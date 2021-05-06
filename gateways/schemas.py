# coding=utf-8
from marshmallow import fields, validate, Schema


class OutputAuthorSchema(Schema):
    name = fields.String()
    ids = fields.List(fields.String())


class OutputRecordSchema(Schema):
    _id = fields.String()
    title = fields.String()
    paperAbstract = fields.String()
    authors = fields.List(fields.Nested(OutputAuthorSchema))
    inCitations = fields.List(fields.String)
    outCitations = fields.List(fields.String)
    uear = fields.Integer()
    pdfurls = fields.List(fields.URL)
    venue = fields.String()
    journalName = fields.String()
    journalVolume = fields.String()
    journalPages = fields.String()
    doi = fields.String()
    fieldsOfStudy = fields.List(fields.String)

