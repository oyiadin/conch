# coding=utf-8
from fastapi import FastAPI, HTTPException
from marshmallow import EXCLUDE
from typing import List, Optional, Dict

from pydantic import BaseModel

from gateways import *
from gateways.schemas import OutputRecordSchema


@app.get("/record/{key:path}")
async def query_record(key: str):
    if key.startswith('doi:'):
        query = {'doi': key[len('doi:'):]}
    elif len(key) == 40:
        query = {'_id': key}
    else:
        raise HTTPException(status_code=400, detail="unknown key pattern")

    schema = OutputRecordSchema(unknown=EXCLUDE, partial=True)
    result = t_records.find_one(query)
    if result is None:
        raise HTTPException(status_code=404)
    return schema.dump(schema.load(result))


@app.post('/search/record')
async def search_record(query_str: str):
    query = { '$text': { '$search': query_str } }
    projection = { 'score': { '$meta': 'textScore' } }
    a_sort = [( 'score', { '$meta': 'textScore' } )]
    results = t_records.find(query, projection).sort(a_sort).limit(50)
    schema = OutputRecordSchema(unknown=EXCLUDE, partial=True)
    records = schema.dump(schema.load(results, many=True), many=True)
    return {
        'totalNumber': len(records),
        'records': records,
    }


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app,
                host=conf['server']['host'],
                port=int(conf['server']['port']))
