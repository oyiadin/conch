# coding=utf-8
import hashlib

from bson import ObjectId
from celery.result import AsyncResult
from fastapi import FastAPI, HTTPException, Cookie, Request
from marshmallow import EXCLUDE
from typing import Optional

from fastapi import Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from gateways import *
from gateways.schemas import OutputRecordSchema
from gateways.session import session_manager



def _query_record(key):
    if key.startswith('doi:'):
        query = {'doi': key[len('doi:'):]}
    elif len(key) == 40:
        query = {'_id': key}
    else:
        raise HTTPException(status_code=400, detail="unknown key pattern")

    record = t_records.find_one(query)
    if record is None:
        raise HTTPException(status_code=404)
    return record


@app.get("/record/{key:path}")
async def query_record(key: str, no_record_history: bool = False,
                       session: Optional[str] = Cookie(None)):
    record = _query_record(key)

    if session is not None and not no_record_history:
        user = get_user(session)
        record_id = str(record['_id'])
        if record_id not in user['visited']:
            t_users.update_one({'_id': user['_id']},
                               {'$push': {'visited': str(record['_id'])}})

    schema = OutputRecordSchema(unknown=EXCLUDE, partial=True)
    return schema.dump(schema.load(record))


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


@app.get('/author/{author_id}')
async def query_author(author_id: int):
    results = t_authors.find_one({'_id': author_id})
    return results


class UserRegistrationModel(BaseModel):
    hashed_password: str
    email: str
    author_id: int


@app.put("/user/homepage")
async def register_user(req: UserRegistrationModel, resp: Response):
    same_email = t_users.find_one({'email': req.email})
    if same_email is not None:
        raise HTTPException(status_code=400,
                            detail="The email has been registered!")
    same_author = t_users.find_one({'author_id': req.author_id})
    if same_author is not None:
        raise HTTPException(status_code=400,
                            detail='The author has been registered!')

    password_salt = conf['gateway']['password_salt']
    req.hashed_password = hashlib.md5(
        (req.hashed_password + password_salt).encode()).hexdigest()

    new_user = req.dict()
    new_user['visited'] = []
    result = t_users.insert_one(new_user)
    user_id = str(result.inserted_id)

    sess_key = session_manager.new_item({'user_id': user_id})
    resp.set_cookie(key='session', value=sess_key,
                    expires=int(conf['gateway']['cookie_expires']))


class UserLoginModel(BaseModel):
    hashed_password: str
    email: str


@app.post("/user/homepage")
async def login_user(req: UserLoginModel, resp: Response):
    password_salt = conf['gateway']['password_salt']
    req.hashed_password = hashlib.md5(
        (req.hashed_password + password_salt).encode()).hexdigest()

    user = t_users.find_one({'email': req.email,
                             'hashed_password': req.hashed_password})
    if user is None:
        raise HTTPException(status_code=404,
                            detail='Invalid credentials')

    sess_key = session_manager.new_item({'user_id': str(user['_id'])})
    resp.set_cookie(key='session', value=sess_key,
                    expires=int(conf['gateway']['cookie_expires']))


@app.get("/user/homepage/logout")
async def logout_user():
    resp = RedirectResponse("/")
    resp.delete_cookie('session')
    return resp


def get_user(sess_key, raise_exc: bool = True):
    if sess_key:
        user_id = ObjectId(session_manager[sess_key]['user_id'])
        user = t_users.find_one({'_id': user_id})
    else:
        user = None
    if user is None:
        if raise_exc:
            raise HTTPException(status_code=404,
                                detail='Invalid session')
        else:
            return None
    return user


def get_user_id(sess_key, *args, **kwargs):
    user = get_user(sess_key, *args, **kwargs)
    if user is None:
        return None
    return str(user['_id'])


@app.put("/recommend/record/{key:path}")
async def request_recommend_records(key: str, session: Optional[str] = Cookie(None)):
    user = get_user(session, raise_exc=False)
    visited_ids = user['visited'] if user else None
    author_id = user['author_id'] if user else None
    record = _query_record(key)
    record_id = str(record['_id'])
    async_result = celery_app.send_task("recommender.recommend",
                                        args=(author_id, record_id, visited_ids))

    celery_app.send_task("recommender.clear_async_result",
                         args=(async_result.id,),
                         countdown=60).forget()
    return {'result_id': async_result.id}


@app.get("/recommend/result/{id}")
async def get_recommend_results(id: str):
    async_result = AsyncResult(id=id, app=celery_app)
    if async_result.state == 'SUCCESS':
        return {'status': 'ok', 'paper_ids': async_result.get()}
    return {'status': 'pending'}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app,
                host=conf['server']['host'],
                port=int(conf['server']['port']))
