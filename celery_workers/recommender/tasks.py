# coding=utf-8
import glob
import gzip
import json
import os
import random

import numpy as np
import pickle
import time

import faiss
from typing import List, Optional, Iterator, Dict

import gensim.models.doc2vec
from celery.result import AsyncResult
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_distances

from celery_workers.recommender import *

index: Optional[faiss.IndexIVFFlat] = None
wv: Optional[gensim.models.KeyedVectors] = None


class yield_corpus:
    def __init__(self,
                 return_id: bool = False,
                 check_word_id: bool = True):
        self.return_id = return_id
        self.check_word_id = check_word_id
        self.cursor = None
        self.first_run = True

    def __iter__(self):
        self.cursor = t_records.find()
        if self.first_run:
            self.first_run = False
        else:
            self.check_word_id = False
        return self.generator()

    def generator(self):
        n = 0
        last_time = time.time()
        for record in self.cursor:
            n += 1
            if n % 100000 == 0:
                logger.debug("[iter corpus] Processed 100000 records within %.2f s",
                             time.time() - last_time)
                last_time = time.time()

            if 'Computer Science' not in record['fieldsOfStudy']:
                continue
            raw_words = record.get('outCitations') or record.get('inCitations')
            if not raw_words or len(raw_words) < 4:
                continue
            raw_words = random.sample(raw_words, k=4)
            raw_words.append(record['_id'])
            random.shuffle(raw_words)

            if self.return_id:
                yield record['_id'], raw_words
            yield raw_words


@app.task(name="recommender.process_database")
def task_process_database():
    global index, wv

    model = gensim.models.word2vec.Word2Vec(
        sentences=yield_corpus(),
        vector_size=int(conf['recommender']['vector_size']),
        workers=int(conf['recommender']['workers']),
        min_count=int(conf['recommender']['min_count']),
        epochs=int(conf['recommender']['epochs']))
    # model.train(iter(corpus),
    #             total_examples=model.corpus_count,
    #             epochs=int(conf['recommender']['epochs']))
    logger.debug("[word2vec] Model trained")

    wv = model.wv
    del model
    wv.save(conf['recommender']['word2vec_wv_path'])
    logger.info("[word2vec] word_vectors saved to %s",
                conf['recommender']['word2vec_wv_path'])

    # model = gensim.models.doc2vec.Doc2Vec.load(
    #     conf['recommender']['word2vec_wv_path'])

    wv.init_sims()

    logger.debug("Building faiss index")
    quantizer = faiss.IndexFlatL2(int(conf['faiss']['dimension']))
    index = faiss.IndexIVFFlat(quantizer,
                               int(conf['faiss']['dimension']),
                               int(conf['faiss']['nlist']),
                               faiss.METRIC_L2)
    vectors = wv.get_normed_vectors()
    index.train(vectors)
    logger.debug("Faiss index trained")
    index.add(vectors)
    index.nprobe = int(conf['faiss']['nprobe'])
    faiss.write_index(index, conf['faiss']['path'])
    logger.debug("Faiss index saved to %s", conf['faiss']['path'])


def normalize(arr: np.ndarray, inplace: bool = False):
    norm = np.linalg.norm(arr)
    if norm == 0:
        return arr

    if not inplace:
        return arr / norm
    else:
        arr /= norm
        return arr


@app.task(name="recommender.recommend")
def task_recommend(author_id: Optional[str],
                   from_paper_id: str,
                   visited_ids: List[str]):
    try:
        from_paper_vector = wv.get_vector(from_paper_id, norm=True)
    except:
        return None

    interest_papers_vectors = []  # to make PyCharm happy
    if author_id:
        author = t_authors.find_one({'_id': author_id})
        paper_ids = author['papers']
        interest_papers_vectors = []
        for paper_id in paper_ids:
            record = t_records.find_one({'_id': paper_id})
            if record is None:
                logger.error("Unable to find paper with _id=%s", paper_id)
                continue
            try:
                vec = wv.get_vector(paper_id, norm=True)
                interest_papers_vectors.append(vec)
            except KeyError:
                continue

            citation_paper_ids = record['outCitations']
            for citation_paper_id in citation_paper_ids:
                try:
                    vec = wv.get_vector(citation_paper_id, norm=True)
                    interest_papers_vectors.append(vec)
                except KeyError:
                    continue

    user_profile_vector = None  # to make PyCharm happy
    if author_id or visited_ids:
        interest_papers_centers = []
        if visited_ids:
            for paper_id in visited_ids:
                try:
                    vec = wv.get_vector(paper_id, norm=True)
                    interest_papers_vectors.append(vec)
                except KeyError:
                    continue
        interest_papers_vectors = np.array(interest_papers_vectors)
        clustering = DBSCAN(eps=float(conf['dbscan']['eps']),
                            min_samples=int(conf['dbscan']['min_samples'])
                            ).fit(interest_papers_vectors)
        core_samples_mask = np.zeros_like(clustering.labels_, dtype=bool)
        core_samples_mask[clustering.core_sample_indices_] = True
        unique_labels = set(clustering.labels_)
        for label in unique_labels:
            if label == -1:
                continue
            class_member_mask = (clustering.labels_ == label)
            interest_papers_centers.append(
                np.average(interest_papers_vectors[class_member_mask], axis=0))
        if len(interest_papers_centers) <= 2:
            interest_papers_centers = interest_papers_vectors
        else:
            interest_papers_centers = np.array(interest_papers_centers)

        user_profile_vector = np.mean(interest_papers_centers,
                                      axis=0, keepdims=True)

    faiss_distances, faiss_indexes = index.search(
        np.array([from_paper_vector]).astype('float32'),
        int(conf['faiss']['search_top_k']))
    faiss_distances = faiss_distances[0]
    faiss_indexes = faiss_indexes[0]
    similar_paper_vectors = []
    similar_paper_ids = []
    for faiss_distance, faiss_index in zip(faiss_distances, faiss_indexes):
        faiss_paper_id = wv.index_to_key[faiss_index]
        vec = wv.get_vector(faiss_paper_id, norm=True)
        similar_paper_vectors.append(vec)
        similar_paper_ids.append(faiss_paper_id)
    similar_paper_vectors = np.array(similar_paper_vectors)

    if author_id or visited_ids:
        user_profile_distances = cosine_distances(
            similar_paper_vectors, user_profile_vector)[..., 0]
        final_paper_ids = []
        faiss_distance_weight = float(conf['recommender']['faiss_distance_weight'])
        normalized_faiss_distances = normalize(faiss_distances)
        user_profile_distance_weight = float(conf['recommender']['user_profile_distance_weight'])
        normalized_user_profile_distances = normalize(user_profile_distances)
        weighted_distances = \
            faiss_distance_weight * normalized_faiss_distances \
            + user_profile_distance_weight * normalized_user_profile_distances
        for sorted_index in weighted_distances.argsort():
            final_paper_ids.append(similar_paper_ids[sorted_index])
    else:
        final_paper_ids = similar_paper_ids

    from_paper_index = final_paper_ids.index(from_paper_id)
    if from_paper_index != -1:
        final_paper_ids.pop(from_paper_index)

    return final_paper_ids


@app.task(name="recommender.load_from_disk")
def task_load_from_disk():
    global index, wv
    index = faiss.read_index(conf['faiss']['path'])
    # noinspection PyTypeChecker
    wv = gensim.models.KeyedVectors.load(
        conf['recommender']['word2vec_wv_path'])  # type: gensim.models.KeyedVectors


@app.task(name="recommender.clear_async_result")
def task_clear_async_result(task_id):
    async_result = AsyncResult(id=task_id, app=app)
    async_result.forget()


if __name__ == '__main__':
    # task_load_from_disk()
    # task_recommend(46641658, '237e5a3778ccff06cd2d3e2d3f76eddf82913a7b')
    task_process_database()
