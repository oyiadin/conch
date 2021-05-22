# coding=utf-8
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
index2word_id: Optional[List] = None
wv: Optional[gensim.models.KeyedVectors] = None


class yield_corpus:
    def __init__(self, return_id: bool = False):
        self.return_id = return_id

    def __iter__(self):
        self.cursor = t_records.find()
        self.n = 1
        self.last_time = time.time()
        return self

    def __next__(self):
        while True:
            record = next(self.cursor)
            words = record.get('outCitations')
            if not words:
                continue
            if self.n % 10000 == 0:
                logger.debug("[iter corpus] Processed 10000 records within %.2f s",
                             time.time() - self.last_time)
                self.last_time = time.time()
            self.n += 1
            if self.return_id:
                return record['_id'], words
            return words


@app.task(name="recommender.process_database")
def task_process_database():
    global index, index2word_id, wv

    corpus = yield_corpus()
    model = gensim.models.word2vec.Word2Vec(
        sentences=iter(corpus),
        size=int(conf['recommender']['vector_size']),
        workers=int(conf['recommender']['workers']),
        min_count=int(conf['recommender']['min_count']))
    model.train(iter(corpus),
                total_examples=model.corpus_count,
                epochs=int(conf['recommender']['epochs']))
    del corpus
    logger.debug("[word2vec] Model trained")

    wv = model.wv
    del model
    wv.save(conf['recommender']['word2vec_wv_path'])
    logger.info("[word2vec] word_vectors saved to %s",
                conf['recommender']['word2vec_wv_path'])

    # model = gensim.models.doc2vec.Doc2Vec.load(
    #     conf['recommender']['word2vec_wv_path'])

    wv.init_sims()
    index2word_id = wv.index2word

    logger.debug("Building faiss index")
    quantizer = faiss.IndexFlatL2(int(conf['faiss']['dimension']))
    index = faiss.IndexIVFFlat(quantizer,
                               int(conf['faiss']['dimension']),
                               int(conf['faiss']['nlist']),
                               faiss.METRIC_L2)
    vectors = wv.vectors_norm
    index.train(vectors)
    logger.debug("Faiss index trained")
    index.add(vectors)
    index.nprobe = int(conf['faiss']['nprobe'])
    faiss.write_index(index, conf['faiss']['path'])
    logger.debug("Faiss index saved to %s", conf['faiss']['path'])


@app.task(name="recommender.recommend")
def task_recommend(author_id: Optional[str], from_paper_id: str):
    try:
        from_paper_vector = wv.word_vec(from_paper_id, use_norm=True)
    except KeyError:
        return None

    user_profile_vector = None  # to make PyCharm happy
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
                vec = wv.word_vec(paper_id, use_norm=True)
                interest_papers_vectors.append(vec)
            except KeyError:
                continue

            citation_paper_ids = record['outCitations']
            for citation_paper_id in citation_paper_ids:
                try:
                    vec = wv.word_vec(citation_paper_id, use_norm=True)
                    interest_papers_vectors.append(vec)
                except KeyError:
                    continue

        interest_papers_centers = []
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
        faiss_paper_id = index2word_id[faiss_index]
        if faiss_paper_id == from_paper_id:
            continue
        vec = wv.word_vec(faiss_paper_id, use_norm=True)
        similar_paper_vectors.append(vec)
        similar_paper_ids.append(faiss_paper_id)
    similar_paper_vectors = np.array(similar_paper_vectors)

    if author_id:
        # TODO: 优化排序
        distances = cosine_distances(
            similar_paper_vectors, user_profile_vector)
        final_paper_ids = []
        for sorted_index in distances[..., 0].argsort():
            final_paper_ids.append(similar_paper_ids[sorted_index])
    else:
        final_paper_ids = similar_paper_ids

    return final_paper_ids


@app.task(name="recommender.load_from_disk")
def task_load_from_disk():
    global index, index2word_id, model
    index = faiss.read_index(conf['faiss']['path'])
    wv = gensim.models.KeyedVectors.load(
        conf['recommender']['word2vec_wv_path'])
    index2word_id = wv.index2word


@app.task(name="recommender.clear_async_result")
def task_clear_async_result(id):
    async_result = AsyncResult(id=id, app=app)
    async_result.forget()


if __name__ == '__main__':
    # task_load_from_disk()
    # task_recommend(46641658, '237e5a3778ccff06cd2d3e2d3f76eddf82913a7b')
    task_process_database()
