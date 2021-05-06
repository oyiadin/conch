# coding=utf-8
import math

import numpy as np
import pickle
import time

import faiss
from typing import List, Tuple, Optional

import gensim.models.doc2vec
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_distances

from celery_workers.recommender import *

index: Optional[faiss.IndexIVFFlat] = None
index2doc_id = None
model: Optional[gensim.models.doc2vec.Doc2Vec] = None


@app.task(name="recommender.create_index")
def create_index(filename: str = None):
    global index
    if filename is None:
        filename = conf['faiss']['path']
    quantizer = faiss.IndexFlatL2(conf['faiss']['dimension'])
    index = faiss.IndexIVFFlat(quantizer,
                               conf['faiss']['dimension'],
                               conf['faiss']['nlist'],
                               faiss.METRIC_L2)
    index.train(pickle.load(open(filename, 'rb')))
    index.nprobe = conf['faiss']['nprobe']
    faiss.write_index(index, filename)


def build_corpus() -> List[Tuple[str, List[str]]]:
    corpus = []
    last_time = time.time()
    for n, record in enumerate(t_records.find(), start=1):
        text = record['title'] + ' ' + record['paperAbstract']
        tokens = gensim.utils.simple_preprocess(text)
        corpus.append((record['_id'], tokens))
        if n % 10000 == 0:
            logger.debug("[build corpus] Processed 10000 records within %.2f s",
                         time.time() - last_time)
            last_time = time.time()
    logger.debug("Corpus built")
    return corpus


def yield_tagged_document(corpus: List[Tuple[str, List[str]]]):
    for n, doc_id_and_doc in enumerate(corpus):
        _, doc = doc_id_and_doc
        yield gensim.models.doc2vec.TaggedDocument(doc, [n])


def yield_pop_corpus(corpus: List[Tuple[str, List[str]]]):
    while corpus:
        yield corpus.pop()


@app.task(name="recommender.process_database")
def task_process_database():
    global index, index2doc_id, model

    corpus = build_corpus()
    with open(conf['recommender']['corpus_path'], 'wb') as f:
        pickle.dump(corpus, f)
    logger.info("Corpus dumped to %s", conf['recommender']['corpus_path'])
    model = gensim.models.doc2vec.Doc2Vec(
        vector_size=int(conf['recommender']['vector_size']),
        epochs=int(conf['recommender']['epochs']),
        min_count=int(conf['recommender']['min_count']))
    model.build_vocab(yield_tagged_document(corpus))
    logger.debug("[doc2vec] Vocabulary built")
    model.train(yield_tagged_document(corpus),
                total_examples=model.corpus_count,
                epochs=int(conf['recommender']['epochs']))
    logger.debug("[doc2vec] Model trained")

    model.save(conf['recommender']['doc2vec_path'])
    logger.info("[doc2vec] Model saved to %s",
                conf['recommender']['doc2vec_path'])

    # corpus = pickle.load(open(conf['recommender']['corpus_path'], 'rb'))
    # model = gensim.models.doc2vec.Doc2Vec.load(
    #     conf['recommender']['doc2vec_path'])

    last_time = time.time()
    vectors = []
    index2doc_id = []
    for n, doc_id_and_doc in enumerate(yield_pop_corpus(corpus), start=1):
        doc_id, doc = doc_id_and_doc
        vec = model.infer_vector(doc)
        vectors.append(vec)
        index2doc_id.append(doc_id)
        t_records.update_one({'_id': doc_id},
                             {'$set': {'doc2vec': vec.tolist()}})
        if n % 10000 == 0:
            logger.debug("[save doc2vec vectors to database] Processed 10000 "
                         "records within %.2f s", time.time() - last_time)
            last_time = time.time()

    del corpus

    with open(conf['recommender']['vectors_path'], 'wb') as f:
        pickle.dump(vectors, f)
    with open(conf['recommender']['index_mappings_path'], 'wb') as f:
        pickle.dump(index2doc_id, f)

    logger.info("Vectors and index mappings dumped to %s; %s",
                conf['recommender']['vectors_path'],
                conf['recommender']['index_mappings_path'])

    # vectors = pickle.load(open(conf['recommender']['vectors_path'], 'rb'))
    # index2doc_id = pickle.load(open(conf['recommender']['index_mappings_path'], 'rb'))

    logger.debug("Building faiss index")
    quantizer = faiss.IndexFlatL2(int(conf['faiss']['dimension']))
    index = faiss.IndexIVFFlat(quantizer,
                               int(conf['faiss']['dimension']),
                               int(conf['faiss']['nlist']),
                               faiss.METRIC_L2)
    vectors = np.array(vectors)
    index.train(vectors)
    logger.debug("Faiss index trained")
    index.add(vectors)
    index.nprobe = int(conf['faiss']['nprobe'])
    faiss.write_index(index, conf['faiss']['path'])
    logger.debug("Faiss index saved to %s", conf['faiss']['path'])


@app.task(name="recommender.recommend")
def task_recommend(user_id: int, from_paper_id: str):
    author = t_authors.find_one({'_id': user_id})
    paper_ids = author['papers']

    interest_papers_vectors = []
    for paper_id in paper_ids:
        record = t_records.find_one({'_id': paper_id})
        if record is None:
            logger.error("Unable to find paper with _id=%s", paper_id)
            continue
        interest_papers_vectors.append(record['doc2vec'])
        citation_paper_ids = record['outCitations']
        for citation_paper_id in citation_paper_ids:
            cited_paper = t_records.find_one({'_id': citation_paper_id})
            if cited_paper is None:
                logger.warning("While finding cited papers, no such paper with "
                               "id %s", citation_paper_id)
                continue
            interest_papers_vectors.append(cited_paper['doc2vec'])

    interest_papers_centers = []
    interest_papers_vectors = np.array(interest_papers_vectors)
    clustering = DBSCAN(eps=0.5, min_samples=2).fit(interest_papers_vectors)
    core_samples_mask = np.zeros_like(clustering.labels_, dtype=bool)
    core_samples_mask[clustering.core_sample_indices_] = True
    unique_labels = set(clustering.labels_)
    for label in unique_labels:
        if label == -1:
            continue
        class_member_mask = (clustering.labels_ == label)
        interest_papers_centers.append(
            np.average(interest_papers_vectors[class_member_mask], axis=0))
    if len(interest_papers_centers) <= 2 and len(interest_papers_vectors) < 10:
        interest_papers_centers = interest_papers_vectors
    else:
        interest_papers_centers = np.array(interest_papers_centers)

    from_paper_vector = t_records.find_one({'_id': from_paper_id})['doc2vec']
    faiss_distances, faiss_indexes = index.search(
        np.array([from_paper_vector]).astype('float32'),
        int(conf['faiss']['search_top_k']))
    faiss_distances = faiss_distances[0]
    faiss_indexes = faiss_indexes[0]
    similar_paper_vectors = []
    similar_paper_ids = []
    for faiss_distance, faiss_index in zip(faiss_distances, faiss_indexes):
        faiss_paper_id = index2doc_id[faiss_index]
        if faiss_paper_id == from_paper_id:
            continue
        similar_paper_vectors.append(
            t_records.find_one({'_id': faiss_paper_id})['doc2vec'])
        similar_paper_ids.append(faiss_paper_id)
    similar_paper_vectors = np.array(similar_paper_vectors)

    distances = cosine_distances(similar_paper_vectors, interest_papers_centers)
    min_distances = np.min(distances, axis=1)

    final_paper_ids = []
    for sorted_index in min_distances.argsort():
        final_paper_ids.append(similar_paper_ids[sorted_index])

    return final_paper_ids


@app.task(name="recommender.load_from_disk")
def task_load_from_disk():
    global index, index2doc_id, model
    index = faiss.read_index(conf['faiss']['path'])
    index2doc_id = pickle.load(
        open(conf['recommender']['index_mappings_path'], 'rb'))
    model = gensim.models.doc2vec.Doc2Vec.load(
        conf['recommender']['doc2vec_path'])


if __name__ == '__main__':
    task_load_from_disk()
    task_recommend(46641658, '237e5a3778ccff06cd2d3e2d3f76eddf82913a7b')
    # task_process_database()
