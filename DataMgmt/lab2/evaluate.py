#!/bin/env python
import logging
import pprint
from pathlib import Path
from collections import defaultdict
from types import SimpleNamespace
import matplotlib.pyplot as plt

from elasticsearch import Elasticsearch, client
from elasticsearch.client import indices
from elasticsearch.helpers import streaming_bulk
from elasticsearch.helpers.actions import bulk
from elasticsearch_dsl import Search, query, Index
from elasticsearch_dsl.analysis import Analyzer, analyzer, token_filter, tokenizer
from elasticsearch_dsl.document import Document
from elasticsearch_dsl.field import Text
from elasticsearch_dsl.query import MultiMatch

from index import get_index_names, search, QueryId, DocId


class RankingMetrics(SimpleNamespace):
    """
    Simple class to store the metrics of an index.

    This class does not store metrics for individual queries.
    """
    index_name: str

    total_retrieved_docs: int = 0 
    """Sum of the number retrieved documents of each query"""

    total_relevant_docs: int = 0
    """Sum of the number of relevant documents for each query"""

    total_retrieved_relevant_docs: int = 0
    """Sum of the number of relevant documents retrieved for each query"""

    avg_precision: float = 0.0
    """Average of the precision of each query"""

    avg_recall: float = 0.0
    """Average of the recall of each query"""

    f_measure: float = 0.0
    """F-Measure calculated from avg_precision and avg_recall"""

    mean_average_precision: float = 0.0
    """Mean of the average precision (AP) of each query"""

    avg_r_precision: float = 0.0
    """Average of the R-Precision of each query"""

    avg_precision_at_recall_level: list[float] = [0.0] * 11
    """Average of the precision at the 11 standard recall levels of each query"""


def evaluate_index(index_name: str, 
                   queries: dict[QueryId, str], 
                   qrels: dict[QueryId, set[DocId]], 
                   client: Elasticsearch):
    # Initialize metrics for the current index evaluation
    m = RankingMetrics()
    m.index_name = index_name
    # TODO: for each query calculate the metrics and then summarize them in m
    matching_queries = []
    matching_qrels = []

    # query_results = {}
    precision_sum = 0.0
    recall_sum = 0.0
    avg_r_precision_sum = 0.0


    for q, q_str in queries.items():
        res = search(q_str, index_name, client)
        matching_queries+= res
        matching_qrels+= qrels[q]
        precision_sum+= len(list(set(qrels[q]) & set(res))) / len(res)
        if len(qrels[q]):
            recall_sum+= len(list(set(qrels[q]) & set(res))) / len(qrels[q])
            R_doc = len(qrels[q])
            res = res[:R_doc]
            avg_r_precision_sum  += len(list(set(qrels[q]) & set(res))) / R_doc
    
    # query_results = {q: search(q, index_name, client) for q in queries.keys()} # dictionnaire clé : id de la query, valeur : liste des queries
    # [query_total := query_total + len(q) for q in query_results.values()] # total de queries
    
    

    # qrel_results = {q: qrels[q] for q in query_results.keys()} # dict clé : id de la query, val : set des qrels
    # [qrel_total := qrel_total + len(q) for q in qrel_results.values()] # total qrels

    # [precision_sum:= precision_sum + len(qrel_results[k])/query_total for k in queries.keys()]

    # [[recall_sum := recall_sum + len(qrel_results[k])/len(matching_qrels)] for k in queries.keys()]

    m.total_retrieved_docs = len(matching_queries)
    m.total_relevant_docs = len(matching_qrels)
    m.total_retrieved_relevant_docs =  len(list(set(matching_queries) & set(matching_qrels)))
    m.avg_precision = precision_sum / len(queries)
    m.avg_recall = recall_sum / len(queries)
    m.avg_r_precision = avg_r_precision_sum/ len(queries)
    m.f_measure = (2*m.avg_precision*m.avg_recall)/(m.avg_recall+m.avg_precision)
    return m


def read_queries() -> dict[QueryId, str]:
    """
    Returns a dictionary that contains for each query id the query phrase.
    """
    QUERY_SEPARATOR = "\t"
    QUERIES_PATH = Path('evaluation') / "query.txt"
    
    with QUERIES_PATH.open() as f:
        lines = [line.strip() for line in f.readlines()]
        parsed = [line.split(QUERY_SEPARATOR) for line in lines if line != ""]
        queries = {int(p[0]): p[1] for p in parsed}
        return queries


def read_qrels() -> dict[QueryId, set[DocId]]:
    """
    Returns a dictionary that contains for each query id the set of relevant document ids.
    When accessing a unknown query id, the dictionary returns an empty set.
    """
    QREL_SEPARATOR = ";"
    DOC_SEPARATOR = ","
    QRELS_PATH = Path('evaluation') / "qrels.txt"

    with QRELS_PATH.open() as f:
        lines = [line.strip() for line in f.readlines()]
        parsed = [line.split(QREL_SEPARATOR) for line in lines if line != ""]
        qrels = defaultdict(set)
        for p in parsed:
            query_id = int(p[0])
            docs = set(int(d) for d in p[1].split(DOC_SEPARATOR))
            qrels[query_id] = docs
        return qrels


def main():
    queries = read_queries()
    qrels = read_qrels()

    client = Elasticsearch()

    # If you want to evaluate manualy created indices, you can
    # list their names here.
    manual_indices = []

    indices = get_index_names()
    indices.extend(manual_indices)

    with open("metrics.txt", "w") as metric_fp:
        for index in indices:
            metrics = evaluate_index(index, queries, qrels, client)
            pprint.pprint(metrics, metric_fp)
            pprint.pprint(metrics)  # Also print to stdout


if __name__ == "__main__":
    # execute only if run as a script
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8',
                        level=logging.WARN)
    main()
