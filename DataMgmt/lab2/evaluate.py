#!/bin/env python
import logging
import pprint
from pathlib import Path
from collections import defaultdict
from types import SimpleNamespace
import numpy as np

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


def get_max_precision_recall(recalls, recall):
    max = 0
    for k in recalls:
        if k[0] == recall and k[1] > max:
            max = k[1]
            break
    return max

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
    avg_p_for_each_query= []
    recall_for_each_query = []
    precision_sum = 0.0
    recall_sum = 0.0
    avg_r_precision_sum = 0.0
    
    for q, q_str in queries.items():
        res = search(q_str, index_name, client)
        count_nbr_of_doc_correct = 1
        avg_prec = []
        recalls = []
        for i in range(1, len(res)+2):
            avg_prec.append(count_nbr_of_doc_correct/i)
            if qrels[q]:
                recall = count_nbr_of_doc_correct/len(qrels[q])
            else:
                if i==1:
                    recall = 0
                else:
                    recall = recalls[i-2]

            recalls.append((recall, count_nbr_of_doc_correct/i))
            if res[i-2] in qrels[q]:
                count_nbr_of_doc_correct += 1
            
        avg_p_for_each_query.append(sum(avg_prec)/len(avg_prec))
        recall_for_each_query.append(recalls)
        matching_queries+= res
        matching_qrels+= qrels[q]
        precision_sum+= len(list(set(qrels[q]) & set(res))) / len(res)
        if len(qrels[q]):
            recall_sum+= len(list(set(qrels[q]) & set(res))) / len(qrels[q])
            R_doc = len(qrels[q])
            res = res[:R_doc]
            avg_r_precision_sum  += len(list(set(qrels[q]) & set(res))) / R_doc

    global_prec_recall = []
    
    for k in recall_for_each_query:
        prec_recall = []
        for i in range(11):
            max_prec_recall = get_max_precision_recall(k, k[i][0])
            prec_recall.append((i/10, max_prec_recall))
        global_prec_recall.append(prec_recall)
    
    mean_prec_recall = []

    for i in range(11):
        sum_prec = 0 
        for q in global_prec_recall:
            sum_prec += q[i][1]
        mean_prec_recall.append(sum_prec/len(queries))
    
    m.total_retrieved_docs = len(matching_queries)
    m.total_relevant_docs = len(matching_qrels)
    m.total_retrieved_relevant_docs =  len(list(set(matching_queries) & set(matching_qrels)))
    m.avg_precision = precision_sum / len(queries)
    m.avg_recall = recall_sum / len(queries)
    m.f_measure = (2*m.avg_precision*m.avg_recall)/(m.avg_recall+m.avg_precision)
    m.mean_average_precision = sum(avg_p_for_each_query)/len(avg_p_for_each_query)
    m.avg_r_precision = avg_r_precision_sum/ len(queries)
    m.avg_precision_at_recall_level = mean_prec_recall
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
