#!/bin/env python
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers.actions import bulk
from elasticsearch_dsl import Search, Index
from elasticsearch_dsl.analysis import Analyzer, analyzer, token_filter, tokenizer
from elasticsearch_dsl.document import Document
from elasticsearch_dsl.field import Text
from elasticsearch_dsl.query import MultiMatch
from pathlib import Path
import json


QueryId = int
"""Type alias for the id of a query"""

DocId = int
"""Type alias for the id of a document"""


def search(query: str, index_name: str, client: Elasticsearch) -> list[DocId]:
    """
    Search a given query on the summary and title fields of the given index.

    Returns the ids of the matching documents.
    """
    q = MultiMatch(query=query, fields=["summary", "title"])
    s = Search(using=client, index=index_name) \
        .query(q)
    hits = s.execute() \
        .hits.hits
    doc_ids = [int(hit['_id']) for hit in hits]
    return doc_ids


def read_docs() -> list[dict]:
    NDJSON_PATH = Path('data') / "cacm.v2.ndjson"

    with NDJSON_PATH.open() as f:
        lines = f.readlines()
        docs = [json.loads(l) for l in lines]

        return docs


def upload_documents(docs: list[dict], index: str, client: Elasticsearch):
    """
    Use bulk insert to upload the given documents to a specified index.

    Warning: Elastic may need a few second to asynchronously index after bulk insert.
    """
    successes, errors = bulk(
        client=client, index=index, actions=docs,
    )
    logging.info("Indexed %d/%d documents" % (successes, len(docs)))


def get_analyzers() -> list[Analyzer]:
    analyzers: list[Analyzer] = [
        analyzer('standard'),
        analyzer('whitespace'),
        analyzer('english'),
        analyzer('english_stop', type='english', stopwords_path="data/common_words.txt")
    ]
    return analyzers


def generate_index_name_from_analyzer(a: Analyzer) -> str:
    index_name = 'cacm_' + a._name
    return index_name
        

def create_indices(client: Elasticsearch) -> list[str]:
    """
    Create an index for each analyzers.
    """
    analyzers: list[Analyzer] = get_analyzers()
    docs = read_docs()

    for a in analyzers:
        index_name = generate_index_name_from_analyzer(a)

        index = Index(index_name)
        index.analyzer(a)

        # Specify mapping
        @index.document
        class Article(Document):
            title = Text(analyzer=a)
            summary = Text(analyzer=a)

        index.delete(ignore=404, using=client)
        index.create(using=client)

        upload_documents(docs, index_name, client)
        yield index_name


def get_index_names() -> list[str]:
    analyzers: list[Analyzer] = get_analyzers()
    index_names = [generate_index_name_from_analyzer(a) for a in analyzers]
    return index_names


def main():
    client = Elasticsearch()

    for index in create_indices(client):
        logging.info(f"Index {index} has been created.")


if __name__ == "__main__":
    # execute only if run as a script
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8',
                        level=logging.INFO)
    main()
