# load csv data into elasticsearch
import os, fnmatch, argparse
from csv import DictReader
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from config import esconfig

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='name of the csv file')
    parser.add_argument('index', help='elasticsearch index name')
    args = parser.parse_args()
    return args

def docs(file_name):
    with open(file_name, 'r') as f:
        dictf = DictReader(f)
        for doc in dictf:
            yield doc

def make_actions(docs, index_name):
    for doc in docs:
        action =  { "_index": index_name }
        action["_source"] = doc
        yield action

def get_credentials():
    user = input('Username: ')
    if user:
        pwd = getpass.getpass()
        return (user, pwd)
    else:
        return(None,None)

def get_clients(user,pwd):
    host = esconfig[0]['host']
    port = esconfig[0]['port']
    if user:
        es = Elasticsearch([host], http_auth=(user,pwd), port=port)
    else:
        es = Elasticsearch(esconfig)
    return es

args = parse_args()
user, pwd = get_credentials()
es = get_clients(user,pwd)
actions = make_actions(docs(args.file), args.index)
b = bulk(es, actions)
