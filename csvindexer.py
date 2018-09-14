# load csv data into elasticsearch
import os, fnmatch, argparse
from csv import DictReader
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from config import esconfig

DOC_TYPE = '_doc'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir_name', help='name of directory with csv')
    parser.add_argument('-i', '--index', help='elasticsearch index name(default=dir_name)')
    args = parser.parse_args()
    return args

def prospect(file_pat, root_dir):
    for path, dirlist, filelist in os.walk(root_dir):
        for name in fnmatch.filter(filelist, file_pat):
            yield os.path.join(path,name)

def harvest(file_names):
    for file_name in file_names:
        with open(file_name, 'r') as f:
            dictf = DictReader(f)
            for doc in dictf:
                yield doc

def make_actions(docs):
    for doc in docs:
        action =  { "_index": index_name, "_type": DOC_TYPE }
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
if args.index == None:
    index_name = args.dir_name
else:
    index_name = args.index
user, pwd = get_credentials()
# print(user, pwd)
es = get_clients(user,pwd)
# print(es.info())
""" for fname in find_files('*.json', 'sample'):
    print(fname)  """


actions = make_actions(harvest(prospect('*.csv', args.dir_name)))
b = bulk(es, actions)
