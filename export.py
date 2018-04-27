import argparse, json, getpass
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from config import esconfig
from elasticsearch.client import IndicesClient

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('index', help='name of the index')
    parser.add_argument('type', help='document type')
    parser.add_argument('-f', '--format', choices={'json', 'bulk'}, default='bulk', help='output format(default=bulk)')
    parser.add_argument('-s', '--settings', action='store_true', help='export settings')
    args = parser.parse_args()
    return args

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
    ic = IndicesClient(es)
    return es,ic

def export_mapping(ic,index_name):
    mapping = ic.get_mapping(index=args.index)
    with open(index_name + '_mapping.json', 'w') as mf:
        json.dump(mapping[index_name], mf)

def export_settings(ic,index_name):
    settings = ic.get(index=args.index)
    with open(index_name + '_settings.json', 'w') as sf:
        json.dump(settings[index_name], sf)

def bulk_export(es,index_name, doc_type):
    s = scan(es, index=index_name, doc_type=doc_type)
    with open(index_name + '_bulk.json', 'w') as f:
        for doc in s:
            preamble = {'index': {'_id': doc['_id']}}
            f.write(json.dumps(preamble))
            f.write("\n")
            f.write(json.dumps(doc['_source']))
            f.write("\n")

def json_export(es, index_name, doc_type):
    s = scan(es, index=index_name, doc_type=doc_type)
    json_data = [doc['_source'] for doc in s]
    with open(args.index + '.json', 'w') as json_file:
        json.dump(json_data, json_file)

args = parse_args()
user, pwd = get_credentials()
es,ic = get_clients(user,pwd)
if args.settings:
    export_settings(ic, args.index)
if args.format == 'bulk':
    bulk_export(es, args.index, args.type)
elif args.format == 'json':
    json_export(es, args.index, args.type)
