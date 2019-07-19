import argparse, json, csv, getpass, yaml
from pprint import pprint
from collections import OrderedDict
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from elasticsearch import Elasticsearch, RequestError
from elasticsearch.client import IndicesClient, CatClient
from elasticsearch_dsl import Search
from pandas import DataFrame

rows = []

def get_config(config_file):
    with open(config_file, 'r') as cf:
        config = yaml.load(cf, yaml.Loader)
        return config


def get_clients(esconfig):
    if esconfig['user']:
        es = Elasticsearch([esconfig['host']], 
                http_auth=(esconfig['user'], esconfig['pwd']), scheme='https')
    else:
        es = Elasticsearch([esconfig['host'])
    ic = IndicesClient(es)
    return es,ic


def get_indices(es1):
    cc = CatClient(es1)
    indices = cc.indices(format='json')
    indices = [index['index'] for index in indices if not index['index'].startswith('.')]
    return indices


def export_mapping(ic,index):
    if os.path.isdir('mappings'):
        os.makedir('mappings')
    mapping = ic.get_mapping(index=index)
    with open(f'mappings/{index}_mapping.json', 'w') as mf:
        json.dump(mapping[index], mf)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', help='name of the index')
    parser.add_argument('-c', '--config', help='cluster configuration file', required=True)
    args = parser.parse_args()
    return args


def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def add_field_stats(es, flat_schema):
    fields_list = []
    for field in flat_schema.keys():
        s = Search(using=es)
        s = s.query('exists', field=field)
        resp = s.execute()
        fields_list.append((field, flat_schema[field], resp.hits.total))
    return fields_list
    

def get_mapping(filename):
    with open(filename, 'r') as mapping:
        mapping = json.load(mapping, object_pairs_hook=OrderedDict)
        return mapping


def unpack(mapping):
    doc_type = list(mapping['mappings'].keys())[0]
    fields_dict = mapping['mappings'][doc_type]['properties']
    return fields_dict


def get_schema(fields_dict):
    schema = {}
    tlf = list(fields_dict.keys())
    for field in tlf:
        if 'type' in fields_dict[field].keys():
            schema[field] = fields_dict[field]['type']
        elif 'properties' in fields_dict[field].keys():
            schema[field] = get_schema(fields_dict[field]['properties'])
    return schema


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                for d in dict_generator(value, [key] + pre):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    for d in dict_generator(v, [key] + pre):
                        yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


def save_to_csv(fields_list, index):
    if not os.path.isdir('fields'):
        os.makedir('fields')
    with open(f'fields/{index}.csv', 'w') as of:
        csv_writer = csv.writer(of)
        csv_writer.writerow(('field name', 'field type', 'count'))
        csv_writer.writerows(fields_list)


def gen_fieldlist(index):
    print(f'exporting mappings for index {index}...')
    export_mapping(ic1,index)
    mapping = get_mapping(f'mappings/{index}_mapping.json')
    fields_dict = unpack(mapping)
    schema = get_schema(fields_dict)
    flat_schema = flatten_json(schema)
    fields_list = add_field_stats(es1, flat_schema)
    save_to_csv(fields_list, index)   


args = parse_args()
config = get_config(args.config)
src = config['clusters']['src']
if 'dest' in config['clusters']:
    dest = config['clusters']['dest']
print('connecting to es1...')
src['pwd'] = getpass.getpass('enter password > ')
es1, ic1 = get_clients(src)
if args.index: 
    gen_fieldslist(args.index)
else:
    indices = get_indices(es1)
    for index in indices:
        gen_fieldslist(index)
