import argparse, json, csv, getpass, yaml
from pprint import pprint
from collections import OrderedDict
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from elasticsearch import Elasticsearch, RequestError
from elasticsearch.client import IndicesClient
from elasticsearch_dsl import Search
from pandas import DataFrame

rows = []

def get_config(config_file):
    with open(config_file, 'r') as cf:
        config = yaml.load(cf, yaml.Loader)
        return config



def get_clients(esconfig, user=None,pwd=None):
    host = esconfig['host']
    if user:
        es = Elasticsearch([host], http_auth=(user,pwd))
    else:
        es = Elasticsearch(esconfig)
    ic = IndicesClient(es)
    return es,ic

def export_mapping(ic,index_name):
    mapping = ic.get_mapping(index=args.index)
    with open(f'{index_name}_mapping.json', 'w') as mf:
        json.dump(mapping[index_name], mf)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help='mapping file input')
    parser.add_argument('-i', '--index', required=True, help='name of the index')
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

def get_ecs_fields():
    with open('ecs_fields.csv', 'r') as ff:
        ff_csv = csv.reader(ff)
        header = next(ff_csv)
        fields = [column[0] for column in ff_csv]
        return fields

def get_field_map(ecs_fields, fields):
    field_map = {}
    ecs_completer = WordCompleter(ecs_fields, sentence=True)
    for field in fields:
        field_map[field] = prompt(field + ' > ', completer = ecs_completer)
    return field_map

def export_ecsmap(ecsmap):
    with open(f'{index}_ecsmap.json', 'w') as emf:
        json.dump(ecsmap, emf)

def generate_ecs_mappings(ecsmap):
    with open(f'{args.index}_mapping.json', 'r') as omf:
        omd = json.load(omf)

def findnreplace(rmap, mapping):
    mapping_json = json.dumps(mapping)
    for k, v in rmap.items():
        mapping_json = mapping_json.replace(k,v)
    return json.loads(mapping_json)

def create_ecsindex(ic, new_mapping):
    try:
        resp = ic.create(index=f'{args.index}_ecs', body=new_mapping)
    except RequestError as e:
        print('target index already exists')
        print(e)

def save_to_csv(flat_schema):
    with open(f'{args.index}.csv', 'w') as of:
        csv_writer = csv.writer(of)
        csv_writer.writerow(('field name', 'field type', 'count'))
        csv_writer.writerows(fields_list)

args = parse_args()
config = get_config(args.config)
if config['clusters']['dest']:
    src = config['clusters']['src']
    dest = config['clusters']['dest']
print('connecting to es1...')
src['pwd'] = getpass.getpass('enter password > ')
es1, ic1 = get_clients(src['host'], src['user'],src['pwd'])
print(f'exporting mappings for index {args.index}...')
export_mapping(ic1,args.index)
mapping = get_mapping(f'{args.index}_mapping.json')
fields_dict = unpack(mapping)
schema = get_schema(fields_dict)
flat_schema = flatten_json(schema)
fields_list = add_field_stats(es1, flat_schema)
save_to_csv(fields_list)
#ecs_fields = get_ecs_fields()
#ecsmap = get_field_map(ecs_fields, flat_schema.keys())
#export_ecsmap(ecsmap)
#pprint(ecsmap)
#new_mapping = findnreplace(ecsmap, mapping)
#print(new_mapping)
#resp = ic.create(index=f'{args.index}_ecs', body=new_mapping)
#create_ecsindex(ic, new_mapping)
