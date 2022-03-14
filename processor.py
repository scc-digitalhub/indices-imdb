import http.client
import json
import os


class Processor:

    def __init__(self, api, parser, data_dir="./data"):
        self.api_name = api['name']
        self.api_path = api['path']
        self.parser = parser
        self.data_dir = data_dir

    def build_path(self, tconst):
        basedir = self.data_dir+'/'+self.api_name
        key = format(int(tconst[2:]), '08d')
        # reverse the id to obtain a partitionable key
        tpath = key[6:8]+'/'+key[4:6]+'/'+key[2:4]
        return basedir+'/'+tpath

    def read_json(self, tconst):
        basedir = self.build_path(tconst)
        inpath = basedir+'/'+tconst+'.json'
        if not os.path.exists(basedir):
            raise Exception("missing data dir {}".format(basedir))
        data = False
        with open(inpath, 'r') as infile:
            print('read from {}'.format(inpath))
            data = json.load(infile)
        return data

    def process_title(self, tconst, title):
        try:
            print('read api for {} on tconst {}'.format(self.api_name, tconst))
            js = self.read_json(tconst)
            valid_keys = ['cast', 'resource']
            keys = [k for k in valid_keys if k in js]
            if not keys:
                raise Exception(
                    'missing resource for {}, skip.'.format(tconst))
            print('process response for {} on tconst {}'.format(
                self.api_name, tconst))
            return self.parser.parse_title_data(tconst,  title, js)
        except Exception as err:
            print('error on {}: {}'.format(tconst, err))
            raise err

    # def process(self, titles):
    #     print('process api {} on {} titles'.format(self.api_name, titles.len()))
    #     for title in titles:
    #         try:
    #             tconst = title['tconst']
    #             self.process_title(tconst, title)
    #             print('done {}'.format(tconst))
    #         except Exception as err:
    #             print('skip {} for error: {}'.format(tconst, err))
