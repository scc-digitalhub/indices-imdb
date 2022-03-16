import http.client
import json
import os


class Processor:

    def __init__(self, api, parser, data_dir="./data"):
        self.api_name = api['name']
        self.api_path = api['path']
        self.api_content = False
        if 'content' in api:
            self.api_content = api['content']
        self.parser = parser
        self.data_dir = data_dir

    def build_basedir(self, action):
        return self.data_dir+'/'+action

    def build_path(self, action, tconst):
        basedir = self.build_basedir(action)+'/'+self.api_name
        key = format(int(tconst[2:]), '08d')
        # reverse the id to obtain a partitionable key
        tpath = key[6:8]+'/'+key[4:6]+'/'+key[2:4]
        return basedir+'/'+tpath

    def read_json(self, action, tconst):
        basedir = self.build_path(action, tconst)
        inpath = basedir+'/'+tconst+'.json'
        if not os.path.exists(basedir):
            raise Exception("missing data dir {}".format(basedir))
        data = False
        with open(inpath, 'r') as infile:
            print('\t read from {}'.format(inpath))
            data = json.load(infile)
        return data

    def write_json(self, action, tconst, data):
        basedir = self.build_path(action, tconst)
        if not os.path.exists(basedir):
            print('\t mkdirs {}'.format(basedir))
            os.makedirs(basedir)
        outpath = basedir + '/'+tconst + '.json'
        with open(outpath, 'w') as outfile:
            print('\t write to {}'.format(outpath))
            json.dump(data, outfile)

    def process_title(self, tconst, title):
        try:
            print('\t read api for {} on tconst {}'.format(self.api_name, tconst))
            js = self.read_json('download', tconst)
            if self.api_content:
                if not self.api_content in js:
                    raise Exception(
                        'missing resource for {}, skip.'.format(tconst))
            print('\t process response for {} on tconst {}'.format(
                self.api_name, tconst))
            result = self.parser.parse_title_data(tconst,  title, js)
            print('\t write result for {} on tconst {}'.format(
                self.api_name, tconst))
            self.write_json('process', tconst, result)
            return result
        except Exception as err:
            print('\t error on {}: {}'.format(tconst, err))
            raise err

    def process(self, titles):
        print('\t process api {} on {} titles'.format(
            self.api_name, titles.len()))
        for title in titles:
            try:
                tconst = title['tconst']
                self.process_title(tconst, title)
                print('\t done {}'.format(tconst))
            except Exception as err:
                print('\t skip {} for error: {}'.format(tconst, err))
