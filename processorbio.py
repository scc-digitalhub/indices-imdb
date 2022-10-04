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

    def build_path(self, action, nconst):
        basedir = self.build_basedir(action)+'/'+self.api_name
        key = format(int(nconst[2:]), '08d')
        # reverse the id to obtain a partitionable key
        tpath = key[6:8]+'/'+key[4:6]+'/'+key[2:4]
        return basedir+'/'+tpath

    def read_json(self, action, nconst):
        basedir = self.build_path(action, nconst)
        inpath = basedir+'/'+nconst+'.json'
        if not os.path.exists(basedir):
            raise Exception("missing data dir {}".format(basedir))
        data = False
        with open(inpath, 'r') as infile:
            print('\t read from {}'.format(inpath))
            data = json.load(infile)
        return data

    def write_json(self, action, nconst, data):
        basedir = self.build_path(action, nconst)
        if not os.path.exists(basedir):
            print('\t mkdirs {}'.format(basedir))
            os.makedirs(basedir)
        outpath = basedir + '/'+nconst + '.json'
        with open(outpath, 'w') as outfile:
            print('\t write to {}'.format(outpath))
            json.dump(data, outfile)

    def process_bio(self, nconst):
        try:
            print('\t read api for {} on nconst {}'.format(self.api_name, nconst))
            js = self.read_json('download', nconst)
            #if self.api_content:
             #   if not self.api_content in js:
              #      raise Exception(
               #         'missing resource for {}, skip.'.format(nconst))
            print('\t process response for {} on nconst {}'.format(
                self.api_name, nconst))
            result = self.parser.parse_bio_data(nconst, js)
            print('\t write result for {} on nconst {}'.format(
                self.api_name, nconst))
            self.write_json('process', nconst, result)
            return result
        except Exception as err:
            print('\t error on {}: {}'.format(nconst, err))
            raise err

    def process(self, nconsts):
        print('\t process api {} on {} nconsts'.format(
            self.api_name, nconsts.len()))
        for i in nconsts:
            try:
                nconst = i['nconst']
                self.process_bio(nconst)
                print('\t done {}'.format(nconst))
            except Exception as err:
                print('\t skip {} for error: {}'.format(nconst, err))
