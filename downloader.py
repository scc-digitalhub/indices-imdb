import http.client
import json
import os


class Downloader:

    def __init__(self, rapidapi_config, api, data_dir="./data"):
        self.rapidapi_host = rapidapi_config['host']
        self.rapidapi_key = rapidapi_config['key']
        self.api_name = api['name']
        self.api_path = api['path']
        self.data_dir = data_dir

    def call_api(self, tconst):
        conn = http.client.HTTPSConnection(self.rapidapi_host)
        headers = {
            'x-rapidapi-host': self.rapidapi_host,
            'x-rapidapi-key': self.rapidapi_key
        }
        url = self.api_path.format(tconst)
        print('call get on {} tconst {}'.format(url, tconst))
        conn.request("GET", url, headers=headers)
        res = conn.getresponse()
        body = res.read()
        data = (body.decode("utf-8"))
        return json.loads(data)

    def write_json(self, tconst, data):
        basedir = self.data_dir+'/'+self.api_name
        outpath = basedir+'/'+tconst+'.json'
        if not os.path.exists(basedir):
            print('mkdirs {}'.format(basedir))
            os.makedirs(basedir)
        with open(outpath, 'w') as outfile:
            print('write to {}'.format(outpath))
            json.dump(data, outfile)

    def download_title(self, tconst):
        try:
            print('call api for {} on tconst {}'.format(self.api_name, tconst))
            js = self.call_api(tconst)
            valid_keys = ['cast', 'resource']
            keys = [k for k in valid_keys if k in js]
            if not keys:
                print("debug {}".format(js))
                raise Exception(
                    'missing resource for {}, skip.'.format(tconst))
            print('save response for {} on tconst {}'.format(
                self.api_name, tconst))
            self.write_json(tconst, js)
        except Exception as err:
            print('error on {}: {}'.format(tconst, err))
            raise err

    def download(self, titles):
        print('download api {} on {} titles'.format(
            self.api_name, titles.len()))
        for tconst in titles:
            try:
                self.download_title(tconst)
                print('done {}'.format(tconst))
            except Exception as err:
                print('skip {} for error: {}'.format(tconst, err))
