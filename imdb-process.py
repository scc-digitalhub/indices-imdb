from email.mime import base
from re import L
from downloader import Downloader
from processor import Processor
import pandas as pd

from businessparser import BusinessParser
from creditsparser import CreditsParser
import os
import sys
from time import sleep
from datetime import datetime

DATA_DIR = "./data"
OVERWRITE = False
TITLES_TSV = DATA_DIR + "/titles.csv"
CHUNKSIZE = 5000

# apis, input should be title id
BUSINESS_API = {
    'name': "business",
    'path': "/title/get-business?tconst={}",
    'content': 'resource',
    'parser': BusinessParser(countries_file=DATA_DIR+"/config/countryEU_codes.txt")
}
CREDITS_API = {
    'name': "credits",
    'path': "/title/get-full-credits?tconst={}",
    'content': 'cast',
    'parser':  CreditsParser(work_file=DATA_DIR+"/config/endpoints_cast.txt")
}
RATINGS_API = {
    'name': "ratings",
    'path': "/title/get-ratings?tconst={}",
    'content': 'rating'
}
MORE_LIKE_API = {
    'name': "more-like-this",
    'path': "/title/get-more-like-this?tconst={}"
}


def build_basepath(api):
    basedir = DATA_DIR+"/process/"+api['name']
    if not os.path.exists(basedir):
        print('\t mkdirs {}'.format(basedir))
        os.makedirs(basedir)
    return basedir


def process_api(api, tdf, idx):
    print("process API {}...".format(api['name']))
    processor = Processor(api, api['parser'], DATA_DIR)

    # process data in batch
    tnow = datetime.now()
    basedir = build_basepath(api)
    outpath = basedir + '/_'+api['name']+'-'+str(idx) + '.parquet'
    if not OVERWRITE and os.path.exists(outpath):
        print(
            "skip batch {} for {}: exists {} / {}".format(api['name'], idx, len(tdf), tnow))
        return len(tdf)

    c = 0
    batch = []
    for index, row in tdf.iterrows():
        tconst = row["tconst"]
        tnow = datetime.now()
        print(
            "process {} for {}: {} / {}".format(api['name'], index+1, tconst, tnow))
        try:
            data = processor.process_title(tconst, row)
            dd = pd.DataFrame([data])
            batch.append(dd)
            c = c+1
            print('success {}'.format(tconst))
        except Exception as err:
            print('skip {} for error: {}'.format(tconst, err))

    # compile batch df
    tnow = datetime.now()
    print(
        "batch {} for {}: {} / {}".format(api['name'], idx, len(batch), tnow))
    if(len(batch) == 0):
        print('skip batch {} for error: no data'.format(api["name"]))
    bdf = pd.concat(batch)
    # write batch
    tnow = datetime.now()

    print(
        "write batch {} for {} to {} / {}".format(api['name'], idx, outpath, tnow))
    bdf.to_parquet(outpath)
    return c


def chunk(df, chunk_size):
    start = 0
    length = df.shape[0]

    if length <= chunk_size:
        yield df[:]
        return

    while start + chunk_size <= length:
        yield df[start:chunk_size + start]
        start = start + chunk_size

    if start < length:
        yield df[start:]


print("IMDB process for RapidAPI")
apilist = [CREDITS_API, BUSINESS_API, RATINGS_API, MORE_LIKE_API]
apis = [a for a in apilist if a['name'] in sys.argv]
if not apis:
    print("no api selected, exit")
    exit()
apins = [a['name'] for a in apis]
print("execute for APIs {}".format(apins))

# read data
now = datetime.now()
print("read dataset from {} / {}".format(TITLES_TSV, now))
tdf = pd.read_csv(TITLES_TSV)
size = len(tdf)
now = datetime.now()
print("items count: {} / {}".format(size, now))

# process apis
stats = {a['name']: {'count': 0, 'total': size} for a in apis}
for api in apis:
    try:
        count = 0
        # iterate in chunks
        idx = 0
        for cdf in chunk(tdf, CHUNKSIZE):
            try:
                now = datetime.now()
                print(
                    "execute API {} on {} items / {}".format(api['name'], len(cdf), now))
                countb = process_api(api, cdf, idx)
                now = datetime.now()
                print(
                    "done API {} on {} items / {}".format(api['name'], countb, now))
                count = count + countb
            except Exception as err:
                print('skip {} for error: {}'.format(api['name'], err))

            idx = idx+1

        stats[api['name']]['count'] = count

        # build final dataset by reading all chunks
        now = datetime.now()
        print(
            'compile full dataset {}: expected chunks {} / {}'.format(api["name"], idx, now))

        ll = []
        for i in range(0, idx):
            basedir = build_basepath(api)
            inpath = basedir + '/_'+api['name']+'-'+str(i) + '.parquet'
            if not os.path.exists(inpath):
                raise Exception(
                    "missing batch parquet for {}: {}".format(i, inpath))
            ldf = pd.read_parquet(inpath)
            ll.append(ldf)

        now = datetime.now()
        print(
            'concat full dataset {}: read chunks {} / {}'.format(api["name"], len(ll), now))
        df = pd.concat(ll)
        stats[api['name']]['count'] = len(df)

        # write
        now = datetime.now()
        basedir = build_basepath(api)
        outpath = basedir+'/'+api['name']+'.parquet'
        print(
            'write full dataset {} size {} to {} / {}'.format(api["name"], len(df), outpath, now))
        df.to_parquet(outpath)
    except Exception as err:
        print('terminated {} for error: {}'.format(api["name"], err))

now = datetime.now()
print('done. / {}'.format(now))
print('stats {}'.format(stats))
