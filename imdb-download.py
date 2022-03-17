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
RAPIDAPI = {
    'host': "imdb8.p.rapidapi.com",
    'key': os.getenv('RAPIDAPI_KEY', ''),
    'delay': 0
}

# apis, input should be title id
BUSINESS_API = {
    'name': "business",
    'path': "/title/get-business?tconst={}",
    'content': 'resource'
}
CREDITS_API = {
    'name': "credits",
    'path': "/title/get-full-credits?tconst={}",
    'content': 'cast'
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


def download_api(api, tdf):
    print("download API {}...".format(api['name']))
    downloader = Downloader(RAPIDAPI, api, DATA_DIR,
                            overwrite=OVERWRITE, delay=RAPIDAPI['delay'])
    # download data
    c = 0
    for index, row in tdf.iterrows():
        tconst = row["tconst"]
        tnow = datetime.now()
        print(
            "download {} for {}: {} / {}".format(api['name'], index+1, tconst, tnow))
        try:
            downloader.download_title(tconst)
            c = c+1
            print('success {}'.format(tconst))
        except Exception as err:
            print('skip {} for error: {}'.format(tconst, err))
    return c


print("IMDB downloader for RapidAPI")
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
        now = datetime.now()
        print("execute API {} on {} items / {}".format(api['name'], size, now))
        count = download_api(api, tdf)
        now = datetime.now()
        print("done API {} on {} items / {}".format(api['name'], count, now))
        stats[api['name']]['count'] = count
    except Exception as err:
        print('skip {} for error: {}'.format(api['name'], err))

now = datetime.now()
print('done. / {}'.format(now))
print('stats {}'.format(stats))
