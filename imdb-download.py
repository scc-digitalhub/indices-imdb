from downloader import Downloader
from processor import Processor
import pandas as pd

from businessparser import BusinessParser
from creditsparser import CreditsParser
import os
from time import sleep

DATA_DIR = "./data"
OVERWRITE = False
TITLES_TSV = DATA_DIR + "/titles.csv"
RAPIDAPI = {
    'host': "imdb8.p.rapidapi.com",
    'key': os.getenv('RAPIDAPI_KEY', ''),
    'delay': 1
}

# apis, input should be title id
BUSINESS_API = {
    'name': "business",
    'path': "/title/get-business?tconst={}"
}

CREDITS_API = {
    'name': "credits",
    'path': "/title/get-full-credits?tconst={}"
}


def download_api(api, tdf):
    print("download API {}...".format(api['name']))
    downloader = Downloader(RAPIDAPI, api, DATA_DIR, overwrite=OVERWRITE, delay=RAPIDAPI['delay'])
    # download data
    c = 0
    for index, row in tdf.iterrows():
        tconst = row["tconst"]
        print("download {} for {}: {}".format(api['name'], index+1, tconst))
        try:
            downloader.download_title(tconst)
            c = c+1
            print('done {}'.format(tconst))
        except Exception as err:
            print('skip {} for error: {}'.format(tconst, err))
    return c


print("IMDB downloader for RapidAPI")
apis = [CREDITS_API, BUSINESS_API]
apins = [a['name'] for a in apis]
print("execute for APIs {}".format(apins))

# read data
print("read dataset from {}".format(TITLES_TSV))
tdf = pd.read_csv(TITLES_TSV).head(1)
size = len(tdf)
print("items count: {}".format(size))

# process apis
for api in apis:
    try:
        print("execute API {} on {} items".format(api['name'], size))
        count = download_api(api, tdf)
        print("done API {} on {} items".format(api['name'], count))
    except Exception as err:
        print('skip {} for error: {}'.format(api['name'], err))
