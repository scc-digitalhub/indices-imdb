# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 15:31:01 2022

@author: antoi
"""
from typing import Any
from abc import abstractmethod
from parser import Parser


class CreditsParser(Parser):
    def __init__(self, work_file) -> None:
        with open(work_file) as infile:
            file_contents = infile.read()
            self.work = file_contents.splitlines()

    def _get_cast(self, dict_from_json):
        cast_billed = {}
        if 'billing' in dict_from_json['cast'][0]:
            for i in dict_from_json['cast']:
                if 'billing' in i.keys():
                    cast_billed[i['id']] = i['billing']
                else:
                    pass
        else:
            for i in dict_from_json['cast']:
                cast_billed[i['id']] = 1

        return cast_billed

    def _get_crew(self, dict_from_json):
        dic = {}
        for e in self.work:
            if e in dict_from_json["crew"]:
                a = 0
                while a != len(dict_from_json["crew"]):
                    try:
                        dic[dict_from_json["crew"][e][a]['id']] = e
                        a = a+1
                    except:
                        a = a+1

            else:
                pass
        return dic

    def parse_title_cast_data(self,  tconst, dict_from_json) -> Any:
        a = self._get_crew(dict_from_json)
        b = self._get_cast(dict_from_json)
        return {
            'tconst': tconst,
            **a,
            **b
        }

    def parse_title_data(self, tconst,  title_dict, dict_from_json) -> Any:
        return self.parse_title_cast_data(tconst,   dict_from_json)
