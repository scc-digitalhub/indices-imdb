# -*- coding: utf-8 -*-
"""
Created on Tue May  3 13:19:27 2022

@author: antoi
"""
from typing import Any
import numpy as np
import datetime
from paser import Parser


class BioParser(Parser):

    def __init__(self) -> None:
        self.keys = ['nconst', 'gender', 'name', 'age']
        
    def check_file(self ,dict_from_json ) : 
        if len(dict_from_json) != 0 : 
            return True 

    def _get_gender(self, dict_from_json):
        if "gender" in dict_from_json.keys():
            if dict_from_json["gender"] == "male":
                return "M"
            elif dict_from_json["gender"] == "female":
                return "F"
            else:
                return "U"
        else:
            return np.nan

    def _get_name(self, dict_from_json):
        if "name" in dict_from_json.keys():
            return dict_from_json["name"]
        else:
            return np.nan

    def _get_age(self, dict_from_json):
        if "birthDate" in dict_from_json.keys():
            return (datetime.datetime.now()-datetime.datetime.strptime(dict_from_json["birthDate"], '%Y-%m-%d')).days/365
        else:
            return np.nan

    def parse_person(self, dict_from_json, nconst):
        try :
            if self.check_file(dict_from_json) : 
                return {'nconst': nconst ,"gender": self._get_gender(dict_from_json), "name": self._get_name(dict_from_json), "age": self._get_age(dict_from_json)}
            else : 
                return {key: None for key in self.keys}
        except :
            return {key: None for key in self.keys}
        
    def parse_person_data(self, nconst, dict_from_json_response,) -> Any:
        return self.parse_person(dict_from_json_response, nconst)

