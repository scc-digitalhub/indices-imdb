# -*- coding: utf-8 -*-
"""
Created on Mon May  9 09:51:03 2022

@author: antoi
"""
from typing import Any
import numpy as np
from parser import Parser


class MetacriticParser(Parser):

    def __init__(self) -> None:
        self.keys = ['tconst', 'User_Score', 'Expert_Score']
        
    def check_file(self ,dict_from_json ) : 
        if len(dict_from_json) != 0 : 
            return True 

    def _average_expert(self, dict_from_json):
        t = np.array([])
        if "reviews" in dict_from_json.keys():
            for items in dict_from_json['reviews']:
                t = np.append(t, items['score'])
        return np.average(t)

    def _average_id(self, dict_from_json):
        if 'userScore' in dict_from_json:
            return dict_from_json['userScore']*10
        else:
            return np.nan

    def parse_meta(self, dict_from_json, tconst):
        if self.check_file(dict_from_json) : 
            try : 
                return {'tconst' : tconst ,"User_Score" :  self._average_id(dict_from_json), "Expert_Score": self._average_expert(dict_from_json)}
            except : 
                return {key: None for key in self.keys}
        else : 
            return {key: None for key in self.keys}

    def parse_metacritic(self, dict_from_json, tconst, title_dict) -> Any:
        return self.parse_meta(dict_from_json, tconst)

    def parse_title_data(self, tconst,  title_dict, dict_from_json_response) -> Any:
        return self.parse_metacritic(dict_from_json_response, tconst, title_dict)

