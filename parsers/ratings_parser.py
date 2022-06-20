# -*- coding: utf-8 -*-
"""
Created on Tue May  3 13:05:47 2022

@author: antoi
"""

from typing import Any
from parser import Parser



class RatingsParser (Parser):
    def __init__(self, work_file) -> None:
        
        with open(work_file) as infile:
            file_contents = infile.read()
            self.work = file_contents.splitlines()
    
    def check_file(self ,dict_from_json ) : 
        if len(dict_from_json) != 0 : 
            return True 
    
    def parse_rating(self, dict_from_json, tconst):
        if self.check_file(dict_from_json) : 
            try : 
                a = 0
                l_dict = {"tconst" : tconst}
                while a != len(self.work):
                    e = self.work[a]
                    l_dict[e] = dict_from_json["ratingsHistograms"][e]["histogram"]
                    a += 1
                return l_dict
            except : 
                return  {key: {} for key in self.work}
        else : 
            return  {key: {} for key in self.work}

    def parse_ratings_data(self, tconst, dict_from_json_response, title_dict) -> Any:
        return self.parse_rating(dict_from_json_response, tconst)

    def parse_title_data(self, tconst, title_dict, dict_from_json_response) -> Any:
        return self.parse_ratings_data(tconst, dict_from_json_response, title_dict)
    
