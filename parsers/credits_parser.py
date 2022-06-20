# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 15:31:01 2022

@author: antoi
"""
from typing import Any
import re
from parser import Parser

class CreditsParser(Parser):
    
    def __init__(self, work_file) -> None:
        with open(work_file) as infile:
            file_contents = infile.read()
            self.work = file_contents.splitlines()
        self.keys= ['tconst', 'writer', 'director', 'animation_department', 'costume_designer', 'cinematographer', 'camera_department', 'sound_department', 'visual_effects', 'assistant_director', 'art_department', 'ordered_cast', 'disordered_cast']
    
    def check_file(self ,dict_from_json ) : 
        if len(dict_from_json) != 0 : 
            return True 


    def _get_cast(self, dict_from_json):
        dic = {k: [] for k in ['ordered_cast', 'disordered_cast']}
        if 'billing' in dict_from_json['cast'][0]:
            for i in dict_from_json['cast']:
                if 'billing' in i.keys():
                    dic['ordered_cast'].append(
                        re.search(r'(/.*/([^/]+)/)', str(i['id']))[2])
                else:
                    pass
        else:
            for i in dict_from_json['cast']:
                dic['disordered_cast'].append(
                    re.search(r'(/.*/([^/]+)/)', str(i['id']))[2])

        return dic

    def _get_crew(self, dict_from_json):
        dic = {k: [] for k in self.work}
        for e in self.work:
            if e in dict_from_json["crew"]:
                a = 0
                while a != len(dict_from_json["crew"]):
                    try:
                        dic[e].append(
                            re.search(r'(/.*/([^/]+)/)', str(dict_from_json["crew"][e][a]['id']))[2])
                        a = a+1
                    except:
                        a = a+1

            else:
                pass
        return dic

    def parse_title_cast_data(self,  tconst, dict_from_json) -> Any:
        if self.check_file (dict_from_json) : 
            try : 
                a = self._get_crew(dict_from_json)
                b = self._get_cast(dict_from_json)
                return {
                    'tconst': tconst,
                    **a,
                    **b
                }
            except : 
                return {key: None for key in self.keys}
        else : 
            return {key: None for key in self.keys}
        

    def parse_title_data(self, tconst,  title_dict, dict_from_json_response) -> Any:
        return self.parse_title_cast_data(tconst,   dict_from_json_response)


