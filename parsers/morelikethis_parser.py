# -*- coding: utf-8 -*-
"""
Created on Tue May  3 15:40:30 2022

@author: antoi
"""
import re
from typing import Any
from parser import Parser


class MoreLikeThisParser (Parser):

    def __init__(self) -> None:
        self.keys = ['tconst' , 'Recommendations']
        
    def check_file(self ,dict_from_json ) : 
        if len(dict_from_json) != 0 : 
            return True 
        
    def _clean_regex(self, tconst):
        title = re.sub('["]', "", tconst)
        return re.search(r'(/.*/([^/]+)/)', title)[2]

    def parse_morelikethis(self, dict_from_json, tconst):
            if self.check_file (dict_from_json) : 
                try : 
                    d={'tconst' : tconst  }
                    r=[]
                    for items in dict_from_json : 
                        try : 
                            r.append(self._clean_regex(items))
                        except : 
                            pass 
                    d['Recommendations'] = r
                    return d
                    
                except : 
                    return {key: None for key in self.keys}
                     
            else : 
                return {key: None for key in self.keys}

    

    def parse_title_data(self, tconst,  title_dict, dict_from_json_response) -> Any:
        return self.parse_morelikethis(  dict_from_json_response,tconst )

