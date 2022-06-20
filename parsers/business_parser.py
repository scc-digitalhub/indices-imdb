
from typing import Any
import numpy as np
from parser import Parser

class BusinessParser(Parser):
    
    def check_file(self ,dict_from_json ) : 
        if len(dict_from_json) != 0 : 
            return True 
        
    def __init__(self, countries_file) -> None:
        self.endpoints = ["gross", "openingWeekendGross"]
        self.keys = ['tconst', 'Budget_Amount', 'Budget_Currency', 'Amount_World', 'Currency_World', 'Weekend_Amount', 'Weekend_Currency', 'AL_Amount_openingWeekendGross', 
                    'AL_Currency_openingWeekendGross', 'AD_Amount_openingWeekendGross', 'AD_Currency_openingWeekendGross',
                    'AM_Amount_openingWeekendGross', 'AM_Currency_openingWeekendGross', 'AT_Amount_openingWeekendGross', 'AT_Currency_openingWeekendGross', 
                    'BY_Amount_openingWeekendGross', 'BY_Currency_openingWeekendGross', 'BE_Amount_openingWeekendGross', 'BE_Currency_openingWeekendGross', 
                    'BA_Amount_openingWeekendGross', 'BA_Currency_openingWeekendGross', 'BG_Amount_openingWeekendGross', 'BG_Currency_openingWeekendGross', 
                    'CH_Amount_openingWeekendGross', 'CH_Currency_openingWeekendGross', 'CY_Amount_openingWeekendGross', 'CY_Currency_openingWeekendGross', 
                    'CZ_Amount_openingWeekendGross', 'CZ_Currency_openingWeekendGross', 'DE_Amount_openingWeekendGross', 'DE_Currency_openingWeekendGross', 
                    'DK_Amount_openingWeekendGross', 'DK_Currency_openingWeekendGross', 'EE_Amount_openingWeekendGross', 'EE_Currency_openingWeekendGross', 
                    'ES_Amount_openingWeekendGross', 'ES_Currency_openingWeekendGross', 'FO_Amount_openingWeekendGross', 'FO_Currency_openingWeekendGross', 
                    'FI_Amount_openingWeekendGross', 'FI_Currency_openingWeekendGross', 'FR_Amount_openingWeekendGross', 'FR_Currency_openingWeekendGross', 
                    'GB_Amount_openingWeekendGross', 'GB_Currency_openingWeekendGross', 'GE_Amount_openingWeekendGross', 'GE_Currency_openingWeekendGross',
                    'GI_Amount_openingWeekendGross', 'GI_Currency_openingWeekendGross', 'GR_Amount_openingWeekendGross', 'GR_Currency_openingWeekendGross', 
                    'HU_Amount_openingWeekendGross', 'HU_Currency_openingWeekendGross', 'HR_Amount_openingWeekendGross', 'HR_Currency_openingWeekendGross', 
                    'IE_Amount_openingWeekendGross', 'IE_Currency_openingWeekendGross', 'IS_Amount_openingWeekendGross', 'IS_Currency_openingWeekendGross', 
                    'IT_Amount_openingWeekendGross', 'IT_Currency_openingWeekendGross', 'LT_Amount_openingWeekendGross', 'LT_Currency_openingWeekendGross', 
                    'LU_Amount_openingWeekendGross', 'LU_Currency_openingWeekendGross', 'LV_Amount_openingWeekendGross', 'LV_Currency_openingWeekendGross', 
                    'MC_Amount_openingWeekendGross', 'MC_Currency_openingWeekendGross', 'MK_Amount_openingWeekendGross', 'MK_Currency_openingWeekendGross',
                    'MT_Amount_openingWeekendGross', 'MT_Currency_openingWeekendGross', 'NO_Amount_openingWeekendGross', 'NO_Currency_openingWeekendGross',
                    'NL_Amount_openingWeekendGross', 'NL_Currency_openingWeekendGross', 'PO_Amount_openingWeekendGross', 'PO_Currency_openingWeekendGross',
                    'PT_Amount_openingWeekendGross', 'PT_Currency_openingWeekendGross', 'RO_Amount_openingWeekendGross', 'RO_Currency_openingWeekendGross',
                    'RU_Amount_openingWeekendGross', 'RU_Currency_openingWeekendGross', 'SE_Amount_openingWeekendGross', 'SE_Currency_openingWeekendGross', 
                    'SI_Amount_openingWeekendGross', 'SI_Currency_openingWeekendGross', 'SK_Amount_openingWeekendGross', 'SK_Currency_openingWeekendGross', 
                    'SM_Amount_openingWeekendGross', 'SM_Currency_openingWeekendGross', 'TR_Amount_openingWeekendGross', 'TR_Currency_openingWeekendGross', 
                    'UA_Amount_openingWeekendGross', 'UA_Currency_openingWeekendGross', 'VA_Amount_openingWeekendGross', 'VA_Currency_openingWeekendGross',
                    'AL_Amount_gross', 'AL_Currency_gross', 'AD_Amount_gross', 'AD_Currency_gross', 'AM_Amount_gross', 'AM_Currency_gross', 'AT_Amount_gross',
                    'AT_Currency_gross', 'BY_Amount_gross', 'BY_Currency_gross', 'BE_Amount_gross', 'BE_Currency_gross', 'BA_Amount_gross', 'BA_Currency_gross',
                    'BG_Amount_gross', 'BG_Currency_gross', 'CH_Amount_gross', 'CH_Currency_gross', 'CY_Amount_gross', 'CY_Currency_gross', 'CZ_Amount_gross',
                    'CZ_Currency_gross', 'DE_Amount_gross', 'DE_Currency_gross', 'DK_Amount_gross', 'DK_Currency_gross', 'EE_Amount_gross', 'EE_Currency_gross',
                    'ES_Amount_gross', 'ES_Currency_gross', 'FO_Amount_gross', 'FO_Currency_gross', 'FI_Amount_gross', 'FI_Currency_gross', 'FR_Amount_gross',
                    'FR_Currency_gross', 'GB_Amount_gross', 'GB_Currency_gross', 'GE_Amount_gross', 'GE_Currency_gross', 'GI_Amount_gross', 'GI_Currency_gross',
                    'GR_Amount_gross', 'GR_Currency_gross', 'HU_Amount_gross', 'HU_Currency_gross', 'HR_Amount_gross', 'HR_Currency_gross', 'IE_Amount_gross',
                    'IE_Currency_gross', 'IS_Amount_gross', 'IS_Currency_gross', 'IT_Amount_gross', 'IT_Currency_gross', 'LT_Amount_gross', 'LT_Currency_gross',
                    'LU_Amount_gross', 'LU_Currency_gross', 'LV_Amount_gross', 'LV_Currency_gross', 'MC_Amount_gross', 'MC_Currency_gross', 'MK_Amount_gross',
                    'MK_Currency_gross', 'MT_Amount_gross', 'MT_Currency_gross', 'NO_Amount_gross', 'NO_Currency_gross', 'NL_Amount_gross', 'NL_Currency_gross',
                    'PO_Amount_gross', 'PO_Currency_gross', 'PT_Amount_gross', 'PT_Currency_gross', 'RO_Amount_gross', 'RO_Currency_gross', 'RU_Amount_gross',
                    'RU_Currency_gross', 'SE_Amount_gross', 'SE_Currency_gross', 'SI_Amount_gross', 'SI_Currency_gross', 'SK_Amount_gross', 'SK_Currency_gross',
                    'SM_Amount_gross', 'SM_Currency_gross', 'TR_Amount_gross', 'TR_Currency_gross', 'UA_Amount_gross', 'UA_Currency_gross', 'VA_Amount_gross', 
                    'VA_Currency_gross']
        with open(countries_file) as infile:
            file_contents = infile.read()
            self.countries = file_contents.splitlines()

    def _business_world(self, end_point, business_dict_from_json_response):
        if end_point in business_dict_from_json_response["resource"] and 'aggregations' in business_dict_from_json_response["resource"][end_point].keys():
            for g in business_dict_from_json_response["resource"][end_point]["aggregations"]:
                a = {k: g[k] for k in list(g)[:2]}
                for key, value in a.items():
                    if key == "area" and value == "XWW":
                        return [g["total"]["amount"], g["total"]["currency"]]
                        break
                    else:
                        return [np.nan, np.nan]
        else:
            return [np.nan, np.nan]

    def _business_countrie(self, end_point, l_country, business_dict_from_json_response):
        gross_curr = {}
        if end_point in business_dict_from_json_response["resource"]:
            if "regional" in business_dict_from_json_response["resource"][end_point]:
                for c_tag in l_country:
                    for dic in business_dict_from_json_response["resource"][end_point]["regional"]:
                        a = {k: dic[k] for k in list(dic)[:2]}
                        for key, value in a.items():
                            if value == c_tag:
                                gross_curr[c_tag+"_Amount"+"_" +
                                           end_point] = [dic["total"]["amount"]][0]
                                gross_curr[c_tag+"_Currency"+"_" +
                                           end_point] = [dic["total"]["currency"]][0]
                                break

        return gross_curr

    def parse_title_business_data(self, tconst,  title_dict, business_dict_from_json_response) -> Any:
        
        if self.check_file(business_dict_from_json_response) : 
            try : 
                # create countries
                columns_gross = []
                for c in self.countries:
                    columns_gross.append(c+"_Amount_gross")
                    columns_gross.append(c+"_Currency_gross")
        
                columns_week_end = []
        
                for c in self.countries:
                    columns_week_end.append(c+"_Amount_openingWeekendGross")
                    columns_week_end.append(c+"_Currency_openingWeekendGross")
        
                columns_general = ["Budget_Amount", "Budget_Currency", "Amount_World", "Currency_World",
                                   "Weekend_Amount", "Weekend_Currency"]
        
                c = ['tconst'] + columns_general+columns_week_end+columns_gross
                dic_initial = {key: None for key in c}
        
                dic_initial['tconst'] = tconst
        
                # add values general
                a = [np.nan, np.nan]
                if "budget" in business_dict_from_json_response["resource"]:
                    a = [business_dict_from_json_response["resource"]["budget"]["amount"],
                         business_dict_from_json_response["resource"]["budget"]["currency"]]
        
                b = self._business_world("openingWeekendGross",
                                         business_dict_from_json_response)
                c = self._business_world("gross", business_dict_from_json_response)
        
                dic_initial['Budget_Amount'] = a[0]
                dic_initial['Budget_Currency'] = a[1]
        
                dic_initial['Weekend_Amount'] = b[0]
                dic_initial['Weekend_Currency'] = b[1]
        
                dic_initial['Amount_World'] = c[0]
                dic_initial['Currency_World'] = c[1]
                
        
                for e in self.endpoints:
                    d_i = self._business_countrie(
                        e, self.countries, business_dict_from_json_response)
                    for key, value in d_i.items():
                        dic_initial[key] = value
                return dic_initial
            except :
                return {key: None for key in self.keys}
        else : 
            return {key: None for key in self.keys}

        

    def parse_title_data(self, tconst,  title_dict, dict_from_json_response) -> Any:
        return self.parse_title_business_data(tconst,  title_dict, dict_from_json_response)

