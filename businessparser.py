
from parser import Parser
from typing import Any

import numpy as np


class BusinessParser(Parser):

    def __init__(self, countries_file) -> None:
        self.endpoints = ["gross", "openingWeekendGross"]
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

        c = columns_general+columns_week_end+columns_gross
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
        dic_initial['Movie_Name'] = title_dict['primaryTitle']

        return dic_initial

    def parse_title_data(self, tconst,  title_dict, dict_from_json_response) -> Any:
        return self.parse_title_business_data(tconst,  title_dict, dict_from_json_response)
