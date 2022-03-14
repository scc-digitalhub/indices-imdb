from abc import abstractmethod


from typing import Any


class Parser:
    @abstractmethod
    def parse_title_data(self, tconst,  title_dict, dict_from_json_response) -> Any:
        """
        parse response
        """
