import re

from utils.setup_logger import log
from utils.utils import normalize_ontology_code, process_spaces, is_not_nan


class OntologyCode:
    def __init__(self, full_code: str):
        # full code corresponds to a (simple or complex) ontology code with or without names
        # e.g., 406506008
        # or 406506008 |Attention deficit hyperactivity disorder|
        # or 726527001 |Weight| : 410671006 |Date|
        # or 3332001 |Occipitofrontal diameter of head| : 246454002 |Occurrence| = 3950001 |Birth|
        self.full_code = full_code
        # this may happen that no ontology code is given foe a column, resp. value
        # in that case, full_code is nan and we cannot compute its ids and names: they are nan too
        if is_not_nan(self.full_code):
            # those fields are computed from the given full_code
            self.concat_ids = self.compute_concat_ids()
            self.concat_names = self.compute_concat_names()
            self.hierarchy = {}
        else:
            self.concat_ids = None
            self.concat_names = None
            self.hierarchy = None

    def compute_concat_ids(self) -> str:
        if "|" not in self.full_code:
            # this is a code without pipe annotation, we can return it as is
            return normalize_ontology_code(self.full_code)
        else:
            # this is a code with pipe annotation,
            # we remove the text inside the pipes + the pipes to only get the ids
            regex = re.compile("(\\|[\\w -(){}]+)*\\|")
            concat_ids = normalize_ontology_code(re.sub(regex, "", self.full_code))
            return concat_ids

    def compute_concat_names(self) -> str:
        if "|" not in self.full_code:
            # this is a code without pipe annotation, thus there is nothing to return
            return ""
        else:
            # this is a code with pipe annotation,
            # we remove all codes + pipes to only get the names
            matches = re.findall("([ =:,(){}]*\\|([\\w -(){}]+)*\\|[ =:,(){}]*)", self.full_code)
            concat_names = ""
            for match_tuple in matches:
                concat_names += re.sub(" *\\| *", "|", process_spaces(match_tuple[0]))
            return concat_names

