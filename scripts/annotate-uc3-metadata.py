import pandas as pd

from enums.MetadataColumns import MetadataColumns
from utils.api_utils import send_query, parse_json_response

if __name__ == '__main__':
    metadata = pd.read_csv("../metadata-UC3.csv")
    print(metadata)

    columns_descriptions = []
    columns_ontos = []

    for column_descr in metadata[MetadataColumns.SIGNIFICATION_EN]:
        print(column_descr)
        url = f"http://data.bioontology.org/annotator?text={column_descr}&apikey=d6fb9c05-3309-4158-892f-65434a9133b9&ontologies=SNOMEDCT,LOINC,GSSO,ORDO,GO,OMIM,HGNC,HP"
        response = send_query(url=url, headers=None)
        the_associated_classes = []
        if response is None:
            print(f"Failed connection to SNOMED-CT API.")
        elif response.status_code == 200:
            data = parse_json_response(response)
            for element in data:
                the_associated_classes.append(element["annotatedClass"]["@id"])
            columns_descriptions.append(column_descr)
            columns_ontos.append(the_associated_classes)
            print(f"{column_descr} -> {the_associated_classes}")
            the_associated_classes = []
        elif response.status_code == 404 or response.status_code == 400:
            print(f"Problem with description '{column_descr}'.")
        else:
            print(f"Failed connection to SNOMED-CT API.")
    new_md = pd.DataFrame(data={"column": columns_descriptions, "ontologies": columns_ontos})
    new_md.to_csv("new_md_uc3.csv")
