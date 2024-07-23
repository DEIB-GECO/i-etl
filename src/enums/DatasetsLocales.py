class DatasetsLocales:
    # use-case 1
    IT_BUZZI_UC1 = "it_IT"
    RS_IMGGE = "sr_RS"
    ES_HSJD = "es_ES"
    # use-case 2
    ES_LAFE = "es_ES"
    IL_HMC = "en_IL"
    # use-case 3
    IT_BUZZI_UC3 = "it_IT"
    ES_TERRASSA = "es_ES"
    DE_UKK = "de_DE"

    @classmethod
    def values(cls):
        xs = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                xs.append(value)
        return xs
