ID_PROBABILITIES = {"1": 0.1, "2": 0.8, "99": 0.1}  # {No, Yes, Not applicable}

OTHER_PROBABILITIES = {"1": 0.1, "2": 0.8, "99": 0.1}  # {No, Yes, Not applicable}

OTHER_GENETIC_PROBABILITIES = {"MLPA": 0.3, "FraX": 0.3, "DNA methylation test": 0.2, "other": 0.2}

OTHER_ID_RESULT_PROBABILITIES = {"Average": 0.4, "Below average": 0.6}

METABOLIC_PROBABILITIES = {"1": 0.1, "2": 0.8, "99": 0.1}  # {No, Yes, Unknown}

METABOLIC_MEASURES_PROBABILITIES = {"1": 0.1, "2": 0.8, "99": 0.1}  # {No, Yes, Unknown}

CT_PROBABILITIES = {"1": 0.3, "2": 0.6, "99": 0.1}  # {No, Yes, Unknown}

MR_PROBABILITIES = {"1": 0.3, "2": 0.6, "99": 0.1}  # {No, Yes, Unknown}

EEG_PROBABILITIES = {"1": 0.3, "2": 0.6, "99": 0.1}  # {No, Yes, Unknown}

KARYOTYPE_PROBABILITIES = {"1": 0.5, "2": 0.4, "99": 0.1}  # {No, Yes, Not applicable}

MICRO_ARRAY_PROBABILITIES = {"1": 0.5, "2": 0.4, "99": 0.1}  # {No, Yes, Not applicable}

MICRO_ARRAY_RESULT_PROBABILITIES = {"positive": 0.5, "negative": 0.4, "inconclusive": 0.1}

MICRO_ARRAY_TYPE_PROBABILITIES = {"1": 0.4, "2": 0.3, "3": 0.3}  # {"deletion","microdeletion","duplication"}

PREVIOUS_PROBABILITIES = {"1": 0.5, "2": 0.4, "99": 0.1}  # {No, Yes, Not applicable}

OTHER_RESULT_PROBABILITIES = {"positive": 0.5, "negative": 0.4, "inconclusive": 0.1}

GEN_PROBABILITIES = {"positive": 0.7, "negative": 0.2, "inconclusive": 0.1}

APPROACH_PROBABILITIES = {"CES": 0.3, "WES": 0.4, "WGS": 0.3}

PLATFORM_PROBABILITIES = {"MiSeq, Illumina": 0.2, "NextSeq550, Illumina": 0.2, "NextSeq 2000, Illumina": 0.2,
                                 "DNBSEQ G-400, MGI": 0.2, None: 0.2}

OTHER_PLATFORM_PROBABILITIES = {"Ion S5, Thermo Fisher Scientific": 0.4, "Sequel II, Pacific Biosciences": 0.4,
                                      "GridION, Oxford Nanopore Technologies": 0.1, "SOLiD 5500xl, SOLiD": 0.1}

REF_PROBABILITIES = {"GRCh 37 (hg19)": 0.5, "GRCh 38": 0.5}

NO_VAR_PROBABILITIES = {1: 0.5, 2: 0.5}

INHERITANCE_PROBABILITIES = {"2": 0.3, "1": 0.3, "3": 0.2, "4": 0.1,
                            "5": 0.1}  # {"Autosomal recessive inheritance", "Autosomal dominant inheritance", "X-linked inheritance, dominant", "X-linked inheritance, recessive", "Mitochondrial mutation"}

ZYGOSITY_PROBABILITIES = {"heterozygosis": 0.3, "het": 0.3, "homozygosis": 0.2, "hom": 0.1, "hemizygous": 0.1}

SEGREGATION_PROBABILITIES = {"No": 0.2, "1": 0.2, "2": 0.2, "3": 0.2,
                                    "4": 0.2}  # {"No","Not applicable","De novo","Maternal","Paternal"}

OTHER_AFFECTED_GENES = {"Yes": 0.2, "No": 0.8}  # {"Yes","No"}

VCF_LOC_PROBABILITIES = {"IMGGE Server": 0.8, None: 0.2}

VCF_OTHER_LOC_PROBABILITIES = {"External HD": 0.8, None: 0.2}
