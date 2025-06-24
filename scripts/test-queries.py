import numpy as np
import pandas as pd


def get_data():
    return [
        # sex
        {"has_subject": 1, "instantiates": 1, "value": "F"}, {"has_subject": 2, "instantiates": 1, "value": "F"},
        {"has_subject": 3, "instantiates": 1, "value": "M"}, {"has_subject": 4, "instantiates": 1, "value": "F"},
        {"has_subject": 5, "instantiates": 1, "value": "M"}, {"has_subject": 6, "instantiates": 1, "value": "M"},
        {"has_subject": 7, "instantiates": 1, "value": "F"}, {"has_subject": 8, "instantiates": 1, "value": "F"},
        # weight
        {"has_subject": 1, "instantiates": 2, "value": 1.7}, {"has_subject": 2, "instantiates": 2, "value": 1.7},
        {"has_subject": 3, "instantiates": 2, "value": 1.75}, {"has_subject": 4, "instantiates": 2, "value": 2.1},
        {"has_subject": 5, "instantiates": 2, "value": 1.7}, {"has_subject": 6, "instantiates": 2, "value": 2.1},
        {"has_subject": 7, "instantiates": 2, "value": 1.79}, {"has_subject": 8, "instantiates": 2, "value": 1.79},
        # diseased
        {"has_subject": 1, "instantiates": 3, "value": False}, {"has_subject": 2, "instantiates": 3, "value": True},
        {"has_subject": 3, "instantiates": 3, "value": False}, {"has_subject": 4, "instantiates": 3, "value": False},
        {"has_subject": 5, "instantiates": 3, "value": False}, {"has_subject": 6, "instantiates": 3, "value": False},
        {"has_subject": 7, "instantiates": 3, "value": True}, {"has_subject": 8, "instantiates": 3, "value": False},
        # covid
        {"has_subject": 1, "instantiates": 4, "value": False}, {"has_subject": 2, "instantiates": 4, "value": True},
        {"has_subject": 3, "instantiates": 4, "value": True}, {"has_subject": 4, "instantiates": 4, "value": True},
        {"has_subject": 5, "instantiates": 4, "value": True}, {"has_subject": 6, "instantiates": 4, "value": False},
        {"has_subject": 7, "instantiates": 4, "value": True}, {"has_subject": 8, "instantiates": 4, "value": False},
    ]


def create_co_occurrence_matrix_from_records(records: list):
    unique_values_per_feature = {}
    for record in records:
        if record["instantiates"] not in unique_values_per_feature:
            unique_values_per_feature[record["instantiates"]] = []
        if record["value"] not in unique_values_per_feature[record["instantiates"]]:
            unique_values_per_feature[record["instantiates"]].append(record["value"])
    print(unique_values_per_feature)

    row_index_1 = []
    row_index_2 = []
    for column, unique_values in unique_values_per_feature.items():
        print(column)
        print(unique_values)
        row_index_1.extend([str(column) for _ in range(len(unique_values))])
        row_index_2.extend([str(value) for value in unique_values])
    print(row_index_1)
    print(row_index_2)

    row_indexes = [np.array(row_index_1), np.array(row_index_2)]
    co_occurrences = pd.DataFrame(0, index=row_indexes, columns=row_indexes)
    print(co_occurrences)

    # enumerate all value pairs
    value_pairs = []
    for r1 in records:
        for r2 in records:
            if r1["instantiates"] < r2["instantiates"] and r1["has_subject"] == r2["has_subject"]:
                # we compute pairs only for the upper non-diagonal cells
                # the lower part of the matrix will be empty to avoid duplicates
                # the diagonal will be computed afterward, without pairs
                value_pairs.append((r1["instantiates"], r1["value"], r2["instantiates"], r2["value"]))
    print(value_pairs)

    # enumerate counts for filling the diagonal
    value_counts = {}
    for record in records:
        fid = record["instantiates"]
        if fid not in value_counts:
            value_counts[fid] = {}
        if record["value"] not in value_counts[fid]:
            value_counts[fid][record["value"]] = 1
        else:
            value_counts[fid][record["value"]] += 1
    print(value_counts)

    # fill the upper part of the co-occurrence matrix
    for value_pair in value_pairs:
        co_occurrences.loc[(str(value_pair[0]), str(value_pair[1])), (str(value_pair[2]), str(value_pair[3]))] += 1
    print(co_occurrences)
    # fill the matrix diagonal
    for fid in value_counts:
        for value in value_counts[fid]:
            co_occurrences.loc[(str(fid), str(value)), (str(fid), str(value))] = value_counts[fid][value]
    print(co_occurrences)
    return co_occurrences


def and_operator(a, b):
    return min(a, b)


def or_operator(a, b):
    return min(a, b)


def not_operator(a):
    return a


if __name__ == '__main__':

    records = get_data()
    co_occurrences = create_co_occurrence_matrix_from_records(records)
    query = []
