from datetime import datetime
from typing import Any

from enums.EnumAsClass import EnumAsClass

THE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Operators(EnumAsClass):
    @classmethod
    def match(cls, field: str | None, value: Any, is_regex: bool) -> dict:
        if is_regex:
            # this is a match with a regex (in value)
            return {
                "$match": {
                    field: {
                        # the value (the regex) should not contain the / delimiters as in /^[0-9]+$/,
                        # but only the regex, as in "^[0-9]+$"
                        "$regex": value
                    }
                }
            }
        else:
            # this is a match with a "hard-coded" value (in value) or a complex operator
            if field is None:
                # the value is a complex operator, e.g., and/or
                return {
                    "$match": value
                }
            else:
                # this is a match with a "hard-coded" value (in value)
                return {
                    "$match": {
                        field: value
                    }
                }

    @classmethod
    def or_operator(cls, list_of_conditions: list[dict]) -> dict:
        # list_of_conditions is a list where each element is a dict-like condition,
        # containing the field and the condition to apply
        # e.g., [{"value": 3}, {"value": {"$type": "bool"}}]
        return {
            "$or": list_of_conditions
        }

    @classmethod
    def project(cls, field: str | list | dict | None, projected_value: str | dict | None) -> dict:
        # this is the SQL SELECT operator
        # where each field that is wanted in the result has value 1 and unwanted fields have value 0
        if type(projected_value) is str:
            # in this case, we want to keep a certain field
            # and choose what should be the value of that field (in case of composed fields)
            if type(field) is dict:
                # if the composed field may be empty, we use an alternative field ("$ifNull", within the field)
                return {
                    "$project": {
                        projected_value: field,
                        "_id": 0
                    }
                }
            else:
                # else, we simply return the projection
                return {
                    "$project": {
                        projected_value: "$" + field,
                        "_id": 0
                    }
                }
        elif type(projected_value) is dict:
            # in case we give a complex projection, e.g., with $split
            return {
                "$project": projected_value
            }
        else:
            # in that case, we only want to keep a certain field
            if isinstance(field, list):
                # we want to keep several fields
                fields = {f: 1 for f in field}
                fields["_id"] = 0
                return {
                    "$project": fields
                }
            elif isinstance(field, dict):
                # we want to compute a novel value while not keeping the old ones
                return {
                    "$project": field
                }
            else:
                # we want to keep a single field
                return {
                    "$project": {
                        field: 1,
                        "_id": 0
                    }
                }

    @classmethod
    def lookup(cls, join_table_name: str, foreign_field: str|None, local_field: str|None, lookup_field_name: str, let: dict|None,
               pipeline: list|None) -> dict:
        # this is a SQL join between two tables
        # from is the "second" table of the join
        # localField is the field of the "second" table to join
        # foreignField is the field of the "first" table to join
        # as is the name of the (new array) field added containing either the joined resource (of the second table) or an empty array if no join could be made for the tuple
        the_lookup = {
            "$lookup": {
                "from": join_table_name,
                "as": lookup_field_name,
            }
        }

        if local_field is not None:
            the_lookup["$lookup"]["localField"] = local_field
        if foreign_field is not None:
            the_lookup["$lookup"]["foreignField"] = foreign_field
        if let is not None:
            the_lookup["$lookup"]["let"] = let
        if pipeline is not None:
            the_lookup["$lookup"]["pipeline"] = pipeline
        return the_lookup

    @classmethod
    def cartesian_product(cls, join_table_name: str, lookup_field_name: str, filter_dict: dict) -> dict:
        # if len(filter_dict) == 0:
        #     pipeline = [{"$project": {field_b: 1, "_id": 0}}]
        # else:
        #     pipeline = [filter_dict, {"$project": {field_b: 1, "_id": 0}}]
        return {
            "$lookup": {
                "from": join_table_name,
                "pipeline": [filter_dict] if len(filter_dict) > 0 else [],
                "as": lookup_field_name
            }
        }

    @classmethod
    def union(cls, second_table_name: str, second_pipeline: list):
        # this is the SQL UNION operator
        return {
            "$unionWith": {
                "coll": second_table_name,
                "pipeline": second_pipeline
            }
        }

    @classmethod
    def sort(cls, field: str, sort_order: int) -> dict:
        # this is the SQL ORDER BY operator
        return {
            "$sort": {
                field: sort_order
            }
        }

    @classmethod
    def sort_many(cls, map_field_order: dict) -> dict:
        # this is the SQL ORDER BY operator
        return {
            "$sort": map_field_order
        }

    @classmethod
    def limit(cls, nb: int) -> dict:
        # this is the SQL LIMIT operator
        return {
            "$limit": nb
        }

    @classmethod
    def group_by(cls, group_key: dict | list | str | None, groups: list) -> dict:
        # this is the SQL GROUP BY operator
        """
        Compute group by (on one or many fields, with one or many operators)
        :param group_key: The $group stage separates documents into groups according to a "group key". The output is one document for each unique group key.
        :param groups: a list of objects for each group by to add to the query (there is only one object is one group by)
        The objects are of the form: {"name": group_by_name, "operator", the aggregation operator (min, max, avg, etc.), "field": the field on which to compute the group by
        If groups is empty, this means that we simply use the group_by operator to simulate a distinct
        :return:
        """
        query = {
            "$group": {}
        }

        query["$group"]["_id"] = group_key

        for group_by in groups:
            query["$group"][group_by["name"]] = {group_by["operator"]: group_by["field"]}
        return query

    @classmethod
    def unwind(cls, field: str) -> dict:
        return {
            "$unwind": f"${field}"
        }

    @classmethod
    def concat(cls, list_of_strings: list) -> dict:
        return {
            "$concat": list_of_strings
        }

    @classmethod
    def if_condition(cls, cond: dict, if_part: dict | str, else_part: dict | str) -> dict:
        res = {
            "$cond": {
                "if": cond,
                "then": if_part,
                "else": else_part
            }
        }
        return res

    @classmethod
    def equality(cls, field: str, value: str) -> dict:
        return {
            "$eq": [field, value]
        }

    @classmethod
    def add_fields(cls, key: str, value: str | dict) -> dict:
        return {
            "$addFields": {
                key: value
            }
        }

    @classmethod
    def set_variables(cls, variables: list) -> dict:
        return {
            "$set": {
                elem["name"]: elem["operation"] for elem in variables
            }
        }

    @classmethod
    def unset_variables(cls, variables: list) -> dict:
        return {
            "$unset": variables
        }

    @classmethod
    def from_datetime_to_isodate(cls, current_datetime: datetime) -> dict:
        return {"$date": current_datetime.strftime(THE_DATETIME_FORMAT)}

    @classmethod
    def merge(cls, table_name: str, on_attribute: str | list, when_matched: str, when_not_matched: str) -> dict:
        # append new tuples, e.g., from an aggregation pipeline, to an existing collection
        # when matched: replace|keepExisting|merge|fail|pipeline
        # when not matched: insert|discard|fail
        return {
            "$merge": {
                "into": table_name,
                "on": on_attribute,
                "whenMatched": when_matched,
                "whenNotMatched": when_not_matched
            }
        }

    @classmethod
    def write_to_table(cls, table_name: str) -> dict:
        return {
            "$out": table_name
        }

    @classmethod
    def filter_array(cls, input_array_name: str, element: str, cond: dict, limit: int) -> dict:
        return {
            "$filter": {
                "input": input_array_name,
                "as": element,
                "cond": cond,  # use element in cond to filter elements
                "limit": limit
            }
        }
