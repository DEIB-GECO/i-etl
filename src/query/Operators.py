from datetime import datetime
from typing import Any

from enums.EnumAsClass import EnumAsClass
from utils.assertion_utils import THE_DATETIME_FORMAT


class Operators(EnumAsClass):
    @classmethod
    def match(cls, field: str, value: Any, is_regex: bool) -> dict:
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
            # this is a match with a "hard-coded" value (in value)
            return {
                "$match": {
                    field: value
                }
            }

    @classmethod
    def project(cls, field: str | list, projected_value: str | dict | None) -> dict:
        # this is the SQL SELECT operator
        # where each field that is wanted in the result has value 1 and unwanted fields have value 0
        if type(projected_value) is str:
            # in this case, we want to keep a certain field
            # and choose what should be the value of that field (in case of composed fields)
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
            else:
                # we want to keep a single field
                return {
                    "$project": {
                        field: 1,
                        "_id": 0
                    }
                }

    @classmethod
    def lookup(cls, join_table_name: str, field_table_1: str, field_table_2: str, lookup_field_name: str) -> dict:
        # this is a SQL join between two tables
        return {
            "$lookup": {
                "from": join_table_name,  # the "second" table of the join
                "localField": field_table_2,  # the field of the "second" table to join
                "foreignField": field_table_1,  # the field of the "first" table to join
                "as": lookup_field_name,  # the name of the (new array) field added containing either the joined resource (of the second table) or an empty array if no join could be made for the tuple
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
    def limit(cls, nb: int) -> dict:
        # this is the SQL LIMIT operator
        return {
            "$limit": nb
        }

    @classmethod
    def group_by(cls, group_key: dict | str | None, groups: list) -> dict:
        # this is the SQL GROUP BY operator
        """
        Compute group by (on one or many fields, with one or many operators)
        :param group_key: The $group stage separates documents into groups according to a "group key". The output is one document for each unique group key.
        :param groups: a list of objects for each group by to add to the query (there is only one object is one group by)
        The objects are of the form: {"name": group_by_name, "operator", the aggregation operator (min, max, avg, etc), "field": the field on which to compute the group by
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
    def from_datetime_to_isodate(cls, current_datetime: datetime) -> dict:
        return {"$date": current_datetime.strftime(THE_DATETIME_FORMAT)}
