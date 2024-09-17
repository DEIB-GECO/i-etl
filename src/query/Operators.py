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
    def project(cls, field: str, projected_value: str | dict | None) -> dict:
        if type(projected_value) is str:
            # in this case, we case to keep a certain field
            # and choose what should be the value of that field (in case of composed fields)
            return {
                "$project": {
                    projected_value: "$" + field
                }
            }
        elif type(projected_value) is dict:
            # in case we give a complex projection, e.g., with $split
            return {
                "$project": projected_value
            }
        else:
            # in that case, we only want to keep a certain field
            return {
                "$project": {
                    field: 1
                }
            }

    @classmethod
    def sort(cls, field: str, sort_order: int) -> dict:
        return {
            "$sort": {
                field: sort_order
            }
        }

    @classmethod
    def limit(cls, nb: int) -> dict:
        return {
            "$limit": nb
        }

    @classmethod
    def group_by(cls, group_key: Any, group_by_name: str, operator: str, field) -> dict:
        return {
            "$group": {
                "_id": group_key,
                group_by_name: {
                    operator: field  # $avg: $<the field on which the avg is computed>
                }
            }
        }

    @classmethod
    def unwind(cls, field: str) -> dict:
        return {
            "$unwind": f"${field}"
        }

    @classmethod
    def from_datetime_to_isodate(cls, current_datetime: datetime) -> dict:
        return {"$date": current_datetime.strftime(THE_DATETIME_FORMAT)}
