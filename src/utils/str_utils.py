from constants.defaults import SNOMED_OPERATORS_LIST


def process_spaces(input_string: str) -> str:
    # this removes trailing spaces and replaces sequences of several spaces by only one
    return ' '.join(input_string.split())


def remove_specific_tokens(input_string: str, tokens: list) -> str:
    for token in tokens:
        input_string = input_string.replace(token, "")
    return input_string


def remove_operators_in_strings(input_string: str) -> str:
    pipe_double_quote_indices = [i if char in ["|", "\""] else -1 for i, char in enumerate(input_string)]
    pipe_double_quote_indices = [i for i in pipe_double_quote_indices if i != -1]

    i = 0
    if len(pipe_double_quote_indices) % 2 != 0 or len(pipe_double_quote_indices) == 0:
        # there is an operator (|, ', or ") which does not have its sibling
        # or there are no operators in that code, so we can return immediately
        return input_string
    else:
        final_text = input_string
        while i < len(pipe_double_quote_indices):
            first_op_index = pipe_double_quote_indices[i]
            second_op_index = pipe_double_quote_indices[i+1]
            text_between_op = input_string[(first_op_index+1): second_op_index]
            new_text_between_op = remove_specific_tokens(input_string=text_between_op, tokens=["(property)", "- finding", "-finding"])
            for operator in SNOMED_OPERATORS_LIST:
                new_text_between_op = new_text_between_op.replace(operator, " ")
            final_text = final_text.replace(text_between_op, new_text_between_op)
            i = i+2
        return final_text
