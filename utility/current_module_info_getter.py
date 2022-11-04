import inspect


def get_current_frame():
    return inspect.getouterframes(inspect.currentframe())[1]


def get_current_line_no():
    return inspect.getlineno(inspect.getouterframes(inspect.currentframe())[1][0])


def get_current_module_name():
    return inspect.getouterframes(inspect.currentframe())[1][1]


def get_current_function_name():
    return inspect.getouterframes(inspect.currentframe())[1][3]
