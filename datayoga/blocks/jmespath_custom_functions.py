import string

from jmespath import functions


# custom functions for Jmespath
class JmespathCustomFunctions(functions.Functions):

    @functions.signature({"types": ["string", "null"]})
    def _func_capitalize(self, arg):
        return string.capwords(str(arg)) if arg is not None else None

    @functions.signature({"types": ["array"]})
    def _func_concat(self, elements):
        return ''.join([str(x) for x in elements])

    @functions.signature({"types": ["string", "null"]})
    def _func_lower(self, element):
        return str(element).lower() if element is not None else None

    @functions.signature({"types": ["string", "null"]})
    def _func_upper(self, element):
        return str(element).upper() if element is not None else None

    @functions.signature({"types": ["string", "null"]},
                         {"types": ["string"]},
                         {"types": ["string"]})
    def _func_replace(self, element, old_value, new_value):
        return str(element).replace(old_value, new_value) if element is not None else None
