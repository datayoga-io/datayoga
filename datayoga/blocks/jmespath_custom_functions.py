import string

from jmespath import functions


# custom functions for Jmespath
class JmespathCustomFunctions(functions.Functions):

    @functions.signature({"types": ["string"]})
    def _func_capitalize(self, arg):
        return string.capwords(str(arg))

    @functions.signature({"types": ["array"]})
    def _func_concat(self, elements):
        return ''.join([str(x) for x in elements])

    @functions.signature({"types": ["string"]})
    def _func_lower(self, element):
        return str(element).lower()

    @functions.signature({"types": ["string"]})
    def _func_upper(self, element):
        return str(element).upper()
