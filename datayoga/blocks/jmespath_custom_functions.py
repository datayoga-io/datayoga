import jmespath
from jmespath import functions

#
# custom function for Jmespath


class JmespathCustomFunctions(functions.Functions):

    @functions.signature({'types': ['array']})
    def _func_concat(self, elements):
        return ''.join([str(x) for x in elements])
