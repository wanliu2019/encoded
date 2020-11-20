from encoded.searches.mixins import CartAggsToFacetsMixin
from snovault.elasticsearch.searches.responses import BasicQueryResponseWithFacets


class CartQueryResponseWithFacets(CartAggsToFacetsMixin, BasicQueryResponseWithFacets):
    '''
    Like BasicQueryResponseWithFacets but uses CartAggsToFacetsMixin instead of AggsToFacetsMixin.
    '''
    
    def __init__(self, results, query_builder, *args, **kwargs):
        super().__init__(results, query_builder, *args, **kwargs)
