from snovault.elasticsearch.searches.interfaces import CART_KEY
from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets


class CartSearchQueryFactoryWithFacets(BasicSearchQueryFactoryWithFacets):
    '''
    Like BasicSearchQueryFactoryWithFacets but expands cart params to @ids and
    adds to post_filters.
    '''

    def _get_post_filters_with_carts(self):
        return (
            super()._get_post_filters()
            + self.params_parser.get_cart()
        )
    
    def _get_post_filters(self):
        return (
            super()._get_post_filters()
            + self.kwargs.get(CART_KEY).as_params()
        )
