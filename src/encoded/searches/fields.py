from encoded.searches.queries import CartSearchQueryFactoryWithFacets
from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
from snovault.elasticsearch.searches.fields import FiltersResponseField


class CartSearchWithFacetsResponseField(BasicSearchWithFacetsResponseField):
    '''
    Like BasicSearchWithFacetsResponseField but uses CartSearchQueryFactoryWithFace
    as query builder.
    '''

    def _build_query(self):
        self.query_builder = CartSearchQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()


class CartFiltersResponseField(FiltersResponseField):
    '''
    Like FiltersResponseField but includes cart params as filters.
    '''
    
    def _get_filters_and_search_terms_from_query_string(self):
        return (
            super()._get_filters_and_search_terms_from_query_string()
            + self.get_params_parser().get_cart()
        )
