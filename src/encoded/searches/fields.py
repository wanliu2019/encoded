from encoded.searches.responses import CartQueryResponseWithFacets
from encoded.searches.queries import CartSearchQueryFactoryWithFacets
from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
from snovault.elasticsearch.searches.fields import FiltersResponseField


class CartSearchWithFacetsResponseField(BasicSearchWithFacetsResponseField):
    '''
    Like BasicSearchWithFacetsResponseField but uses CartSearchQueryFactoryWithFacets
    as query builder and CartQueryResponseWithFacets as response.
    '''

    def _build_query(self):
        self.query_builder = CartSearchQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()

    def _execute_query(self):
        self.results = CartQueryResponseWithFacets(
            results=self.query.execute(),
            query_builder=self.query_builder
        )


class CartFiltersResponseField(FiltersResponseField):
    '''
    Like FiltersResponseField but includes cart params as filters.
    '''
    
    def _get_filters_and_search_terms_from_query_string(self):
        return (
            self.get_query_builder()._get_post_filters_with_carts()
            + self.get_params_parser().get_search_term_filters()
            + self.get_params_parser().get_advanced_query_filters()
        )
