/**
 * Renders cart-related controls at the top of search results.
 */
import React from 'react';
import PropTypes from 'prop-types';
import url from 'url';
import { CartAddAllSearch } from './add_multiple';
import { isAllowedElementsPossible } from './util';


/**
 * Controls at the top of search result lists. Show the Add All button if the search results could
 * contain experiments.
 */
const CartSearchControls = ({ searchResults }) => {
    // Don't allow cart controls when viewing a cart's contents with the /cart-search/ path.
    const parsedUrl = url.parse(searchResults['@id']);
    if (parsedUrl.pathname !== '/cart-search/' && isAllowedElementsPossible(searchResults.filters)) {
        return (
            <div className="cart__search-controls">
                <CartAddAllSearch searchResults={searchResults} />
            </div>
        );
    }
    return null;
};

CartSearchControls.propTypes = {
    /** Search results object used to render search page */
    searchResults: PropTypes.object.isRequired,
};

export default CartSearchControls;
