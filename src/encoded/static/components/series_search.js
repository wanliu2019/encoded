import React from 'react';
import PropTypes from 'prop-types';
import url from 'url';
import { Panel, PanelBody } from '../libs/ui/panel';
import { ResultTable } from './search';
import QueryString from '../libs/query_string';
import * as globals from './globals';

// The series search page includes the following five series
const seriesList = {
    OrganismDevelopmentSeries: {
        title: 'Organism development',
        schema: 'organism_development_series',
    },
    TreatmentTimeSeries: {
        title: 'Treatment time',
        schema: 'treatment_time_series',
    },
    TreatmentConcentrationSeries: {
        title: 'Treatment concentration',
        schema: 'treatment_concentration_series',
    },
    ReplicationTimingSeries: {
        title: 'Replication timing',
        schema: 'replication_timing_series',
    },
    GeneSilencingSeries: {
        title: 'Gene silencing',
        schema: 'gene_silencing_series',
    },
};

// Fetch data from href
function getSeriesData(seriesLink, fetch) {
    return fetch(seriesLink, {
        method: 'GET',
        headers: {
            Accept: 'application/json',
        },
    }).then((response) => {
        if (response.ok) {
            return response.json();
        }
        throw new Error('not ok');
    }).catch((e) => {
        console.log('OBJECT LOAD ERROR: %s', e);
    });
}

// The series search page displays a table of results corresponding to a selected series
// Buttons for each series are displayed like tabs or links
const SeriesSearch = (props, context) => {
    const [parsedUrl, setParsedUrl] = React.useState(url.parse(props.context['@id']));
    const [query, setQuery] = React.useState(new QueryString(parsedUrl.query));
    let originalSeries = 'OrganismDevelopmentSeries';
    if (query.getKeyValues('type')[0]) {
        originalSeries = query.getKeyValues('type')[0];
    }
    const [selectedSeries, setSelectedSeries] = React.useState(originalSeries);
    const [descriptionData, setDescriptionData] = React.useState(null);

    const searchBase = url.parse(context.location_href).search || '';

    const handleClick = React.useCallback((series) => {
        setParsedUrl(url.parse(props.context['@id']));
        setQuery(new QueryString(parsedUrl.query));
        query.deleteKeyValue('type');
        query.addKeyValue('type', series);
        const href = `?${query.format()}`;
        context.navigate(href);
        // Get series description from schema
        const seriesDescriptionHref = `/profiles/${seriesList[series].schema}.json`;
        getSeriesData(seriesDescriptionHref, context.fetch).then((response) => {
            setDescriptionData(response.description);
        });
    }, [context, parsedUrl.query, props.context, query, setParsedUrl, setQuery]);

    const currentRegion = (assembly, region) => {
        if (assembly && region) {
            this.lastRegion = {
                assembly,
                region,
            };
        }
        return SeriesSearch.lastRegion;
    };

    // Select series from tab buttons
    React.useEffect(() => {
        const seriesDescriptionHref = `/profiles/${seriesList[selectedSeries].schema}.json`;
        getSeriesData(seriesDescriptionHref, context.fetch).then((response) => {
            setDescriptionData(response.description);
        });
        if (!(query.getKeyValues('type')[0])) {
            query.addKeyValue('type', originalSeries);
            const href = `?${query.format()}`;
            context.navigate(href);
        }
    }, [context, context.fetch, originalSeries, query, selectedSeries]);

    return (
        <div className="layout">
            <div className="layout__block layout__block--100">
                <div className="ricktextblock block series-search" data-pos="0,0,0">
                    <h1>Functional genomics series</h1>
                    <div className="outer-tab-container">
                        <div className="tab-container series-tabs">
                            {Object.keys(seriesList).map(s => (
                                <button
                                    key={s}
                                    className={`tab-button${selectedSeries === s ? ' selected' : ''}`}
                                    onClick={() => handleClick(s)}
                                >
                                    <div className="tab-inner">
                                        <div className="tab-icon">
                                            <img src={`/static/img/series/${s.replace('Series', '')}.svg`} alt={s} />
                                        </div>
                                        {seriesList[s].title}
                                    </div>
                                </button>
                            ))}
                        </div>
                        <div className="tab-border" />
                    </div>
                    <div className="tab-body">
                        <div className="tab-description">{descriptionData}</div>
                        <div className="series-wrapper">
                            <Panel>
                                <PanelBody>
                                    <ResultTable
                                        {...props}
                                        searchBase={searchBase}
                                        onChange={context.navigate}
                                        currentRegion={currentRegion}
                                        seriesFlag
                                    />
                                </PanelBody>
                            </Panel>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

SeriesSearch.propTypes = {
    context: PropTypes.object.isRequired,
};

SeriesSearch.contextTypes = {
    location_href: PropTypes.string,
    navigate: PropTypes.func,
    fetch: PropTypes.func,
};

globals.contentViews.register(SeriesSearch, 'SeriesSearch');
