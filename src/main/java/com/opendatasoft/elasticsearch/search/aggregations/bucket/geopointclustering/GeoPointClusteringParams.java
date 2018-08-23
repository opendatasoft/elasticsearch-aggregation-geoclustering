package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.common.ParseField;

/**
 * Encapsulates relevant parameter defaults and validations for the geo point clustering aggregation.
 */
final class GeoPointClusteringParams {

    /* recognized field names in JSON */
    static final ParseField FIELD_SIZE = new ParseField("size");
    static final ParseField FIELD_SHARD_SIZE = new ParseField("shard_size");
    static final ParseField FIELD_ZOOM = new ParseField("zoom");
    static final ParseField FIELD_EXTENT = new ParseField("extent");
    static final ParseField FIELD_RADIUS = new ParseField("radius");
    static final ParseField FIELD_RATIO = new ParseField("ratio");

    static int checkZoom(int zoom) {
        if ((zoom < 1) || (zoom > 25)) {
            throw new IllegalArgumentException("Invalid geohash aggregation zoom of " + zoom
                    + ". Must be between 1 and 25.");
        }
        return zoom;
    }

    private GeoPointClusteringParams() {
        throw new AssertionError("No instances intended");
    }
}
