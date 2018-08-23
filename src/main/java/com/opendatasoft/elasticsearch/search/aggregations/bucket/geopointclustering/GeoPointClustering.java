package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code geopoint_clustering} aggregation.
 * precision.
 */
public interface GeoPointClustering extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a {@code geopoint_clustering} cell. The key of the bucket is the {@code geohash} of the cell
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {
    }

    /**
     * @return  The buckets of this aggregation (each bucket representing a cluster of points)
     */
    @Override
    List<? extends Bucket> getBuckets();
}
