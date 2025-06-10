package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorsReducer;
import org.elasticsearch.search.aggregations.InternalAggregations;

/**
 *  Class for reducing a list of {@link com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering.InternalGeoPointClustering.Bucket} to a single {@link InternalAggregations} and the number of documents.
 *  Inspired by {@link org.elasticsearch.search.aggregations.bucket.BucketReducer} but adapted for the
 *  GeoPointClustering aggregation.
 *  Particularity: it calculates a weighted centroid based on the document counts of the buckets. This weighted centroid
 *  is then used to create the final cluster centroid in the reduce phase InternalGeoPointClustering::AggregatorReducer.get().
 *
 */
public final class BucketReducer implements Releasable {

    private final AggregatorsReducer aggregatorsReducer;
    private final InternalGeoPointClustering.Bucket proto;
    private long count = 0;
    private double centroidLat = 0;
    private double centroidLon = 0;

    /**
     * Untouched from the original BucketReducer.
     */
    public BucketReducer(InternalGeoPointClustering.Bucket proto, AggregationReduceContext context, int size) {
        this.aggregatorsReducer = new AggregatorsReducer(proto.getAggregations(), context, size);
        this.proto = proto;
    }

    /**
     * Accepts a bucket and updates the count and weighted centroid.
     * The centroid is calculated as the sum of latitudes and longitudes
     * weighted by the document count of each bucket.
     *
     * @param bucket the bucket to accept
     */
    public void accept(InternalGeoPointClustering.Bucket bucket) {
        count += bucket.getDocCount();
        centroidLat += bucket.centroid.getLat() * bucket.getDocCount();
        centroidLon += bucket.centroid.getLon() * bucket.getDocCount();
        aggregatorsReducer.accept(bucket.getAggregations());
    }

    /**
     * Untouched from the original BucketReducer.
     */
    public InternalGeoPointClustering.Bucket getProto() {
        return proto;
    }

    /**
     * Untouched from the original BucketReducer.
     */
    public InternalAggregations getAggregations() {
        return aggregatorsReducer.get();
    }

    /**
     * Untouched from the original BucketReducer.
     */
    public long getDocCount() {
        return count;
    }

    /**
     * Returns the weighted centroid of the buckets processed so far.
     * The centroid is calculated as the sum of latitudes and longitudes
     * weighted by the document count of each bucket.
     *
     * @return the weighted centroid as a {@link GeoPoint}
     */
    public GeoPoint getWeightedCentroid() {
        return new GeoPoint(
                centroidLat / count, centroidLon / count);
    }

    /**
     * Untouched from the original BucketReducer.
     */
    @Override
    public void close() {
        Releasables.close(aggregatorsReducer);
    }
}
