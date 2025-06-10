package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface

public interface GeoPointClusteringAggregatorSupplier {
    Aggregator build(
        String name,
        AggregatorFactories factories,
        ValuesSource.GeoPoint valuesSource,
        int precision,
        double radius,
        double ratio,
        int requiredSize,
        int shardSize,
        AggregationContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metaData
    ) throws IOException;
}
