package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Constructs the per-shard aggregator instance for GeoPointClustering aggregation.
 */
public class GeoPointClusteringAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final int precision;
    private final double radius;
    private final double ratio;
    private final int requiredSize;
    private final int shardSize;

    GeoPointClusteringAggregatorFactory(
            String name,
            ValuesSourceConfig config,
            int precision,
            double radius,
            double ratio,
            int requiredSize,
            int shardSize,
            QueryShardContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.precision = precision;
        this.radius = radius;
        this.ratio = ratio;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
    }

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that
     * doesn't have values.
     */
    @Override
    protected Aggregator createUnmapped(
            SearchContext searchContext,
            Aggregator parent,
            Map<String,
            Object> metaData
    ) throws IOException {
        final InternalAggregation aggregation = new InternalGeoPointClustering(name, radius, ratio, requiredSize,
                Collections.<InternalGeoPointClustering.Bucket> emptyList(),  metaData);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
            SearchContext searchContext,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metaData
    ) throws IOException {
        return new GeoPointClusteringAggregator(
                name,
                factories,
                (ValuesSource.GeoPoint) config.getValuesSource(),
                precision,
                radius,
                ratio,
                requiredSize,
                shardSize,
                searchContext,
                parent,
                cardinality,
                metaData);
    }

}
