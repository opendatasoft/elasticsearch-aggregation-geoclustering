package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GeoPointClusteringAggregatorFactory extends ValuesSourceAggregatorFactory<GeoPoint> {

    private final int precision;
    private final double radius;
    private final double ratio;
    private final int requiredSize;
    private final int shardSize;

    GeoPointClusteringAggregatorFactory(
            String name, ValuesSourceConfig<GeoPoint> config, int precision, double radius, double ratio,
            int requiredSize, int shardSize, SearchContext context,
            AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.precision = precision;
        this.radius = radius;
        this.ratio = ratio;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
    }

    @Override
    protected Aggregator createUnmapped(
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData
    ) throws IOException {
        final InternalAggregation aggregation = new InternalGeoPointClustering(name, radius, ratio, requiredSize,
                Collections.<InternalGeoPointClustering.Bucket> emptyList(), pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, context, parent, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
            final GeoPoint valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        return new GeoPointClusteringAggregator(
                name, factories, valuesSource, precision, radius, ratio, requiredSize, shardSize, context, parent,
                pipelineAggregators, metaData);
    }

}
