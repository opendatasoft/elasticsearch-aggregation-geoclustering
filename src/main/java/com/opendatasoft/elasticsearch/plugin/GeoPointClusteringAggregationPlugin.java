package com.opendatasoft.elasticsearch.plugin;

import com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering.GeoPointClusteringAggregationBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering.InternalGeoPointClustering;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;

public class GeoPointClusteringAggregationPlugin extends Plugin implements SearchPlugin {
    @Override
    public ArrayList<SearchPlugin.AggregationSpec> getAggregations() {
        ArrayList<SearchPlugin.AggregationSpec> r = new ArrayList<>();

        r.add(
            new AggregationSpec(
                GeoPointClusteringAggregationBuilder.NAME,
                GeoPointClusteringAggregationBuilder::new,
                GeoPointClusteringAggregationBuilder::parse
            ).addResultReader(InternalGeoPointClustering::new)
                .setAggregatorRegistrar(GeoPointClusteringAggregationBuilder::registerAggregators)
        );

        return r;
    }
}
