package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.List;

public class ParsedGeoPointClustering extends ParsedMultiBucketAggregation<ParsedGeoPointClustering.ParsedBucket>
        implements GeoPointClustering {

    @Override
    public String getType() {
        return GeoPointClusteringAggregationBuilder.NAME;
    }

    @Override
    public List<? extends GeoPointClustering.Bucket> getBuckets() {
        return buckets;
    }

    private static ObjectParser<ParsedGeoPointClustering, Void> PARSER = new ObjectParser<>(
            ParsedGeoPointClustering.class.getSimpleName(), true, ParsedGeoPointClustering::new);
    static {
        declareMultiBucketAggregationFields(PARSER, ParsedBucket::fromXContent, ParsedBucket::fromXContent);
    }

    public static ParsedGeoPointClustering fromXContent(XContentParser parser, String name) throws IOException {
        ParsedGeoPointClustering aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket
            implements GeoPointClustering.Bucket {

        private String geohashAsString;

        @Override
        public GeoPoint getKey() {
            return GeoPoint.fromGeohash(geohashAsString);
        }

        @Override
        public String getKeyAsString() {
            return geohashAsString;
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), geohashAsString);
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseXContent(parser, false,
                    ParsedBucket::new, (p, bucket) -> bucket.geohashAsString = p.textOrNull());
        }
    }
}
