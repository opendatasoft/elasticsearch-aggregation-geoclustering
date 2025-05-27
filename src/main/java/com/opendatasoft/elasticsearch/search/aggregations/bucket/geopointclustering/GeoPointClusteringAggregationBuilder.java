package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GeoPointClusteringAggregationBuilder extends ValuesSourceAggregationBuilder<GeoPointClusteringAggregationBuilder> {
    public static final String NAME = "geo_point_clustering";
    public static final ValuesSourceRegistry.RegistryKey<GeoPointClusteringAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, GeoPointClusteringAggregatorSupplier.class);

    static final int DEFAULT_ZOOM = 1;
    static final int DEFAULT_EXTENT = 256;
    static final int DEFAULT_MAX_NUM_CELLS = 10000;
    static final int DEFAULT_RADIUS = 40;
    static final double DEFAULT_RATIO = 0;

    private static final ObjectParser<GeoPointClusteringAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoPointClusteringAggregationBuilder.NAME);
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
        PARSER.declareInt(GeoPointClusteringAggregationBuilder::size, GeoPointClusteringParams.FIELD_SIZE);
        PARSER.declareInt(GeoPointClusteringAggregationBuilder::shardSize, GeoPointClusteringParams.FIELD_SHARD_SIZE);
        PARSER.declareInt(GeoPointClusteringAggregationBuilder::zoom, GeoPointClusteringParams.FIELD_ZOOM);
        PARSER.declareInt(GeoPointClusteringAggregationBuilder::extent, GeoPointClusteringParams.FIELD_EXTENT);
        PARSER.declareInt(GeoPointClusteringAggregationBuilder::radius, GeoPointClusteringParams.FIELD_RADIUS);
        PARSER.declareDouble(GeoPointClusteringAggregationBuilder::ratio, GeoPointClusteringParams.FIELD_RATIO);
    }

    public static GeoPointClusteringAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoPointClusteringAggregationBuilder(aggregationName), null);
    }

    private int zoom = DEFAULT_ZOOM;
    private int radius = DEFAULT_RADIUS;
    private int extent = DEFAULT_EXTENT;
    private int requiredSize = DEFAULT_MAX_NUM_CELLS;
    private int shardSize = -1;
    private double ratio = DEFAULT_RATIO;

    public GeoPointClusteringAggregationBuilder(String name) {
        super(name);
    }

    protected GeoPointClusteringAggregationBuilder(
        GeoPointClusteringAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metaData
    ) {
        super(clone, factoriesBuilder, metaData);
        this.zoom = clone.zoom;
        this.radius = clone.radius;
        this.extent = clone.extent;
        this.ratio = clone.ratio;
        this.requiredSize = clone.requiredSize;
        this.shardSize = clone.shardSize;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoPointClusteringAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public GeoPointClusteringAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        zoom = in.readInt();
        radius = in.readInt();
        extent = in.readInt();
        ratio = in.readDouble();
        requiredSize = in.readVInt();
        shardSize = in.readVInt();
    }

    /**
     * Write to stream.
     */
    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeInt(zoom);
        out.writeInt(radius);
        out.writeInt(extent);
        out.writeDouble(ratio);
        out.writeVInt(requiredSize);
        out.writeVInt(shardSize);
    }

    public GeoPointClusteringAggregationBuilder zoom(int zoom) {
        this.zoom = GeoPointClusteringParams.checkZoom(zoom);
        return this;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    public int zoom() {
        return zoom;
    }

    public GeoPointClusteringAggregationBuilder extent(int extent) {
        if (extent <= 0) {
            throw new IllegalArgumentException("[extent] must be greater than 0. Found [" + extent + "] in [" + name + "]");
        }
        this.extent = extent;
        return this;
    }

    public int extent() {
        return extent;
    }

    public GeoPointClusteringAggregationBuilder radius(int radius) {
        if (radius <= 0) {
            throw new IllegalArgumentException("[radius] must be greater than 0. Found [" + radius + "] in [" + name + "]");
        }
        this.radius = radius;
        return this;
    }

    public GeoPointClusteringAggregationBuilder ratio(double ratio) {
        if (ratio > 2) {
            throw new IllegalArgumentException("[rati] must be lower or equal than 2. Found [" + ratio + "] in [" + name + "]");
        }
        this.ratio = ratio;
        return this;
    }

    public GeoPointClusteringAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        this.requiredSize = size;
        return this;
    }

    public int size() {
        return requiredSize;
    }

    public GeoPointClusteringAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        int shardSize = this.shardSize;

        int requiredSize = this.requiredSize;

        if (shardSize < 0) {
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize);
        }

        if (requiredSize <= 0 || shardSize <= 0) {
            throw new ElasticsearchException(
                "parameters [required_size] and [shard_size] must be >0 in geohash_grid aggregation [" + name + "]."
            );
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        int pixelRadius = this.radius;

        double mapWidthHeight = extent * Math.pow(2, zoom);

        // Ground resolution == meter/pixel.
        double groundResolution = GeoUtils.EARTH_EQUATOR / mapWidthHeight;

        double radius = pixelRadius * groundResolution;

        // Compute a precision value for radius.
        // The precision is the geohash level where radius in meter is smaller than the geohash cell dimension
        // https://www.elastic.co/guide/en/elasticsearch/guide/current/geohashes.html
        // This precision is used to create geohash grid buckets before merging them.
        int precision = GeoUtils.geoHashLevelsForPrecision(radius);

        return new GeoPointClusteringAggregatorFactory(
            name,
            config,
            precision,
            radius,
            ratio,
            requiredSize,
            shardSize,
            context,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(GeoPointClusteringParams.FIELD_ZOOM.getPreferredName(), zoom);
        builder.field(GeoPointClusteringParams.FIELD_EXTENT.getPreferredName(), extent);
        builder.field(GeoPointClusteringParams.FIELD_RADIUS.getPreferredName(), radius);
        builder.field(GeoPointClusteringParams.FIELD_RATIO.getPreferredName(), ratio);
        builder.field(GeoPointClusteringParams.FIELD_SIZE.getPreferredName(), requiredSize);
        if (shardSize > -1) {
            builder.field(GeoPointClusteringParams.FIELD_SHARD_SIZE.getPreferredName(), shardSize);
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        GeoPointClusteringAggregationBuilder other = (GeoPointClusteringAggregationBuilder) obj;
        if (zoom != other.zoom) {
            return false;
        }
        if (extent != other.extent) {
            return false;
        }
        if (radius != other.radius) {
            return false;
        }
        if (ratio != other.ratio) {
            return false;
        }
        if (requiredSize != other.requiredSize) {
            return false;
        }
        if (shardSize != other.shardSize) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoom, extent, radius, ratio, requiredSize, shardSize);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoPointClusteringAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (
                name,
                factories,
                valuesSource,
                precision,
                radius,
                ratio,
                requiredSize,
                shardSize,
                context,
                parent,
                cardinality,
                metaData) -> null,
            true
        );
    }

}
