package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.locationtech.spatial4j.distance.DistanceUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class InternalGeoPointClustering extends InternalMultiBucketAggregation
        <InternalGeoPointClustering, InternalGeoPointClustering.Bucket> implements GeoPointClustering {
    static class Bucket extends
            InternalMultiBucketAggregation.InternalBucket implements GeoPointClustering.Bucket, Comparable<Bucket> {

        protected long geohashAsLong;
        protected GeoPoint centroid;
        protected long docCount;
        protected InternalAggregations aggregations;
        protected boolean visited = false;
        protected Set<Long> geohashesList;

        Bucket(long geohashAsLong, GeoPoint centroid, long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.centroid = centroid;
            this.aggregations = aggregations;
            this.geohashAsLong = geohashAsLong;
            this.geohashesList = new HashSet<>();
            this.geohashesList.add(geohashAsLong);
        }

        /**
         * Read from a stream.
         */
        private Bucket(StreamInput in) throws IOException {
            geohashAsLong = in.readLong();
            docCount = in.readVLong();
            final long hash = in.readLong();
            centroid = new GeoPoint(decodeLatitude(hash), decodeLongitude(hash));
            visited = in.readBoolean();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(geohashAsLong);
            out.writeVLong(docCount);
            out.writeLong(encodeLatLon(centroid.lat(), centroid.lon()));
            out.writeBoolean(visited);
            aggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            return GeoHashUtils.stringEncode(geohashAsLong);
        }

        @Override
        public GeoPoint getKey() {
            return GeoPoint.fromGeohash(geohashAsLong);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public int compareTo(Bucket other) {
            if (this.geohashAsLong > other.geohashAsLong) {
                return 1;
            }
            if (this.geohashAsLong < other.geohashAsLong) {
                return -1;
            }
            return 0;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            long docCount = 0;
            double centroidLat = 0;
            double centroidLon = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                centroidLat += bucket.centroid.getLat() * bucket.docCount;
                centroidLon += bucket.centroid.getLon() * bucket.docCount;
                aggregationsList.add(bucket.aggregations);
            }
            final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return new Bucket(geohashAsLong, new GeoPoint(
                    centroidLat / docCount, centroidLon / docCount), docCount, aggs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("geohash_grids",
                    geohashesList.stream().map(GeoHashUtils::stringEncode).collect(Collectors.toList()));
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field("centroid", centroid);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bucket bucket = (Bucket) o;
            return geohashAsLong == bucket.geohashAsLong &&
                    docCount == bucket.docCount &&
                    Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(geohashAsLong, docCount, aggregations);
        }

    }

    private final double radius;
    private final double ratio;
    private final int requiredSize;
    private final List<Bucket> buckets;

    InternalGeoPointClustering(
            String name, double radius, double ratio, int requiredSize, List<Bucket> buckets,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.radius = radius;
        this.ratio = ratio;
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoPointClustering(StreamInput in) throws IOException {
        super(in);
        radius = in.readDouble();
        ratio = in.readDouble();
        requiredSize = readSize(in);
        buckets = in.readList(Bucket::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(radius);
        out.writeDouble(ratio);
        writeSize(requiredSize, out);
        out.writeList(buckets);
    }

    public static long encodeLatLon(double lat, double lon) {
        return (Integer.toUnsignedLong(GeoEncodingUtils.encodeLatitude(lat)) << 32) |
                Integer.toUnsignedLong(GeoEncodingUtils.encodeLongitude(lon));
    }

    public static double decodeLatitude(long encodedLatLon) {
        return GeoEncodingUtils.decodeLatitude((int) (encodedLatLon >>> 32));
    }

    public static double decodeLongitude(long encodedLatLon) {
        return GeoEncodingUtils.decodeLongitude((int) (encodedLatLon & 0xFFFFFFFFL));
    }

    @Override
    public String getWriteableName() {
        return GeoPointClusteringAggregationBuilder.NAME;
    }

    @Override
    public InternalGeoPointClustering create(List<Bucket> buckets) {
        return new InternalGeoPointClustering(
                this.name, this.radius, this.ratio, this.requiredSize, buckets, this.pipelineAggregators(),
                this.metaData
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.geohashAsLong, prototype.centroid, prototype.docCount, aggregations);
    }

    @Override
    public List<InternalGeoPointClustering.Bucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalGeoPointClustering doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoPointClustering grid = (InternalGeoPointClustering) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (Bucket bucket : grid.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.geohashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = Math.toIntExact(
                reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size())
        );
        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        for (LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            List<Bucket> sameCellBuckets = cursor.value;
            ordered.insertWithOverflow(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
        }
        buckets.close();

        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }

        List<Bucket> clusters = new ArrayList<>();

        for (Bucket bucket: list) {

            if (bucket.visited) {
                continue;
            }

            bucket.visited = true;

            List<Bucket> revisit = new ArrayList<>();

            for (Bucket potentialNeighbor: list) {
                computeDistance(bucket, potentialNeighbor, revisit, reduceContext);

            }

            for (Bucket potentialNeighbor: revisit) {
                computeDistance(bucket, potentialNeighbor, null, reduceContext);
            }

            clusters.add(bucket);

        }

        return new InternalGeoPointClustering(
                getName(), radius, ratio, requiredSize, clusters, pipelineAggregators(), getMetaData()
        );
    }

    private void computeDistance(Bucket bucket, Bucket potentialNeighbor, List<Bucket> revisit, ReduceContext reduceContext) {
        if (potentialNeighbor.visited) {
            return;
        }

        double neighborDistance = GeoUtils.arcDistance(
                bucket.centroid.lat(),
                bucket.centroid.lon(),
                potentialNeighbor.centroid.lat(),
                potentialNeighbor.centroid.lon()
        );

        double avgLat = (bucket.centroid.lat() + potentialNeighbor.centroid.lat()) / 2;

        double fixedRadius = radius * Math.cos(DistanceUtils.toRadians(avgLat));

        if (neighborDistance <= fixedRadius) {
            potentialNeighbor.visited = true;
            long mergedDocCount = bucket.docCount + potentialNeighbor.docCount;
            double newCentroidLat = (bucket.centroid.getLat() * bucket.docCount +
                    potentialNeighbor.centroid.getLat() * potentialNeighbor.docCount) / mergedDocCount;
            double newCentroidLon = (bucket.centroid.getLon() * bucket.docCount +
                    potentialNeighbor.centroid.getLon() * potentialNeighbor.docCount) / mergedDocCount;
            bucket.centroid = new GeoPoint(newCentroidLat, newCentroidLon);

            bucket.docCount = mergedDocCount;
            List<InternalAggregations> aggregationsList = new ArrayList<>();
            aggregationsList.add(bucket.aggregations);
            aggregationsList.add(potentialNeighbor.aggregations);
            bucket.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            bucket.geohashesList.add(potentialNeighbor.geohashAsLong);
        } else if (revisit != null && ratio > 0 && neighborDistance / fixedRadius < ratio ) {
            revisit.add(potentialNeighbor);
        }
    }


    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    // package protected for testing
    int getRequiredSize() {
        return requiredSize;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(requiredSize, buckets);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalGeoPointClustering other = (InternalGeoPointClustering) obj;
        return Objects.equals(requiredSize, other.requiredSize) &&
                Objects.equals(buckets, other.buckets);
    }

    static class BucketPriorityQueue extends PriorityQueue<Bucket> {

        BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Bucket o1, Bucket o2) {
            int res = o1.compareTo(o2);
            return res < 0;
        }
    }
}
