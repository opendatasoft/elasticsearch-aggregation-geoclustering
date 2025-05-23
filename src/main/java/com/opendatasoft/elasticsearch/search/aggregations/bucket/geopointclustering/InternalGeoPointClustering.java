package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class InternalGeoPointClustering extends InternalMultiBucketAggregation<
    InternalGeoPointClustering,
    InternalGeoPointClustering.Bucket> implements GeoPointClustering {

    static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements GeoPointClustering.Bucket, Comparable<Bucket> {

        protected long geohashAsLong;
        protected GeoPoint centroid;
        protected long docCount;
        long bucketOrd;
        protected InternalAggregations subAggregations;
        protected boolean visited = false;
        protected Set<Long> geohashesList;

        Bucket(long geohashAsLong, GeoPoint centroid, long docCount, InternalAggregations subAggregations) {
            this.docCount = docCount;
            this.centroid = centroid;
            this.subAggregations = subAggregations;
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
            subAggregations = InternalAggregations.readFrom(in);
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(geohashAsLong);
            out.writeVLong(docCount);
            out.writeLong(encodeLatLon(centroid.lat(), centroid.lon()));
            out.writeBoolean(visited);
            subAggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            return Geohash.stringEncode(geohashAsLong);
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
            return subAggregations;
        }

        @Override
        public int compareTo(Bucket other) {
            return Long.compare(this.geohashAsLong, other.geohashAsLong);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("geohash_grids", geohashesList.stream().map(Geohash::stringEncode).collect(Collectors.toList()));
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field("centroid", centroid);
            subAggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bucket bucket = (Bucket) o;
            return geohashAsLong == bucket.geohashAsLong
                && docCount == bucket.docCount
                && Objects.equals(subAggregations, bucket.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(geohashAsLong, docCount, subAggregations);
        }

    }

    private final double radius;
    private final double ratio;
    private final int requiredSize;
    private final List<Bucket> buckets;

    InternalGeoPointClustering(
        String name,
        double radius,
        double ratio,
        int requiredSize,
        List<Bucket> buckets,
        Map<String, Object> metaData
    ) {
        super(name, metaData);
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

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(radius);
        out.writeDouble(ratio);
        writeSize(requiredSize, out);
        out.writeList(buckets);
    }

    public static long encodeLatLon(double lat, double lon) {
        return (Integer.toUnsignedLong(GeoEncodingUtils.encodeLatitude(lat)) << 32) | Integer.toUnsignedLong(
            GeoEncodingUtils.encodeLongitude(lon)
        );
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
        return new InternalGeoPointClustering(this.name, this.radius, this.ratio, this.requiredSize, buckets, this.metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.geohashAsLong, prototype.centroid, prototype.docCount, aggregations);
    }

    @Override
    public List<InternalGeoPointClustering.Bucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    /**
     * Reduces the given aggregations to a single one and returns it.
     */
    @Override
    public InternalGeoPointClustering reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<Bucket>> buckets = null;

        // collect and deduplicate buckets from each shard
        // cast an array of InternalGeoPointClustering agg to a HashMap of buckets
        // buckets are deduplicated
        for (InternalAggregation shard_aggregation : aggregations) {
            InternalGeoPointClustering shard_clusters = (InternalGeoPointClustering) shard_aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(shard_clusters.buckets.size(), reduceContext.bigArrays());
            }
            for (Bucket bucket : shard_clusters.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.geohashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        // cast the buckets HashMap to a Lucene PriorityQueue
        // the priority queue is used to retain the top N bucket
        final int size = Math.toIntExact(reduceContext.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size()));
        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        for (LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            List<Bucket> sameCellBuckets = cursor.value;
            ordered.insertWithOverflow(reduceBucket(sameCellBuckets, reduceContext));
        }
        buckets.close();

        // cast Lucene PriorityQueue to GeoPointClustering buckets
        Bucket[] candidate_clusters = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            candidate_clusters[i] = ordered.pop();
        }

        // merge buckets if needed, according to radius and ratio plugin parameters
        List<Bucket> final_clusters = new ArrayList<>();
        for (Bucket bucket : candidate_clusters) {
            if (bucket.visited) {
                continue;
            }

            bucket.visited = true;
            List<Bucket> revisit = new ArrayList<>();
            for (Bucket potentialNeighbor : candidate_clusters) {
                computeDistance(bucket, potentialNeighbor, revisit, reduceContext);
            }
            for (Bucket potentialNeighbor : revisit) {
                computeDistance(bucket, potentialNeighbor, null, reduceContext);
            }
            final_clusters.add(bucket);
        }

        return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, final_clusters, getMetadata());
    }

    @Override
    public Bucket reduceBucket(List<Bucket> buckets, ReduceContext context) {
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        long docCount = 0;
        double centroidLat = 0;
        double centroidLon = 0;
        long geohashAsLong = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            centroidLat += bucket.centroid.getLat() * bucket.docCount;
            centroidLon += bucket.centroid.getLon() * bucket.docCount;
            aggregationsList.add(bucket.subAggregations);
            geohashAsLong = bucket.geohashAsLong;
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return new Bucket(geohashAsLong, new GeoPoint(centroidLat / docCount, centroidLon / docCount), docCount, aggs);
    }

    /**
     * Merge buckets (clusters) if their distance is below some threshold, defined from radius plugin parameter.
     * It allows to not stuck clusters to the geohash grid.
     * E.g. at zoom level 1, points in France metropolitan area will output 4 clusters instead of 1 without this merge process,
     * because geohash cells g, u, e, s would be used.
     */
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

        double fixedRadius = radius * Math.cos(Math.toRadians(avgLat));

        if (neighborDistance <= fixedRadius) {
            potentialNeighbor.visited = true;
            long mergedDocCount = bucket.docCount + potentialNeighbor.docCount;
            double newCentroidLat = (bucket.centroid.getLat() * bucket.docCount + potentialNeighbor.centroid.getLat()
                * potentialNeighbor.docCount) / mergedDocCount;
            double newCentroidLon = (bucket.centroid.getLon() * bucket.docCount + potentialNeighbor.centroid.getLon()
                * potentialNeighbor.docCount) / mergedDocCount;
            bucket.centroid = new GeoPoint(newCentroidLat, newCentroidLon);

            bucket.docCount = mergedDocCount;
            List<InternalAggregations> aggregationsList = new ArrayList<>();
            aggregationsList.add(bucket.subAggregations);
            aggregationsList.add(potentialNeighbor.subAggregations);
            bucket.subAggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            bucket.geohashesList.add(potentialNeighbor.geohashAsLong);
        } else if (revisit != null && ratio > 0 && neighborDistance / fixedRadius < ratio) {
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

    @Override
    public int hashCode() {
        return Objects.hash(requiredSize, buckets);
    }

    @Override
    public boolean equals(Object obj) {
        InternalGeoPointClustering other = (InternalGeoPointClustering) obj;
        return Objects.equals(requiredSize, other.requiredSize) && Objects.equals(buckets, other.buckets);
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
