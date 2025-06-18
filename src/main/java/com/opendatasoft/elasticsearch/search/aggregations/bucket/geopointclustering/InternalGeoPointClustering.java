package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class InternalGeoPointClustering extends InternalMultiBucketAggregation<
    InternalGeoPointClustering,
    InternalGeoPointClustering.Bucket> implements GeoPointClustering {

    static class Bucket extends InternalMultiBucketAggregation.InternalBucketWritable
        implements
            GeoPointClustering.Bucket,
            Comparable<Bucket> {

        protected long hashAsLong;
        protected GeoPoint centroid;
        protected long docCount;
        protected InternalAggregations aggregations;  // sub-aggregations for this bucket
        protected boolean visited = false;
        protected Set<Long> geohashesList;

        Bucket(long hashAsLong, GeoPoint centroid, long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.centroid = centroid;
            this.aggregations = aggregations;
            this.hashAsLong = hashAsLong;
            this.geohashesList = new HashSet<>();
            this.geohashesList.add(hashAsLong);
        }

        /**
         * Read from a stream.
         */
        private Bucket(StreamInput in) throws IOException {
            hashAsLong = in.readLong();
            docCount = in.readVLong();
            final long hash = in.readLong();
            centroid = new GeoPoint(decodeLatitude(hash), decodeLongitude(hash));
            visited = in.readBoolean();
            aggregations = InternalAggregations.readFrom(in);
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(hashAsLong);
            out.writeVLong(docCount);
            out.writeLong(encodeLatLon(centroid.lat(), centroid.lon()));
            out.writeBoolean(visited);
            aggregations.writeTo(out);
        }

        /**
         * Returns the value used as the final key for buckets.
         */
        @Override
        public String getKeyAsString() {
            return Geohash.stringEncode(hashAsLong);
        }

        @Override
        public GeoPoint getKey() {
            return GeoPoint.fromGeohash(hashAsLong);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        public long hashAsLong() {
            return hashAsLong;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public int compareTo(Bucket other) {
            return Long.compare(this.hashAsLong, other.hashAsLong);
        }

        final void bucketToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("geohash_grids", geohashesList.stream().map(Geohash::stringEncode).collect(Collectors.toList()));
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field("centroid", centroid);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bucket bucket = (Bucket) o;
            return hashAsLong == bucket.hashAsLong && docCount == bucket.docCount && Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashAsLong, docCount, aggregations);
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
        buckets = in.readCollectionAsList(getBucketReader());
    }

    /**
     * Write (serialize) partial results to a stream for the reduce phase.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(radius);
        out.writeDouble(ratio);
        writeSize(requiredSize, out);
        out.writeCollection((Collection<? extends Writeable>) buckets);
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

    protected Reader<Bucket> getBucketReader() {
        return Bucket::new;
    }

    @Override
    public String getWriteableName() {
        return GeoPointClusteringAggregationBuilder.NAME;
    }

    @Override
    public InternalGeoPointClustering create(List<Bucket> buckets) {
        return new InternalGeoPointClustering(this.name, this.radius, this.ratio, this.requiredSize, buckets, this.metadata);
    }

    public Bucket createBucket(long hashAsLong, GeoPoint centroid, long docCount, InternalAggregations aggregations) {
        return new Bucket(hashAsLong, centroid, docCount, aggregations);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.hashAsLong, prototype.centroid, prototype.docCount, aggregations);
    }

    @Override
    public List<InternalGeoPointClustering.Bucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    /**
     * Merges candidate clusters by checking their proximity and merging them if they are within the defined radius.
     * This method performs two passes:
     * 1. Identifies potential neighbors and merges them if they are close enough.
     * 2. Revisits borderline cases to ensure all close neighbors are considered.
     *
     * @param candidateClusters Array of candidate clusters to merge.
     * @param context           The aggregation reduce context for reducing internal aggregations.
     * @return A list of merged buckets representing the final clusters.
     */
    private List<Bucket> mergeBuckets(Bucket[] candidateClusters, AggregationReduceContext context) {
        List<Bucket> finalClusters = new ArrayList<>();
        for (Bucket bucket : candidateClusters) {
            if (bucket.visited) {
                continue; // Already merged into another cluster
            }

            bucket.visited = true;
            List<Bucket> revisit = new ArrayList<>();

            // First pass: identify and merge close neighbors, collect borderline cases
            for (Bucket potentialNeighbor : candidateClusters) {
                computeDistance(bucket, potentialNeighbor, revisit, context);
            }

            // Second pass: revisit borderline neighbors if ratio condition is met
            for (Bucket potentialNeighbor : revisit) {
                computeDistance(bucket, potentialNeighbor, null, context);
            }

            finalClusters.add(bucket);
        }
        return finalClusters;
    }

    /**
     * Returns a reducer that merges buckets from multiple shards into a single coherent set of top buckets.
     * This reducer is used during the reduction phase of the multi-bucket aggregation GeoPointClustering.
     */
    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext context, int size) {
        return new AggregatorReducer() {

            final LongObjectPagedHashMap<BucketReducer> bucketsReducer = new LongObjectPagedHashMap<>(size, context.bigArrays());

            /**
             * Accepts a partial aggregation result from a shard and accumulates its buckets into the corresponding reducers.
             * bucketsReducer is instantiated once one the coordinator node.
             * Then, once per shard the accept() method::
             * - for each cluster, check if a reducer already exists for that cluster's hash
             * - if not, create a new reducer and add the bucket to it
             * - if a reducer exists, simply add the bucket to that reducer
             *
             * @param aggregation The partial aggregation from a shard.
             */
            @Override
            public void accept(InternalAggregation aggregation) {
                final InternalGeoPointClustering grid = (InternalGeoPointClustering) aggregation;
                for (Bucket bucket : grid.getBuckets()) {
                    BucketReducer reducer = bucketsReducer.get(bucket.hashAsLong);
                    if (reducer == null) {
                        reducer = new BucketReducer(bucket, context, size);
                        bucketsReducer.put(bucket.hashAsLong, reducer);
                    }
                    reducer.accept(bucket);
                }
            }

            /**
             * Finalizes the reduction by merging the accumulated bucket data into a sorted list of top buckets.
             *
             * @return A fully reduced InternalAggregation with the top N buckets.
             */
            @Override
            public InternalAggregation get() {
                if (buckets == null) {
                    // If no buckets were collected, return an empty aggregation
                    return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, Collections.emptyList(), getMetadata());
                }

                final int size = Math.toIntExact(
                    context.isFinalReduce() == false ? bucketsReducer.size() : Math.min(requiredSize, bucketsReducer.size())
                );
                try (
                    BucketPriorityQueue<Bucket, Bucket> ordered = new BucketPriorityQueue<>(size, context.bigArrays(), Function.identity())
                ) {
                    // Populate the priority queue with the reduced clusters
                    bucketsReducer.forEach(entry -> {
                        Bucket cluster = createBucket(
                            entry.key,
                            entry.value.getWeightedCentroid(),
                            entry.value.getDocCount(),
                            entry.value.getAggregations()
                        );
                        ordered.insertWithOverflow(cluster);
                    });
                    Bucket[] clusters = new Bucket[(int) ordered.size()];
                    for (int i = (int) ordered.size() - 1; i >= 0; i--) {
                        clusters[i] = ordered.pop();
                    }
                    List<Bucket> finalClusters = mergeBuckets(clusters, context);
                    context.consumeBucketsAndMaybeBreak(finalClusters.size());
                    return create(finalClusters);
                }
            }

            /**
             * Releases resources used by the reducer once reduction is complete.
             */
            @Override
            public void close() {
                bucketsReducer.forEach(r -> Releasables.close(r.value));
                Releasables.close(bucketsReducer);
            }
        };
    }

    /**
     * Applies final adjustments to the aggregation result based on the sampling context,
     * scaling up the document counts and finalizing nested aggregations accordingly.
     *
     * @param samplingContext Context used to scale sample results back to estimated totals.
     * @return A new InternalAggregation with scaled doc counts and finalized sub-aggregations.
     */
    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return create(
            buckets.stream()
                .<Bucket>map(
                    b -> this.createBucket(
                        b.hashAsLong,
                        b.centroid,
                        samplingContext.scaleUp(b.docCount),
                        InternalAggregations.finalizeSampling(b.aggregations, samplingContext)
                    )
                )
                .toList()
        );
    }

    /**
     * Computes the distance between a bucket and a potential neighboring bucket,
     * and merges them if they are within a defined clustering radius.
     * If the neighbor is not merged but falls within a proximity threshold,
     * it is added to a revisit list for later consideration.
     *
     * @param bucket           The current cluster bucket being evaluated.
     * @param potentialNeighbor A nearby bucket that may be merged into the current one.
     * @param revisit          A list of buckets to potentially revisit for merging.
     * @param reduceContext    Context used to reduce internal aggregations during merging.
     */
    private void computeDistance(Bucket bucket, Bucket potentialNeighbor, List<Bucket> revisit, AggregationReduceContext reduceContext) {
        // Skip if this neighbor has already been processed
        if (potentialNeighbor.visited) {
            return;
        }

        // Compute geographical (arc) distance between centroids
        double neighborDistance = GeoUtils.arcDistance(
            bucket.centroid.lat(),
            bucket.centroid.lon(),
            potentialNeighbor.centroid.lat(),
            potentialNeighbor.centroid.lon()
        );

        // Calculate average latitude to adjust radius based on Earth's curvature
        double avgLat = (bucket.centroid.lat() + potentialNeighbor.centroid.lat()) / 2;

        // Apply latitude correction to the fixed clustering radius
        double fixedRadius = radius * Math.cos(Math.toRadians(avgLat));

        // Check if neighbor falls within clustering radius
        if (neighborDistance <= fixedRadius) {
            // Mark neighbor as visited and merge it into the current bucket
            potentialNeighbor.visited = true;

            // Compute merged document count
            long mergedDocCount = bucket.docCount + potentialNeighbor.docCount;

            // Compute new weighted centroid
            double newCentroidLat = (bucket.centroid.getLat() * bucket.docCount + potentialNeighbor.centroid.getLat()
                * potentialNeighbor.docCount) / mergedDocCount;
            double newCentroidLon = (bucket.centroid.getLon() * bucket.docCount + potentialNeighbor.centroid.getLon()
                * potentialNeighbor.docCount) / mergedDocCount;
            bucket.centroid = new GeoPoint(newCentroidLat, newCentroidLon);

            // Update document count and reduce sub-aggregations
            bucket.docCount = mergedDocCount;
            List<InternalAggregations> aggregationsList = new ArrayList<>();
            aggregationsList.add(bucket.aggregations);
            aggregationsList.add(potentialNeighbor.aggregations);
            bucket.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);

            // Track the geohash of the merged neighbor
            bucket.geohashesList.add(potentialNeighbor.hashAsLong);
        }
        // If not merged, check if it should be revisited later
        else if (revisit != null && ratio > 0 && neighborDistance / fixedRadius < ratio) {
            revisit.add(potentialNeighbor);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.bucketToXContent(builder, params);
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

    static class BucketPriorityQueue<A, B extends Bucket> extends ObjectArrayPriorityQueue<A> {

        private final Function<A, B> bucketSupplier;

        BucketPriorityQueue(int size, BigArrays bigArrays, Function<A, B> bucketSupplier) {
            super(size, bigArrays);
            this.bucketSupplier = bucketSupplier;
        }

        @Override
        protected boolean lessThan(A o1, A o2) {
            final B b1 = bucketSupplier.apply(o1);
            final B b2 = bucketSupplier.apply(o2);
            int cmp = Long.compare(b2.hashAsLong(), b1.hashAsLong());
            if (cmp == 0) {
                cmp = b2.compareTo(b1);
                if (cmp == 0) {
                    cmp = System.identityHashCode(o2) - System.identityHashCode(o1);
                }
            }
            return cmp > 0;
        }
    }
}
