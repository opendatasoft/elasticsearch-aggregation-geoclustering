package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BucketAndOrd;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GeoPointClusteringAggregator extends BucketsAggregator {

    private final int requiredSize;
    private final int shardSize;
    private final int precision;
    private final double radius;
    private final double ratio;
    private final LongKeyedBucketOrds bucketOrds;
    private ObjectArray<GeoPoint> centroids;
    private final ValuesSource.GeoPoint valuesSource;

    /**
     * Constructs a GeoPointClusteringAggregator instance, once per shard.
     * This aggregator collects geo-point values, clusters them into geo buckets.
     *
     * @param name The name of the aggregation.
     * @param factories The factories for all the sub-aggregators under this aggregator.
     * @param valuesSource The source of geo-point values for the aggregation.
     * @param precision The precision level for geohash encoding.
     * @param radius The clustering radius in geographical units.
     * @param ratio The ratio used for clustering calculations.
     * @param requiredSize The number of top buckets to return.
     * @param shardSize The number of buckets to consider per shard.
     * @param aggregationContext The context for the aggregation execution.
     * @param parent The parent aggregator in the aggregation tree (may be null for top level aggregators).
     * @param cardinality Upper bound of the number of buckets that aggregation will collect.
     * @param metaData Metadata associated with this aggregator.
     * @throws IOException If an I/O error occurs during initialization.
     */
    public GeoPointClusteringAggregator(
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
        CardinalityUpperBound cardinality,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, aggregationContext, parent, cardinality, metaData);
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.radius = radius;
        this.ratio = ratio;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = LongKeyedBucketOrds.build(bigArrays(), cardinality);
        centroids = bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    /**
     * getLeafCollector() is called on each shard. It collects documents matching the query (if any) for a particular
     * Lucene segment.
     * For each document:
     *   - retrieves the geo-point values from the field data
     *   - calculates the geohash cell for each geo-point value
     *   - if this geohash cell is new, creates a bucket with the geohash cell hash as the key
     * In the end each buck is a cluster of geo-points based on a geohash cell.
     */
    @Override
    public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx, final LeafBucketCollector sub) {
        // access GeoPoint content from Lucene field_data
        final MultiGeoPointValues values = valuesSource.geoPointValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            /**
             * Collect the given {@code doc} in the bucket owned by {@code owningBucketOrdinal}.
             * We don't know how many buckets will fall into any particular owning bucket, that's why
             * we are using {@link LongKeyedBucketOrds} which amounts to a hash lookup.
             */
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                // always 0 because the plugin does not support sub-aggregations
                assert owningBucketOrdinal == 0;

                // iterate over documents; values contain current document field_data
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    long previous = Long.MAX_VALUE;

                    // loop over all Geopoints for the current document
                    for (int i = 0; i < valuesCount; ++i) {
                        GeoPoint value = values.nextValue();
                        // get the geohash cell hash for the current GeoPoint value
                        // this hash is used as the bucket key
                        final long geohash_bucket_key = Geohash.longEncode(value.getLon(), value.getLat(), precision);

                        // already seen, from the previous doc
                        if (previous != geohash_bucket_key || i == 0) {
                            long bucketOrdinal = bucketOrds.add(owningBucketOrdinal, geohash_bucket_key);
                            double centroidLat = 0.0;
                            double centroidLon = 0.0;
                            // already seen, from the HashTable
                            if (bucketOrdinal < 0) {
                                bucketOrdinal = -1 - bucketOrdinal;
                                collectExistingBucket(sub, doc, bucketOrdinal);
                                GeoPoint centroid = centroids.get(bucketOrdinal);
                                centroidLat = centroid.lat();
                                centroidLon = centroid.lon();
                            } else {
                                centroids = bigArrays().grow(centroids, bucketOrdinal + 1);
                                // collect the given doc in the given bucket (identified by the bucket ordinal)
                                collectBucket(sub, doc, bucketOrdinal);
                            }

                            centroidLon = centroidLon + (value.getLon() - centroidLon) / bucketDocCount(bucketOrdinal);
                            centroidLat = centroidLat + (value.getLat() - centroidLat) / bucketDocCount(bucketOrdinal);

                            centroids.set(bucketOrdinal, new GeoPoint(centroidLat, centroidLon));
                            // store the geohash key for the next iteration: if the key is the same, we will skip the
                            // bucket creation. Identical geohash keys are consecutive due to the way Lucene stores
                            // geopoints in the doc_values structure
                            previous = geohash_bucket_key;
                        }
                    }
                }
            }

        };
    }

    InternalGeoPointClustering.Bucket newEmptyBucket() {
        return new InternalGeoPointClustering.Bucket(0, null, 0, null);
    }

    /**
     * Builds the final {@link InternalAggregation} results for each owning bucket ordinal.
     * <p>
     * This method handles the main logic of collecting, prioritizing, and organizing the clustered geo buckets.
     * Since {@link GeoPointClusteringAggregator} does not support sub-aggregations, it always operates under
     * a single owning bucket ordinal.
     * </p>
     *
     * <p>The process follows several steps:
     * <ul>
     *   <li>Compute the number of sub-buckets per parent bucket (up to shardSize).</li>
     *   <li>Use a Lucene-style {@link org.apache.lucene.util.PriorityQueue} to retain only the top N buckets
     *       per owning bucket, based on geohash ordering.</li>
     *   <li>Track ordinals of selected buckets for possible sub-aggregations.</li>
     *   <li>Build the final {@link InternalGeoPointClustering} instances using the selected buckets.</li>
     * </ul>
     * </p>
     *
     * @param owningBucketOrds The owning bucket ordinals passed from the parent aggregator (only one in our case).
     * @return An array of {@link InternalAggregation} results, one for each owning bucket ordinal.
     * @throws IOException If an error occurs during aggregation data collection or memory access.
     */
    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        // GeoPointClusteringAggregator does not support sub-aggregations, so we always have a unique owning bucket
        // ordinal [0]

        // final buckets result array for the current owningBucketOrds
        try (ObjectArray<InternalGeoPointClustering.Bucket[]> topBucketsPerOrd = bigArrays().newObjectArray(owningBucketOrds.size())) {
            try (IntArray bucketsSizePerOrd = bigArrays().newIntArray(owningBucketOrds.size())) {

                // first pass: determine how many ords to collect and store expected bucket sizes per ord
                long ordsToCollect = 0;
                for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                    int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrds.get(ordIdx)), shardSize);
                    ordsToCollect += size;
                    bucketsSizePerOrd.set(ordIdx, size);
                }

                // allocate array to store ords for later sub-aggregation processing
                try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                    long ordsCollected = 0;

                    // loop over parent buckets
                    for (long ordIdx = 0; ordIdx < topBucketsPerOrd.size(); ordIdx++) {

                        // store top buckets in a Lucene-style PriorityQueue of size 'size'
                        try (
                            InternalGeoPointClustering.BucketPriorityQueue<
                                BucketAndOrd<InternalGeoPointClustering.Bucket>,
                                InternalGeoPointClustering.Bucket> ordered = new InternalGeoPointClustering.BucketPriorityQueue<>(
                                    bucketsSizePerOrd.get(ordIdx),
                                    bigArrays(),
                                    b -> b.bucket
                                )
                        ) {
                            BucketAndOrd<InternalGeoPointClustering.Bucket> spare = null;
                            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds.get(ordIdx));
                            while (ordsEnum.next()) {
                                if (spare == null) {
                                    // allocate memory for the spare bucket wrapper on first use
                                    checkRealMemoryCBForInternalBucket();
                                    spare = new BucketAndOrd<>(newEmptyBucket());
                                }

                                // populate the bucket fields from current ord values
                                spare.bucket.hashAsLong = ordsEnum.value();
                                spare.bucket.centroid = centroids.get(ordsEnum.ord());
                                spare.bucket.docCount = bucketDocCount(ordsEnum.ord());
                                spare.ord = ordsEnum.ord();

                                // insert the bucket into the priority queue.
                                // if the queue is full, it will evict the least prioritized bucket (the one with the
                                // lowest geohash value)
                                spare = ordered.insertWithOverflow(spare);
                            }

                            // extract top buckets from the priority queue into a sorted array of buckets
                            final int orderedSize = (int) ordered.size();
                            final InternalGeoPointClustering.Bucket[] buckets = new InternalGeoPointClustering.Bucket[orderedSize];
                            for (int i = orderedSize - 1; i >= 0; --i) {
                                BucketAndOrd<InternalGeoPointClustering.Bucket> bucketBucketAndOrd = ordered.pop();
                                buckets[i] = bucketBucketAndOrd.bucket;
                                ordsArray.set(ordsCollected + i, bucketBucketAndOrd.ord);
                            }

                            // store this bucket array per owning bucket ord
                            topBucketsPerOrd.set(ordIdx, buckets);
                            ordsCollected += orderedSize;
                        }
                    }

                    assert ordsCollected == ordsArray.size();

                    // build sub-aggregations for all collected buckets using ords array
                    buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, (b, aggs) -> b.aggregations = aggs);
                }
            }

            // final aggregation object creation for each owning bucket
            return buildAggregations(
                Math.toIntExact(owningBucketOrds.size()),
                ordIdx -> buildAggregation(name, radius, ratio, requiredSize, Arrays.asList(topBucketsPerOrd.get(ordIdx)), metadata())
            );
        }
    }

    InternalGeoPointClustering buildAggregation(
        String name,
        double radius,
        double ratio,
        int requiredSize,
        List<InternalGeoPointClustering.Bucket> buckets,
        Map<String, Object> metaData
    ) {
        return new InternalGeoPointClustering(name, radius, ratio, requiredSize, buckets, metaData);
    }

    @Override
    public InternalGeoPointClustering buildEmptyAggregation() {
        return new InternalGeoPointClustering(name, radius, ratio, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, centroids);
    }

}
