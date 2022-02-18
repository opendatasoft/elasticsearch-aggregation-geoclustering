package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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

    public GeoPointClusteringAggregator(
            String name,
            AggregatorFactories factories,
            ValuesSource.GeoPoint valuesSource,
            int precision,
            double radius,
            double ratio,
            int requiredSize,
            int shardSize,
            SearchContext aggregationContext,
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
        bucketOrds = LongKeyedBucketOrds.build(aggregationContext.bigArrays(), cardinality);
        centroids = context.bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    /**
     * getLeafCollector() is called for each shard.
     * collect() is called for each document: it accumulates doc values
     */
    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);  // GeoPoint field_data
        return new LeafBucketCollectorBase(sub, values) {
            /**
             * Collect the given {@code doc} in the bucket owned by {@code owningBucketOrdinal}.
             * We don't know how many buckets will fall into any particular owning bucket, that's why
             * we are using {@link LongKeyedBucketOrds} which amounts to a hash lookup.
             */
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                assert owningBucketOrdinal == 0;
                if (values.advanceExact(doc)) {  // iterate over documents; values contain current document field_data
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {  // iterate over field_data for the current doc
                        GeoPoint value = values.nextValue();
                        final long geohash_bucket_key = Geohash.longEncode(value.getLon(), value.getLat(), precision);
                        if (previous != geohash_bucket_key || i == 0) {  // already seen, from the previous doc
                            long bucketOrdinal = bucketOrds.add(owningBucketOrdinal, geohash_bucket_key);
                            double centroidLat = 0.0;
                            double centroidLon = 0.0;
                            if (bucketOrdinal < 0) {  // already seen, from the HashTable
                                bucketOrdinal = -1 - bucketOrdinal;
                                collectExistingBucket(sub, doc, bucketOrdinal);
                                GeoPoint centroid = centroids.get(bucketOrdinal);
                                centroidLat = centroid.lat();
                                centroidLon = centroid.lon();
                            } else {
                                centroids = context.bigArrays().grow(centroids, bucketOrdinal + 1);
                                // collect the given doc in the given bucket (identified by the bucket ordinal)
                                collectBucket(sub, doc, bucketOrdinal);
                            }

                            centroidLon = centroidLon + (value.getLon() - centroidLon) / bucketDocCount(bucketOrdinal);
                            centroidLat = centroidLat + (value.getLat() - centroidLat) / bucketDocCount(bucketOrdinal);

                            centroids.set(bucketOrdinal, new GeoPoint(centroidLat, centroidLon));
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
     * buildAggregations is called for each shard after the collect phase, and will create an
     * InternalGeoPointClustering aggregation then sent to the master node for the reduce phase.
     * The resulting buckets are ordered.
     */
    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrdinals) throws IOException {
        InternalGeoPointClustering.Bucket[][] topBucketsPerOrd = new InternalGeoPointClustering.Bucket[owningBucketOrdinals.length][];
        InternalGeoPointClustering[] results = new InternalGeoPointClustering[owningBucketOrdinals.length];

        for (int ordIdx = 0; ordIdx < owningBucketOrdinals.length; ordIdx++) {
            final int size = (int) Math.min(bucketOrds.size(), shardSize);

            // store buckets in a Lucene PriorityQueue
            InternalGeoPointClustering.BucketPriorityQueue ordered =  new InternalGeoPointClustering.BucketPriorityQueue(size);
            InternalGeoPointClustering.Bucket spare = null;
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrdinals[ordIdx]);
            while (ordsEnum.next()) {
                if (spare == null) {
                    spare = newEmptyBucket();
                }

                spare.geohashAsLong = ordsEnum.value();
                spare.centroid = centroids.get(ordsEnum.ord());
                spare.docCount = bucketDocCount(ordsEnum.ord());
                spare.bucketOrd = ordsEnum.ord();
                spare = ordered.insertWithOverflow(spare);
            }

            // feed the final aggregation from the PriorityQueue
            topBucketsPerOrd[ordIdx] = new InternalGeoPointClustering.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBucketsPerOrd[ordIdx][i] = ordered.pop();
            }
            results[ordIdx] = new InternalGeoPointClustering(
                name, radius, ratio, requiredSize, Arrays.asList(topBucketsPerOrd[ordIdx]), metadata());
        }

        buildSubAggsForAllBuckets(
                topBucketsPerOrd,
                b -> b.bucketOrd,
                (b, aggregations) -> b.subAggregations = aggregations
        );
        return results;
    }

    @Override
    public InternalGeoPointClustering buildEmptyAggregation() {
        return new InternalGeoPointClustering(
                name, radius, ratio, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, centroids);
    }

}
