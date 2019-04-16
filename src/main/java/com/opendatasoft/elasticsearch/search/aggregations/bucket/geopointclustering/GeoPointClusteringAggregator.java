package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

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
    private final LongHash bucketOrds;
    private ObjectArray<GeoPoint> centroids;
    private final ValuesSource.GeoPoint valuesSource;

    GeoPointClusteringAggregator(
            String name, AggregatorFactories factories, ValuesSource.GeoPoint valuesSource, int precision,
            double radius, double ratio, int requiredSize, int shardSize, SearchContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.radius = radius;
        this.ratio = ratio;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
        centroids = context.bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        GeoPoint value = values.nextValue();
                        final long val = GeoHashUtils.longEncode(value.getLon(), value.getLat(), precision);
                        if (previous != val || i == 0) {
                            long bucketOrdinal = bucketOrds.add(val);
                            double[] pt = new double[2];
                            double centroidLat = 0.0;
                            double centroidLon = 0.0;
                            if (bucketOrdinal < 0) { // already seen
                                bucketOrdinal = -1 - bucketOrdinal;
                                collectExistingBucket(sub, doc, bucketOrdinal);
                                GeoPoint centroid = centroids.get(bucketOrdinal);
                                centroidLat = centroid.lat();
                                centroidLon = centroid.lon();
//                                final long mortonCode = centroids.get(bucketOrdinal);
//                                pt[0] = InternalGeoPointClustering.decodeLongitude(mortonCode);
//                                pt[1] = InternalGeoPointClustering.decodeLatitude(mortonCode);
                            } else {
                                centroids = context.bigArrays().grow(centroids, bucketOrdinal + 1);
                                collectBucket(sub, doc, bucketOrdinal);
                            }

                            centroidLon = centroidLon + (value.getLon() - centroidLon) / bucketDocCount(bucketOrdinal);
                            centroidLat = centroidLat + (value.getLat() - centroidLat) / bucketDocCount(bucketOrdinal);

                            centroids.set(bucketOrdinal, new GeoPoint(centroidLat, centroidLon));
                            previous = val;
                        }
                    }
                }
            }
        };
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoPointClustering.Bucket {

        long bucketOrd;

        OrdinalBucket() {
            super(0, null, 0, null);
        }

    }

    @Override
    public InternalGeoPointClustering buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int size = (int) Math.min(bucketOrds.size(), shardSize);

        InternalGeoPointClustering.BucketPriorityQueue ordered =
                new InternalGeoPointClustering.BucketPriorityQueue(size);
        OrdinalBucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new OrdinalBucket();
            }

            spare.geohashAsLong = bucketOrds.get(i);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            spare = (OrdinalBucket) ordered.insertWithOverflow(spare);
        }

        final InternalGeoPointClustering.Bucket[] list = new InternalGeoPointClustering.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdinalBucket bucket = (OrdinalBucket) ordered.pop();
            bucket.centroid = centroids.get(bucket.bucketOrd);
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new InternalGeoPointClustering(
                name, radius, ratio, requiredSize, Arrays.asList(list), pipelineAggregators(), metaData());
    }

    @Override
    public InternalGeoPointClustering buildEmptyAggregation() {
        return new InternalGeoPointClustering(
                name, radius, ratio, requiredSize, Collections.emptyList(), pipelineAggregators(), metaData());
    }


    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}
