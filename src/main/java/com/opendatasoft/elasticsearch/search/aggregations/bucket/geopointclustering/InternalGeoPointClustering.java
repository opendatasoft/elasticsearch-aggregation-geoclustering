package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

public class InternalGeoPointClustering extends InternalMultiBucketAggregation<InternalGeoPointClustering, InternalGeoPointClusteringBucket>
    implements
        GeoPointClustering {

    private final double radius;
    private final double ratio;
    private final int requiredSize;
    private final List<InternalGeoPointClusteringBucket> buckets;

    InternalGeoPointClustering(
        String name,
        double radius,
        double ratio,
        int requiredSize,
        List<InternalGeoPointClusteringBucket> buckets,
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
        buckets = (List<InternalGeoPointClusteringBucket>) in.readCollectionAsList(getBucketReader());
    }

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(radius);
        out.writeDouble(ratio);
        writeSize(requiredSize, out);
        out.writeCollection(buckets);
    }

    @Override
    public String getWriteableName() {
        return GeoPointClusteringAggregationBuilder.NAME;
    }

    @Override
    public InternalGeoPointClustering create(List<InternalGeoPointClusteringBucket> buckets) {
        return new InternalGeoPointClustering(this.name, this.radius, this.ratio, this.requiredSize, buckets, this.metadata);
    }

    @Override
    public InternalGeoPointClusteringBucket createBucket(InternalAggregations aggregations, InternalGeoPointClusteringBucket prototype) {
        return new InternalGeoPointClusteringBucket(prototype.geohashAsLong, prototype.centroid, prototype.docCount, aggregations);
    }

    @Override
    public List<InternalGeoPointClusteringBucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext context, int size) {
        return new AggregatorReducer() {
            // LongObjectPagedHashMap<List<InternalGeoPointClusteringBucket>> buckets = new LongObjectPagedHashMap<>(size,
            // context.bigArrays());
            LongObjectPagedHashMap<List<InternalGeoPointClusteringBucket>> buckets = null;
            List<InternalGeoPointClusteringBucket> final_clusters = new ArrayList<>();

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalGeoPointClustering shard_clusters = (InternalGeoPointClustering) aggregation;
                if (buckets == null) {
                    buckets = new LongObjectPagedHashMap<>(shard_clusters.buckets.size(), context.bigArrays());
                }
                for (InternalGeoPointClusteringBucket bucket : shard_clusters.getBuckets()) {
                    List<InternalGeoPointClusteringBucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                    if (existingBuckets == null) {
                        existingBuckets = new ArrayList<>(size);
                        buckets.put(bucket.geohashAsLong, existingBuckets);
                    }
                    existingBuckets.add(bucket);
                }

                // cast the buckets HashMap to a Lucene PriorityQueue
                // the priority queue is used to retain the top N bucket
                final int size = Math.toIntExact(context.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size()));
                BucketPriorityQueue ordered = new BucketPriorityQueue(size);
                for (LongObjectPagedHashMap.Cursor<List<InternalGeoPointClusteringBucket>> cursor : buckets) {
                    List<InternalGeoPointClusteringBucket> sameCellBuckets = cursor.value;
                    ordered.insertWithOverflow(reduceBucket(sameCellBuckets, context));
                }
                buckets.close();

                // cast Lucene PriorityQueue to GeoPointClustering buckets
                InternalGeoPointClusteringBucket[] candidate_clusters = new InternalGeoPointClusteringBucket[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    candidate_clusters[i] = ordered.pop();
                }

                // merge buckets if needed, according to radius and ratio plugin parameters
                for (InternalGeoPointClusteringBucket bucket : candidate_clusters) {
                    if (bucket.visited) {
                        continue;
                    }

                    bucket.visited = true;
                    List<InternalGeoPointClusteringBucket> revisit = new ArrayList<>();
                    for (InternalGeoPointClusteringBucket potentialNeighbor : candidate_clusters) {
                        computeDistance(bucket, potentialNeighbor, revisit, context);
                    }
                    for (InternalGeoPointClusteringBucket potentialNeighbor : revisit) {
                        computeDistance(bucket, potentialNeighbor, null, context);
                    }
                    final_clusters.add(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, final_clusters, getMetadata());
            }

        };

        // // collect and deduplicate buckets from each shard
        // // cast an array of InternalGeoPointClustering agg to a HashMap of buckets
        // // buckets are deduplicated
        // for (InternalAggregation shard_aggregation : aggregations) {
        // InternalGeoPointClustering shard_clusters = (InternalGeoPointClustering) shard_aggregation;
        // if (buckets == null) {
        // buckets = new LongObjectPagedHashMap<>(shard_clusters.buckets.size(), reduceContext.bigArrays());
        // }
        // for (InternalGeoPointClusteringBucket bucket : shard_clusters.buckets) {
        // List<InternalGeoPointClusteringBucket> existingBuckets = buckets.get(bucket.geohashAsLong);
        // if (existingBuckets == null) {
        // existingBuckets = new ArrayList<>(aggregations.size());
        // buckets.put(bucket.geohashAsLong, existingBuckets);
        // }
        // existingBuckets.add(bucket);
        // }
        // }

        // // cast the buckets HashMap to a Lucene PriorityQueue
        // // the priority queue is used to retain the top N bucket
        // final int size = Math.toIntExact(reduceContext.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size()));
        // BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        // for (LongObjectPagedHashMap.Cursor<List<InternalGeoPointClusteringBucket>> cursor : buckets) {
        // List<InternalGeoPointClusteringBucket> sameCellBuckets = cursor.value;
        // ordered.insertWithOverflow(reduceBucket(sameCellBuckets, reduceContext));
        // }
        // buckets.close();

        // // cast Lucene PriorityQueue to GeoPointClustering buckets
        // InternalGeoPointClusteringBucket[] candidate_clusters = new InternalGeoPointClusteringBucket[ordered.size()];
        // for (int i = ordered.size() - 1; i >= 0; i--) {
        // candidate_clusters[i] = ordered.pop();
        // }

        // // merge buckets if needed, according to radius and ratio plugin parameters
        // List<InternalGeoPointClusteringBucket> final_clusters = new ArrayList<>();
        // for (InternalGeoPointClusteringBucket bucket : candidate_clusters) {
        // if (bucket.visited) {
        // continue;
        // }

        // bucket.visited = true;
        // List<InternalGeoPointClusteringBucket> revisit = new ArrayList<>();
        // for (InternalGeoPointClusteringBucket potentialNeighbor : candidate_clusters) {
        // computeDistance(bucket, potentialNeighbor, revisit, reduceContext);
        // }
        // for (InternalGeoPointClusteringBucket potentialNeighbor : revisit) {
        // computeDistance(bucket, potentialNeighbor, null, reduceContext);
        // }
        // final_clusters.add(bucket);
        // }

        // return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, final_clusters, getMetadata());
    }

    protected Reader<InternalGeoPointClusteringBucket> getBucketReader() {
        return InternalGeoPointClusteringBucket::new;
    }

    /**
     * Reduces the given aggregations to a single one and returns it.
     */

    /**
     * Reduces the given aggregations to a single one and returns it.
     */
    // @Override
    // public InternalGeoPointClustering reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    // LongObjectPagedHashMap<List<InternalGeoPointClusteringBucket>> buckets = null;

    // // collect and deduplicate buckets from each shard
    // // cast an array of InternalGeoPointClustering agg to a HashMap of buckets
    // // buckets are deduplicated
    // for (InternalAggregation shard_aggregation : aggregations) {
    // InternalGeoPointClustering shard_clusters = (InternalGeoPointClustering) shard_aggregation;
    // if (buckets == null) {
    // buckets = new LongObjectPagedHashMap<>(shard_clusters.buckets.size(), reduceContext.bigArrays());
    // }
    // for (InternalGeoPointClusteringBucket bucket : shard_clusters.buckets) {
    // List<InternalGeoPointClusteringBucket> existingBuckets = buckets.get(bucket.geohashAsLong);
    // if (existingBuckets == null) {
    // existingBuckets = new ArrayList<>(aggregations.size());
    // buckets.put(bucket.geohashAsLong, existingBuckets);
    // }
    // existingBuckets.add(bucket);
    // }
    // }

    // // cast the buckets HashMap to a Lucene PriorityQueue
    // // the priority queue is used to retain the top N bucket
    // final int size = Math.toIntExact(reduceContext.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size()));
    // BucketPriorityQueue ordered = new BucketPriorityQueue(size);
    // for (LongObjectPagedHashMap.Cursor<List<InternalGeoPointClusteringBucket>> cursor : buckets) {
    // List<InternalGeoPointClusteringBucket> sameCellBuckets = cursor.value;
    // ordered.insertWithOverflow(reduceBucket(sameCellBuckets, reduceContext));
    // }
    // buckets.close();

    // // cast Lucene PriorityQueue to GeoPointClustering buckets
    // InternalGeoPointClusteringBucket[] candidate_clusters = new InternalGeoPointClusteringBucket[ordered.size()];
    // for (int i = ordered.size() - 1; i >= 0; i--) {
    // candidate_clusters[i] = ordered.pop();
    // }

    // // merge buckets if needed, according to radius and ratio plugin parameters
    // List<InternalGeoPointClusteringBucket> final_clusters = new ArrayList<>();
    // for (InternalGeoPointClusteringBucket bucket : candidate_clusters) {
    // if (bucket.visited) {
    // continue;
    // }

    // bucket.visited = true;
    // List<InternalGeoPointClusteringBucket> revisit = new ArrayList<>();
    // for (InternalGeoPointClusteringBucket potentialNeighbor : candidate_clusters) {
    // computeDistance(bucket, potentialNeighbor, revisit, reduceContext);
    // }
    // for (InternalGeoPointClusteringBucket potentialNeighbor : revisit) {
    // computeDistance(bucket, potentialNeighbor, null, reduceContext);
    // }
    // final_clusters.add(bucket);
    // }

    // return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, final_clusters, getMetadata());
    // }

    // @Override
    public InternalGeoPointClusteringBucket reduceBucket(List<InternalGeoPointClusteringBucket> buckets, AggregationReduceContext context) {
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        long docCount = 0;
        double centroidLat = 0;
        double centroidLon = 0;
        long geohashAsLong = 0;
        for (InternalGeoPointClusteringBucket bucket : buckets) {
            docCount += bucket.docCount;
            centroidLat += bucket.centroid.getLat() * bucket.docCount;
            centroidLon += bucket.centroid.getLon() * bucket.docCount;
            aggregationsList.add(bucket.subAggregations);
            geohashAsLong = bucket.geohashAsLong;
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return new InternalGeoPointClusteringBucket(
            geohashAsLong,
            new GeoPoint(centroidLat / docCount, centroidLon / docCount),
            docCount,
            aggs
        );
    }

    /**
     * Merge buckets (clusters) if their distance is below some threshold, defined from radius plugin parameter.
     * It allows to not stuck clusters to the geohash grid.
     * E.g. at zoom level 1, points in France metropolitan area will output 4 clusters instead of 1 without this merge process,
     * because geohash cells g, u, e, s would be used.
     */
    private void computeDistance(
        InternalGeoPointClusteringBucket bucket,
        InternalGeoPointClusteringBucket potentialNeighbor,
        List<InternalGeoPointClusteringBucket> revisit,
        AggregationReduceContext context
    ) {
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
            bucket.subAggregations = InternalAggregations.reduce(aggregationsList, context);
            bucket.geohashesList.add(potentialNeighbor.geohashAsLong);
        } else if (revisit != null && ratio > 0 && neighborDistance / fixedRadius < ratio) {
            revisit.add(potentialNeighbor);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalGeoPointClusteringBucket bucket : buckets) {
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

    static class BucketPriorityQueue extends PriorityQueue<InternalGeoPointClusteringBucket> {

        BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(InternalGeoPointClusteringBucket o1, InternalGeoPointClusteringBucket o2) {
            int res = o1.compareTo(o2);
            return res < 0;
        }
    }
}
