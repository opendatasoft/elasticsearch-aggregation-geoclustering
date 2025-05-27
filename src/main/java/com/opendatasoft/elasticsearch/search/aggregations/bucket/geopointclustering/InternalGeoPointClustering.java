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
import java.util.Collections;
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
            // État partagé pour la collecte des buckets
            // final LongObjectPagedHashMap<List<InternalGeoPointClusteringBucket>> bucketsReducer = new LongObjectPagedHashMap<>(size, context.bigArrays());
            private LongObjectPagedHashMap<List<InternalGeoPointClusteringBucket>> buckets;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalGeoPointClustering shardClusters = (InternalGeoPointClustering) aggregation;

                // Initialisation paresseuse de la HashMap
                if (buckets == null) {
                    buckets = new LongObjectPagedHashMap<>(Math.max(shardClusters.buckets.size(), 16), context.bigArrays());
                }

                // Collecte et déduplication des buckets de ce shard
                for (InternalGeoPointClusteringBucket bucket : shardClusters.buckets) {
                    List<InternalGeoPointClusteringBucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                    if (existingBuckets == null) {
                        existingBuckets = new ArrayList<>();
                        buckets.put(bucket.geohashAsLong, existingBuckets);
                    }
                    existingBuckets.add(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                if (buckets == null) {
                    // Cas où aucune agrégation n'a été acceptée
                    return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, Collections.emptyList(), getMetadata());
                }

                try {
                    // Réduction des buckets avec la PriorityQueue
                    final int queueSize = Math.toIntExact(
                        context.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size())
                    );
                    BucketPriorityQueue ordered = new BucketPriorityQueue(queueSize);

                    for (LongObjectPagedHashMap.Cursor<List<InternalGeoPointClusteringBucket>> cursor : buckets) {
                        List<InternalGeoPointClusteringBucket> sameCellBuckets = cursor.value;
                        ordered.insertWithOverflow(reduceBucket(sameCellBuckets, context));
                    }

                    // Conversion de la PriorityQueue en array
                    InternalGeoPointClusteringBucket[] candidateClusters = new InternalGeoPointClusteringBucket[ordered.size()];
                    for (int i = ordered.size() - 1; i >= 0; i--) {
                        candidateClusters[i] = ordered.pop();
                    }

                    // Fusion des buckets selon les paramètres radius et ratio
                    List<InternalGeoPointClusteringBucket> finalClusters = mergeBuckets(candidateClusters, context);

                    return new InternalGeoPointClustering(getName(), radius, ratio, requiredSize, finalClusters, getMetadata());

                } finally {
                    // Nettoyage des ressources
                    if (buckets != null) {
                        buckets.close();
                    }
                }
            }
        };
    }

    /**
     * Extrait la logique de fusion des buckets pour améliorer la lisibilité
     */
    private List<InternalGeoPointClusteringBucket> mergeBuckets(
        InternalGeoPointClusteringBucket[] candidateClusters,
        AggregationReduceContext context
    ) {
        List<InternalGeoPointClusteringBucket> finalClusters = new ArrayList<>();

        for (InternalGeoPointClusteringBucket bucket : candidateClusters) {
            if (bucket.visited) {
                continue;
            }

            bucket.visited = true;
            List<InternalGeoPointClusteringBucket> revisit = new ArrayList<>();

            // Premier passage pour identifier les voisins potentiels
            for (InternalGeoPointClusteringBucket potentialNeighbor : candidateClusters) {
                computeDistance(bucket, potentialNeighbor, revisit, context);
            }

            // Second passage pour traiter les voisins identifiés
            for (InternalGeoPointClusteringBucket potentialNeighbor : revisit) {
                computeDistance(bucket, potentialNeighbor, null, context);
            }

            finalClusters.add(bucket);
        }
        return finalClusters;
    }

    /**
     * Méthode de réduction d'un bucket - adaptée pour le nouveau contexte
     */
    private InternalGeoPointClusteringBucket reduceBucket(
        List<InternalGeoPointClusteringBucket> buckets,
        AggregationReduceContext context
    ) {
        if (buckets.size() == 1) {
            return buckets.get(0);
        }

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

    protected Reader<InternalGeoPointClusteringBucket> getBucketReader() {
        return InternalGeoPointClusteringBucket::new;
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
