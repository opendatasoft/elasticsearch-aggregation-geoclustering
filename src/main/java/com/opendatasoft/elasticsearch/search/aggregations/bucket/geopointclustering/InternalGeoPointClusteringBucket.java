package com.opendatasoft.elasticsearch.search.aggregations.bucket.geopointclustering;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class InternalGeoPointClusteringBucket extends InternalMultiBucketAggregation.InternalBucketWritable
    implements
        GeoPointClustering.Bucket,
        Comparable<InternalGeoPointClusteringBucket> {

    protected long geohashAsLong;
    protected GeoPoint centroid;
    protected long docCount;
    long bucketOrd;
    protected InternalAggregations subAggregations;
    protected boolean visited = false;
    protected Set<Long> geohashesList;

    public InternalGeoPointClusteringBucket(long geohashAsLong, GeoPoint centroid, long docCount, InternalAggregations subAggregations) {
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
    public InternalGeoPointClusteringBucket(StreamInput in) throws IOException {
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
    public InternalAggregations getAggregations() {
        return subAggregations;
    }

    @Override
    public int compareTo(InternalGeoPointClusteringBucket other) {
        return Long.compare(this.geohashAsLong, other.geohashAsLong);
    }

    final void bucketToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        // TODO: builder.field??
        subAggregations.toXContentInternal(builder, params);
        builder.endObject();
    }

    // @Override
    // move into InternalGeoPointClustering??
    // public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    // builder.startObject();
    // builder.field("geohash_grids", geohashesList.stream().map(Geohash::stringEncode).collect(Collectors.toList()));
    // builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
    // builder.field("centroid", centroid);
    // subAggregations.toXContentInternal(builder, params);
    // builder.endObject();
    // return builder;
    // }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalGeoPointClusteringBucket bucket = (InternalGeoPointClusteringBucket) o;
        return geohashAsLong == bucket.geohashAsLong
            && docCount == bucket.docCount
            && Objects.equals(subAggregations, bucket.subAggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(geohashAsLong, docCount, subAggregations);
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

}
