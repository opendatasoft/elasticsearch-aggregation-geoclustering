Elasticsearch Geo Point clustering aggregation plugin
=====================================================

This aggregations computes a geohash precision from a `zoom` and a `distance` (in pixel).
It groups points (from `field` parameter) into buckets that represent geohash cells and computes each bucket's center.
Then it merges these cells if the distance between two clusters' centers is lower than the `distance` parameter.

```json
{
  "aggregations": {
    "<aggregation_name>": {
      "geohash_clustering": {
        "field": "<field_name>",
        "zoom": "<zoom>"
      }
    }
  }
}
```
Input parameters :
 - `field`: must be of type geo_point.
 - `zoom`: mandatory integer parameter between 0 and 20. It represents the zoom level used in the request to aggregate geo points.
 - `radius`: radius in pixel. It is used to compute a geohash precision then to merge cluters based on this distance. Default to `50`. 
 - `ratio`: ratio used to make a second merging pass. If the value is `0`, no second pass is made. Default to `2`. 
 - `extent`: Extent of the tiles. Default to `256`

For example :

```json
{
    "aggregations" : {
        "my_cluster_aggregation" : {
            "geohash_clustering": {
                "field": "geo_point",
                "zoom": 1,
                "distance": 50
            }
        }
    }
}
```

```json
{
    "aggregations": {
         "my_cluster_aggregation": {
            "buckets": [
               {
                  "geohash_grids": [
                     "u0"
                  ],
                  "doc_count": 90293,
                  "centroid": {
                     "lat": 48.8468417795375,
                     "lon": 2.331401154398918
                  }
               }
            ]
         }
    }

}
```

Installation
------------

Plugin versions are available for (at least) all minor versions of Elasticsearch since 6.0.

The first 3 digits of plugin version is Elasticsearch versioning. The last digit is used for plugin versioning under an elasticsearch version.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)
`./bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v6.4.2.0/geopoint-clustering-aggregation-6.4.2.0.zip`

| elasticsearch version | plugin version | plugin url |
| --------------------- | -------------- | ---------- |
| 6.0.1 | 6.0.1.0 | https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v6.0.1.0/geopoint-clustering-aggregation-6.0.1.0.zip|
| 6.1.4 | 6.1.4.0 | https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v6.1.4.0/geopoint-clustering-aggregation-6.1.4.0.zip|
| 6.2.4 | 6.2.4.0 | https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v6.2.4.0/geopoint-clustering-aggregation-6.2.4.0.zip|
| 6.3.2 | 6.3.2.0 | https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v6.3.2.0/geopoint-clustering-aggregation-6.3.2.0.zip|
| 6.4.2 | 6.4.2.0 | https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v6.4.2.0/geopoint-clustering-aggregation-6.4.2.0.zip|
