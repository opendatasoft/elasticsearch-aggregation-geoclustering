## Elasticsearch Geo Point clustering aggregation plugin

This plugin extends Elasticsearch with a `geo_point_clustering` aggregation, allowing to fetch [geo_point](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/geo-point.html) documents as clusters of points.
It is very similar to what is done with the official [geohash_grid aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-aggregations-bucket-geohashgrid-aggregation.html) except that final clusters are not bound to the geohash grid.

For example, at zoom level 1 with points across France, `geohash_grid` agg will output 3 clusters stuck to geohash cells u, e, s, while `geo_point_clustering` will merge these clusters into one.
This is done during the reduce phase.

Contrary to `geohash_grid` aggregation, buckets keys are a tuple(centroid, geohash cells) instead of geohash cells only, because one cluster can be linked to several geohash cells, due to the cluster merge process during the reduce phase.
Centroids are built during the shard collect phase.

Please note that [geo_shape data type](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/geo-shape.html) is not supported.


## Usage
### Install

Install plugin with:
`./bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-aggregation-geoclustering/releases/download/v7.17.28.0/geopoint-clustering-aggregation-7.17.28.0.zip`


### Quickstart
#### Intro
```json
{
  "aggregations": {
    "<aggregation_name>": {
      "geo_point_clustering": {
        "field": "<field_name>",
        "zoom": "<zoom>"
      }
    }
  }
}
```
Input parameters :
- `field`: must be of type [geo_point](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/geo-point.html)
- `zoom`: mandatory integer parameter between 0 and 25. It represents the zoom level used in the request to aggregate geo points
- `radius`: radius in pixel. It is used during the reduce phase to merge close clusters. Default to `40`
- `ratio`: ratio used to make a second merging pass during the reduce phase. If the value is `0`, no second pass is made. Default to `0`
- `extent`: Extent of the tiles. Default to `256`


#### Real-life example

Create an index:
```json
PUT test
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
  }
}
```

Push some points:
```json
POST test/_bulk?refresh
{"index":{"_id":1}}
{"location":[2.454929, 48.821578]}
{"index":{"_id":2}}
{"location":[2.245858, 48.86914]}
{"index":{"_id":3}}
{"location":[2.240358, 48.863481]}
{"index":{"_id":4}}
{"location":[2.25292, 48.847176]}
{"index":{"_id":5}}
{"location":[2.279111, 48.872383]}
{"index":{"_id":6}}
{"location":[2.336267, 48.822021]}
{"index":{"_id":7}}
{"location":[2.338677, 48.822672]}
{"index":{"_id":8}}
{"location":[2.336643, 48.822493]}
{"index":{"_id":9}}
{"location":[2.438465, 48.84204]}
{"index":{"_id":10}}
{"location":[2.381554, 48.835382]}
{"index":{"_id":11}}
{"location":[2.407744, 48.83733]}
{"index":{"_id":12}}
{"location":[2.34521, 48.849358]}
{"index":{"_id":13}}
{"location":[2.252938, 48.846041]}
{"index":{"_id":14}}
{"location":[2.279715, 48.871775]}
{"index":{"_id":15}}
{"location":[2.380629, 48.879757]}
```

Perform an aggregation:
```json
POST test/_search?size=0
{
  "aggregations": {
    "clusters": {
      "geo_point_clustering": {
        "field": "location",
        "zoom": 9
      }}}}
```

Result:
```json
"aggregations" : {
    "clusters" : {
      "buckets" : [
        {
          "geohash_grids" : [
            "u09wn",
            "u09tz",
            "u09ty",
            "u09tx",
            "u09tv",
            "u09tt"
          ],
          "doc_count" : 9,
          "centroid" : {
            "lat" : 48.83695897646248,
            "lon" : 2.380013056099415
          }
        },
        {
          "geohash_grids" : [
            "u09w5",
            "u09tg",
            "u09tf"
          ],
          "doc_count" : 6,
          "centroid" : {
            "lat" : 48.86166598415002,
            "lon" : 2.258483301848173
          }
        }
      ]
    }
```


## Development environment setup

### Build

Built with Java 17.
Apply spotless code formatting with:
``` shell
./gradlew spotlessApply
```

### Development Environment Setup

Build the plugin using gradle:
``` shell
./gradlew build
```

or
``` shell
./gradlew assemble  # (to avoid the test suite)
```

In case you have to upgrade Gradle, you can do it with `./gradlew wrapper --gradle-version x.y.z`.

Then the following command will start a dockerized ES and will install the previously built plugin:
``` shell
docker compose up
```

You can now check the Elasticsearch instance on `localhost:9200` and the plugin version at `localhost:9200/_cat/plugins`.

Please be careful during development: you'll need to manually rebuild the .zip using `./gradlew build` on each code
change before running `docker compose up` up again.

> NOTE: In `docker-compose.yml` you can uncomment the debug env and attach a REMOTE JVM on `*:5005` to debug the plugin.
