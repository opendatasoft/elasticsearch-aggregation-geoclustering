package com.opendatasoft.elasticsearch;

//import org.elasticsearch.common.xcontent.XContentParser;
//import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
//import static org.hamcrest.Matchers.containsString;

public class GeoPointClusteringParserTests extends ESTestCase {

    public void testParseValidZoom() throws Exception {
//        int zoom = randomIntBetween(1, 25);
//        XContentParser stParser = createParser(JsonXContent.jsonXContent,
//                "{\"field\":\"my_loc\", \"zoom\":" + zoom + "}");
//        XContentParser.Token token = stParser.nextToken();
//        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
//        assertNotNull(GeoPointClusteringAggregationBuilder.parse("geo_point_clustering", stParser));
    }

    public void testParseInValidZoom() throws Exception {
//        int zoom = randomIntBetween(26, 99);
//        XContentParser stParser = createParser(JsonXContent.jsonXContent,
//                "{\"field\":\"my_loc\", \"zoom\":" + zoom + "}");
//        XContentParser.Token token = stParser.nextToken();
//        assertSame(XContentParser.Token.START_OBJECT, token);

//        XContentParseException ex = expectThrows(XContentParseException.class,
//                () -> GeoPointClusteringAggregationBuilder.parse("geo_point_clustering", stParser));
//        assertThat(ex.getMessage(), containsString("[geo_point_clustering] failed to parse field [zoom]"));
    }
}
