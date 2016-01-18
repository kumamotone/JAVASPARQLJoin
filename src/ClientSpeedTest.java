import com.bluelinelabs.logansquare.LoganSquare;
import com.bluelinelabs.logansquare.annotation.JsonObject;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonValue;
import com.google.gson.Gson;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformScopeRename;

import java.io.IOException;

/**
 * 非常に大きなJSONオブジェクトの生成テスト
 */

public class ClientSpeedTest {
    public static void main(String[] args) throws IOException {
        // Querying.QueryCSV(ViewInfo.ENDPOINT30, "/Users/kumamoto/new/SPARQLJoin/viewqueries/product.sparql");
        //String s = Transform.getJSONFromFile("/Users/kumamoto/new/SPARQLJoin/viewqueries/feature.csv", "\n");
        long start = System.nanoTime();
        JsonArray value = Querying.QueryJSON(new ViewInfo("test", ViewInfo.ENDPOINT30, "/Users/kumamoto/new/SPARQLJoin/viewqueries/product2.sparql"));
        System.out.println("" +  value.size());
        // Feature f = LoganSquare.parse(s, Feature.class);
        // LoganSquare.parseArray();
        // Gson gson = new Gson();
        // Feature f =  gson.fromJson(s,Feature.class);

        // Benry.WriteFile("/Users/kumamoto/new/SPARQLJoin/viewqueries/test.json", writeStr);

        long end = System.nanoTime();
        System.out.printf("%f",((end - start) / 1000000f));


        // String file = Benry.ReadFile("/Users/kumamoto/new/SPARQLJoin/viewqueries/feature.json");
        // System.out.printf("convert: %f",((end - start) / 1000000f));
    }
}
