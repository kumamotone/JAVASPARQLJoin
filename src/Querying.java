import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonValue;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * SPARQL エンドポイントとの通信
 */
class Querying {
    static long getFileSize(String path) {
        File file = new File(path);
        return file.length()/1000/1000;
    }
    /**
     * filename のSPARQLクエリを実行してJSONArrayで返す
     */
    static JsonArray QueryJSON(ViewInfo viewinfo) {
        long start = System.nanoTime();
        String retSrc = CallEPJSON(viewinfo.endpoint, viewinfo.filename);
        long end = System.nanoTime();
        double arrivedTime = ((end - start) / 1000000f);
        System.out.printf("Arrived %s\t%f \n",viewinfo.viewname, arrivedTime);

        start = System.nanoTime();
        JsonValue value = Json.parse(retSrc);
        end = System.nanoTime();

        JsonArray retArray = value.asObject().get("results").asObject().get("bindings").asArray();
        System.out.printf("Convert %s\t%f \n",viewinfo.viewname,((end - start) / 1000000f));

        Benry.WriteFile(viewinfo.filename.replace("sparql", "json") ,retSrc);
        long size = getFileSize(viewinfo.filename.replace("sparql", "json"));
        System.out.printf("Size %s\t%d \n",viewinfo.viewname, size);
        double transferTime = (double)size*1000*1000 / viewinfo.speedKB;
        System.out.printf("Transfer Time %s\t%f \n",viewinfo.viewname, transferTime);
        System.out.printf("Execute Time %s\t%f \n",viewinfo.viewname, arrivedTime-transferTime);

        return retArray;
    }

    static String CallEPJSON(String endpoint, String filename) {

        StringBuilder builder = new StringBuilder();
        HttpClient client = new DefaultHttpClient();
        StringBuilder requestUrl = new StringBuilder(endpoint);

        String queryString = Benry.ReadFile(filename);

        List<NameValuePair> params = new LinkedList<NameValuePair>();
        params.add(new BasicNameValuePair("query", queryString));
        //params.add(new BasicNameValuePair("format", "text/csv"));
        params.add(new BasicNameValuePair("format", "application/sparql-results+json"));
        String qs = URLEncodedUtils.format(params, "utf-8");
        requestUrl.append("?");
        requestUrl.append(qs);

        HttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(requestUrl.toString());
        try {
            HttpResponse response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                //HTTPレスポンスが200よりページは存在する
                //レスポンスからHTTPエンティティ（実体）を生成

                HttpEntity entity = response.getEntity();
                String retSrc = EntityUtils.toString(entity);
                return retSrc;
                //return value.asObject().get("results").asObject().get("bindings").asArray();
                // return res;
                //        return new JsonArray();
                //            System.out.println("parse start");
                //              JsonValue value = Json.parse(retSrc);
//                System.out.println("parse end");
                // JSONObject jsonobject = new JSONObject(retSrc);

                //return value.asObject().get("results").asObject().get("bindings").asArray();
                //return getJSONObject("results").getJSONArray("bindings");
            } else {
                System.out.println("Failed to download file");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    static Relation QueryCSV(String endpoint, String filename) {
        String retSrc = CallEPCSV(endpoint, filename);

        long start = System.nanoTime();
        Relation res = new Relation(retSrc.toCharArray(), true);
        long end = System.nanoTime();
        System.out.printf("convert: %f",((end - start) / 1000000f));
        Benry.WriteFile(filename.replace("sparql", "csv") ,retSrc);
        return res;
    }


    static String CallEPCSV(String endpoint , String filename) {
        StringBuilder builder = new StringBuilder();
        HttpClient client = new DefaultHttpClient();
        StringBuilder requestUrl = new StringBuilder(endpoint);

        String queryString = Benry.ReadFile(filename);

        List<NameValuePair> params = new LinkedList<NameValuePair>();
        params.add(new BasicNameValuePair("query", queryString));
        params.add(new BasicNameValuePair("format", "text/csv"));
        String qs = URLEncodedUtils.format(params, "utf-8");
        requestUrl.append("?");
        requestUrl.append(qs);

        HttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(requestUrl.toString());
        try {
            HttpResponse response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                String retSrc = EntityUtils.toString(entity);
                return retSrc;
            } else {
                System.out.println("Failed to download file");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
