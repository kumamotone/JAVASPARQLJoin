//package org.aksw.jena_sparql_api.example;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import org.aksw.jena_sparql_api.cache.core.QueryExecutionFactoryCacheEx;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontendImpl;
import org.aksw.jena_sparql_api.cache.h2.CacheCoreH2;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.retry.core.QueryExecutionFactoryRetry;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
// import org.apache.commons.collections15.map.HashedMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.RunnableFuture;

class Relation {
    String[] attr;
    ArrayList<String[]> table;

    static String readFile(String path) {
        byte[] encoded = {};
        try {
            encoded = Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(encoded, StandardCharsets.UTF_8);
    }

    static ArrayList<String> split(char s[], char delim) {
        ArrayList<String> res = new ArrayList<String>();
        int j = 0;
        for (int i = 0; i < s.length; i++) {
            if (s[i] == delim) {
                res.add(new String(s, j, i - j));
                j = i + 1;
            }
        }
        return res;
    }

    // from csv
    Relation(char s[], boolean removeQuates) {
        ArrayList<String> rows = split(s, '\n');
        table = new ArrayList<String[]>();
        int m = 0;
        for (int i = 0; i < rows.size(); i++) {
            ArrayList<String> cols = split(rows.get(i).toCharArray(), ',');
            m = cols.size();
            String[] target = new String[m];
            for (int j = 0; j < m; j++) {
                String c = cols.get(j);
                if (removeQuates && c.length() >= 2 && c.charAt(0) == '"' && c.charAt(c.length() - 1) == '"') {
                    c = c.substring(1, c.length() - 1);
                }
                target[j] = c;
            }
            if (i == 0) {
                attr = target;
            } else {
                if(m < attr.length) continue;
                table.add(target);
            }
        }
    }

    // from hash join between two relations
    Relation(Relation R, Relation S, String keyR, String keyS) {
        if (R.table.size() > S.table.size()) {
            Relation tmp;
            tmp = R;
            R = S;
            S = tmp;

            String tmp2;
            tmp2 = keyR;
            keyR = keyS;
            keyS = tmp2;
        }

        int r = -1, s = -1;
        for (int i = 0; i < R.attr.length; i++) {
            if (R.attr[i].equals(keyR)) r = i;
        }
        for (int i = 0; i < S.attr.length; i++) {
            if (S.attr[i].equals(keyS)) s = i;
        }
        if (r == -1 || s == -1) {
            attr = new String[0];
            table = new ArrayList<String[]>();
            return;
        }

        HashMap<String, ArrayList<Integer>> hashtable = new HashMap<String, ArrayList<Integer>>();
        for (int i = 0; i < S.table.size(); i++) {
            if (!hashtable.containsKey(S.table.get(i)[s])) {
                hashtable.put(S.table.get(i)[s], new ArrayList<Integer>());
            }
            hashtable.get(S.table.get(i)[s]).add(i);
        }

        int n = R.attr.length + S.attr.length;
        attr = new String[n];
        for (int k = 0; k < R.attr.length; k++) {
            attr[k] = R.attr[k];
        }
        for (int k = 0; k < S.attr.length; k++) {
            attr[R.attr.length + k] = S.attr[k];
        }

        table = new ArrayList<String[]>();

        for (int i = 0; i < R.table.size(); i++) {
            if (!hashtable.containsKey(R.table.get(i)[r])) continue;
            ArrayList<Integer> target = hashtable.get(R.table.get(i)[r]);
            for (Integer j : target) {
                String[] vals = new String[n];
                for (int k = 0; k < R.attr.length; k++) {
                    String[] ppp = R.table.get(i);
                    vals[k] = ppp[k];
                    // System.out.println(vals.length + " " + ppp.length + " " + k + " " + R.attr.length);
                }
                for (int k = 0; k < S.attr.length; k++) {
                    vals[R.attr.length + k] = S.table.get(j)[k];
                }
                table.add(vals);
            }
        }
    }
}

/**
 * 色々やる便利なクラス
 */
class Benry {
    static Relation QueryJSON(String endpoint , String filename) {
        //StringBuilderを使って可変長の文字列を扱う
        //StringBuilderの使い方：http://www.javadrive.jp/start/stringbuilder/index1.html
        StringBuilder builder = new StringBuilder();
        //HttpClientのインスタンスを作る（HTTPリクエストを送るために必要）
        HttpClient client = new DefaultHttpClient();
        //HttpGetのインスタンスを作る（GETリクエストを送るために必要）
        StringBuilder requestUrl = new StringBuilder(endpoint);

        String queryString = Benry.readFile(filename);

        List<NameValuePair> params = new LinkedList<NameValuePair>();
        params.add(new BasicNameValuePair("query", queryString));
        params.add(new BasicNameValuePair("format", "text/csv"));
        String qs = URLEncodedUtils.format(params, "utf-8");
        requestUrl.append("?");
        requestUrl.append(qs);

        HttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(requestUrl.toString());
//        HttpGet httpGet = new HttpGet(
//                "?query=DEFINE+get%3Arefresh+%220%22+DEFINE+get%3Asoft+%22replace%22%0APREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0APREFIX+rdfs%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0APREFIX+bsbm%3A+%3Chttp%3A%2F%2Fwww4.wiwiss.fu-berlin.de%2Fbizer%2Fbsbm%2Fv01%2Fvocabulary%2F%3E%0APREFIX+dc%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Felements%2F1.1%2F%3E%0ASELECT+%3Fft+%3Fltlbl+%3Fftcmt+WHERE+%7B%0A++%3Fft+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type%3E+%3Chttp%3A%2F%2Fwww4.wiwiss.fu-berlin.de%2Fbizer%2Fbsbm%2Fv01%2Fvocabulary%2FProductFeature%3E.%0A++%3Fft+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23label%3E+%3Fftlbl.%0A++%3Fft+rdfs%3Acomment+%3Fftcmnt+.%0A++%3Fft+dc%3Adate+%3Fftdate+.%0A%7D%0A");
        //httpGet.getParams().setParameter("format", "application/sparql-results+json");
        try {
            //リクエストしたリンクが存在するか確認するために、HTTPリクエストを送ってHTTPレスポンスを取得する
            HttpResponse response = client.execute(httpGet);
            //返却されたHTTPレスポンスの中のステータスコードを調べる
            // -> statusCodeが200だったらページが存在。404だったらNot found（ページが存在しない）。500はInternal server error。
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                //HTTPレスポンスが200よりページは存在する
                //レスポンスからHTTPエンティティ（実体）を生成

                HttpEntity entity = response.getEntity();
                String retSrc = EntityUtils.toString(entity);
                long start = System.nanoTime();
                Relation res = new Relation(retSrc.toCharArray(), true);
                long end = System.nanoTime();
                System.out.printf("convert: %f",((end - start) / 1000000f));
                return res;
        //        return new JsonArray();
    //            System.out.println("parse start");
  //              JsonValue value = Json.parse(retSrc);
//                System.out.println("parse end");
                // JSONObject jsonobject = new JSONObject(retSrc);

                //return value.asObject().get("results").asObject().get("bindings").asArray();
                //return getJSONObject("results").getJSONArray("bindings");
                //HTTPエンティティからコンテント（中身）を生成
                //InputStream content = entity.getContent();
                //コンテントからInputStreamReaderを生成し、さらにBufferedReaderを作る
                //InputStreamReaderはテキストファイル（InputStream）を読み込む
                //BufferedReaderはテキストファイルを一行ずつ読み込む
                //（参考）http://www.tohoho-web.com/java/file.htm
                //BufferedReader reader = new BufferedReader(new InputStreamReader(content));
                //String line;
                //readerからreadline()で行を読んで、builder文字列(StringBuilderクラス)に格納していく。
                //※このプログラムの場合、lineは一行でなのでループは回っていない
                //※BufferedReaderを使うときは一般にこのように記述する。
                //while ((line = reader.readLine()) != null) {
                //    builder.append(line);
                //}
            } else {
                System.out.println("Failed to download file");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 文字列をJSONオブジェクトに変換する
        /*
        try {
            //JSON Arrayを作成する(文字列としてのJSONをJSON Arrayに変換)
            //builderはStringBuilderクラスなのでtoString()で文字列に変換
            //String str = builder.toString();
            long start = System.nanoTime();

            JSONObject jsonobject = new JSONObject(builder.toString());
            long end = System.nanoTime();
            System.out.printf("convert: %f",((end - start) / 1000000f));
            System.out.println();
            return jsonobject.getJSONObject("results").getJSONArray("bindings");
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        return null;
    }



    static String readFile(String path)
    {
        byte[] encoded = {};
        try {
            encoded = Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(encoded, StandardCharsets.UTF_8);
    }

    static JsonArray hashJoinJSONArrays(JsonArray R, JsonArray S, String X, String Y) {
        if (R.size() > S.size()) {
            JsonArray tmp;
            tmp = R;
            R = S;
            S = tmp;

            String tmp2;
            tmp2 = X;
            X = Y;
            Y = tmp2;
        }

        HashMap<String,ArrayList<JsonObject>> hashtable = new HashMap<String,ArrayList<JsonObject>>();

        for (int i = 0; i < R.size(); i++) {
                JsonObject r = R.get(i).asObject();
            String rkey = r.get(X).asObject().get("value").asString(); // R.getJSONObject(i).getString(X);
            if(!hashtable.containsKey(rkey.toString())) {
                ArrayList<JsonObject> list = new ArrayList<JsonObject>();
                list.add(r);
                hashtable.put(rkey, list);
            } else {
                ArrayList<JsonObject> list = hashtable.get(rkey);
                list.add(r);
            }
        }

        JsonArray result = new JsonArray();
        for (int i = 0; i < S.size(); i++) {
            JsonObject s = S.get(i).asObject();
            // String joinKey = s.getJSONObject(Y).getString("value"); // R.getJSONObject(i).getString(X);
            String joinKey = s.get(Y).asObject().get("value").asString(); // R.getJSONObject(i).getString(X);
            if(hashtable.containsKey(joinKey)) {
                ArrayList<JsonObject> r_list = hashtable.get(joinKey);
                for (int j = 0; j < r_list.size(); j++) {
                    JsonObject tuple = new JsonObject();
                    // HashMap<String,String> t = new HashMap<String,String>();
                    JsonObject r = r_list.get(j);
                    tuple.merge(r);
                    /*
                    Iterator<?> keys = r.keys();
                    while (keys.hasNext()) {
                        String key = (String) keys.next();
                        tuple.put(key, r.get(key).asString());
                    }

                    keys = s.keys();
                    while (keys.hasNext()) {
                        String key = (String) keys.next();
                        tuple.put(key, s.getString(key));
                    }
                    */
                    tuple.merge(s);

                    result.add(tuple);
                }
            }
        }
        System.out.println(result.size());
        return result;
    }
}

class ViewInfo implements Runnable {
    public static final String ENDPOINT30 = "http://130.158.76.30:8890/sparql";
    public static final String ENDPOINT22 = "http://130.158.76.22:8890/sparql";

    // private
    private String viewname;
    private String endpoint;
    private String filename;
    private String queryString;
    public boolean flag;

    public Relation getResult() {
        return result;
    }

    private Relation result;

    Thread thread;

    public ViewInfo(String viewname, String endpoint, String filename) {
        // パラメータをセット
        this.viewname = viewname;
        this.endpoint = endpoint;
        this.filename = filename;

        // クエリをファイルから読み込み
        this.queryString = Benry.readFile(filename);

        thread = new Thread(this);
        thread.start();
    }

    /**
     * クエリを実行
     */
    public void execQuery() {
    }

    static HashMap<String,ViewInfo> ViewInfoFactory() {
        ViewInfo product = new ViewInfo("product", ENDPOINT30, "/Users/kumamoto/new/SPARQLJoin/viewqueries/product.sparql");
        ViewInfo feature = new ViewInfo("feature", ENDPOINT22, "/Users/kumamoto/new/SPARQLJoin/viewqueries/feature.sparql");
        ViewInfo review = new ViewInfo("review", ENDPOINT30, "/Users/kumamoto/new/SPARQLJoin/viewqueries/review.sparql");
        ViewInfo person = new ViewInfo("person", ENDPOINT22, "/Users/kumamoto/new/SPARQLJoin/viewqueries/person.sparql");
        HashMap<String,ViewInfo> ret = new HashMap<String,ViewInfo>();
        ret.put(review.viewname, review);
        ret.put(product.viewname, product);
        ret.put(feature.viewname, feature);
        ret.put(person.viewname, person);
        return ret;
    }

    @Override
    public void run() {
        /*** 計測会死 ***/
        long start = System.nanoTime();

        System.out.println("Sending to "+ this.viewname);


        this.result = Benry.QueryJSON(this.endpoint, this.filename);
        System.out.println(this.viewname + ".length \t" + this.getResult().table.size());

        long end = System.nanoTime();
        System.out.println("Arrived"+this.viewname+"," + (end - start) / 1000000f);
        /*** 計測終了 ***/

        this.flag = true;
        //System.out.println(bindings.get(0));

        //joinCallback(this.dokomade);
        return;
    }
}
public class SPARQLJoin {

    public static void main(String[] args) throws Exception {
        long スタート = System.nanoTime();
        String[][] plan = {
                {"product", "prdct", "review", "rvwfr"}
                ,
                {"review", "rvwprsn", "person", "prsn"}
                ,
                {"product", "prdctft", "feature", "ft"}

        };

        HashMap<String,ViewInfo> viewinfos = ViewInfo.ViewInfoFactory();

        while(!viewinfos.get(plan[0][0]).flag || !viewinfos.get(plan[0][2]).flag) {
            //System.out.print("w");
            Thread.sleep(30);
        }

        long start = System.nanoTime();
//        JsonArray result = Benry.hashJoinJSONArrays(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
        Relation result = new Relation(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
        long end = System.nanoTime();
        System.out.printf("%s vs %s, %f\n", plan[0][0], plan[0][2] ,((end - start) / 1000000f));
        while(!viewinfos.get(plan[1][0]).flag || !viewinfos.get(plan[1][2]).flag) {
            Thread.sleep(30);
        }

        start = System.nanoTime();
//        JsonArray result2 = Benry.hashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
        Relation result2 = new Relation(result, viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
        end = System.nanoTime();
        System.out.printf("%s vs %s, %f\n", plan[1][0], plan[1][2] ,((end - start) / 1000000f));


        start = System.nanoTime();
//        JsonArray result2 = Benry.hashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
        Relation result3 = new Relation(result2, viewinfos.get(plan[2][2]).getResult(),  plan[2][1],plan[2][3]);
        end = System.nanoTime();
        System.out.printf("%s vs %s, %f\n", plan[2][0], plan[2][2] ,((end - start) / 1000000f));


        long 真end = System.nanoTime();
        System.out.printf("total, %f\n",((真end - スタート) / 1000000f));
    }
}