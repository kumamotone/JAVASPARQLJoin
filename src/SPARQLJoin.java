//package org.aksw.jena_sparql_api.example;

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
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RunnableFuture;

/**
 * 色々やる便利なクラス
 */
class Benry {
    /**
     * ファイルから全部読んで返す
     * @param path パス
     * @param encoding エンコーディング
     * @return 読み込んだファイル
     */
    static String readFile(String path, Charset encoding)
    {
        byte[] encoded = {};
        try {
            encoded = Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(encoded, encoding);
    }

    static JSONArray hashJoinJSONArrays(JSONArray R, JSONArray S, String X, String Y) {
        if (R.length() > S.length()) {
            JSONArray tmp;
            tmp = R;
            R = S;
            S = tmp;

            String tmp2;
            tmp2 = X;
            X = Y;
            Y = tmp2;
        }

        HashMap<String,ArrayList<JSONObject>> hashtable = new HashMap<String,ArrayList<JSONObject>>();

        for (int i = 0; i < R.length(); i++) {
            JSONObject r = R.getJSONObject(i);
            String rkey = r.getString(X); // R.getJSONObject(i).getString(X);
            if(!hashtable.containsKey(rkey.toString())) {
                ArrayList<JSONObject> list = new ArrayList<JSONObject>();
                list.add(r);
                hashtable.put(rkey, list);
            } else {
                ArrayList<JSONObject> list = hashtable.get(rkey);
                list.add(r);
            }
        }

        JSONArray result = new JSONArray();
        for (int i = 0; i < S.length(); i++) {
            JSONObject s = S.getJSONObject(i);
            // String joinKey = s.getJSONObject(Y).getString("value"); // R.getJSONObject(i).getString(X);
            String joinKey = s.getString(Y); // R.getJSONObject(i).getString(X);
            if(hashtable.containsKey(joinKey)) {
                ArrayList<JSONObject> r_list = hashtable.get(joinKey);
                for (int j = 0; j < r_list.size(); j++) {
                    JSONObject tuple = new JSONObject();
                    // HashMap<String,String> t = new HashMap<String,String>();
                    JSONObject r = r_list.get(j);
                    Iterator<?> keys = r.keys();
                    while (keys.hasNext()) {
                        String key = (String) keys.next();
                        tuple.put(key, r.getString(key));
                    }

                    keys = s.keys();
                    while (keys.hasNext()) {
                        String key = (String) keys.next();
                        tuple.put(key, s.getString(key));
                    }

                    result.put(tuple);
                }
            }
        }
        System.out.println(result.length());
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
    private int dokomade;
    public boolean flag;

    public JSONArray getResult() {
        return result;
    }

    private JSONArray result;

    // SPARQL things.
    QueryExecutionFactory qef;
    QueryExecution qe;

    Thread thread;

    /**
     * コンストラクタ
     * @param viewname
     * @param endpoint
     * @param filename
     * @param dokomade
     */
    public ViewInfo(String viewname, String endpoint, String filename, int dokomade) {
        // パラメータをセット
        this.viewname = viewname;
        this.endpoint = endpoint;
        this.filename = filename;
        this.dokomade = dokomade;

        // クエリをファイルから読み込み
        this.queryString = Benry.readFile(filename, StandardCharsets.UTF_8);

        // SPARQLクライアントの設定
        this.qef = new QueryExecutionFactoryHttp(this.endpoint);
        this.qef = new QueryExecutionFactoryRetry(this.qef, 5, 10000);
        this.qe = this.qef.createQueryExecution(this.queryString);


        long timeToLive = 1;// 24l * 60l * 60l * 1000l;

        // This creates a 'cache' folder, with a database file named 'sparql.db'
        // Technical note: the cacheBackend's purpose is to only deal with streams,
        // whereas the frontend interfaces with higher level classes - i.e. ResultSet and Model

        CacheBackend cacheBackend = null;
        try {
            //cacheBackend = CacheCoreH2.create("sparql", timeToLive, true);
            cacheBackend = CacheCoreH2.create(false, "~/", "hoga", 1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);
        this.qef = new QueryExecutionFactoryCacheEx(qef, cacheFrontend);

        thread = new Thread(this);
        thread.start();
    }

    /**
     * クエリを実行
     */
    public void execQuery() {
    }

    static HashMap<String,ViewInfo> ViewInfoFactory() {
        ViewInfo product = new ViewInfo("product", ENDPOINT30, "/Users/kumamoto/new/SPARQLJoin/viewqueries/product.sparql", 0);
        ViewInfo feature = new ViewInfo("feature", ENDPOINT22, "/Users/kumamoto/new/SPARQLJoin/viewqueries/feature.sparql", 0);
        ViewInfo review = new ViewInfo("review", ENDPOINT30, "/Users/kumamoto/new/SPARQLJoin/viewqueries/review.sparql", 1);
        HashMap<String,ViewInfo> ret = new HashMap<String,ViewInfo>();
        ret.put(product.viewname, product);
        ret.put(feature.viewname, feature);
        ret.put(review.viewname, review);
        return ret;
    }

    @Override
    public void run() {
        /*** 計測会死 ***/
        long start = System.nanoTime();

        System.out.println("Sending to "+ this.viewname);
        ResultSet rs = qe.execSelect();
        //long end = System.nanoTime();
        this.result = new JSONArray();
        while (rs.hasNext()) {
            JSONObject jo = new JSONObject();
            Binding b = rs.nextBinding();
            //List<String> vars = rs.getResultVars();
            Iterator<?> vars = b.vars();
            while (vars.hasNext()) {
                Var var = (Var) vars.next();
                jo.put(var.getVarName(), b.get(var).getIndexingValue().toString());
                //jo.put(s, b.get((Var) s);
            }
            this.result.put(jo);
        }
        //System.out.println("Time:" + (end - start) / 1000000f + "ms");
        /*
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(baos, rs);
        end = System.nanoTime();
        System.out.println("Time:" + (end - start) / 1000000f + "ms");
        JSONObject jo = new JSONObject(baos.toString());
        JSONObject jo2 = (JSONObject) jo.get("results");
        JSONArray bindings = (JSONArray)jo2.get("bindings");
        this.result = bindings;
        */

        System.out.println(this.viewname + ".length \t" + rs.getRowNumber());

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
        String[][] plan = {
                {"product", "prdct", "review", "rvwfr"}
                ,
                {"product", "prdctft", "feature", "ft"}
        };
        HashMap<String,ViewInfo> viewinfos = ViewInfo.ViewInfoFactory();

        while(!viewinfos.get(plan[0][0]).flag || !viewinfos.get(plan[0][2]).flag) {
            //System.out.print("w");
            Thread.sleep(10);
        }

        long start = System.nanoTime();
        JSONArray result = Benry.hashJoinJSONArrays(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
        long end = System.nanoTime();
        System.out.printf("%s vs %s, %f", plan[0][0], plan[0][2] ,((end - start) / 1000000f));
        while(!viewinfos.get(plan[1][0]).flag || !viewinfos.get(plan[1][2]).flag) {
            Thread.sleep(10);
        }

        start = System.nanoTime();
        JSONArray result2 = Benry.hashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
        end = System.nanoTime();
        System.out.printf("%s vs %s, %f", plan[1][0], plan[1][2] ,((end - start) / 1000000f));
    }
}