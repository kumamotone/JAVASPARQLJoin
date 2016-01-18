import com.eclipsesource.json.JsonArray;

import java.util.HashMap;

/**
 * ビュー情報
 */
class ViewInfo implements Runnable {
    public static final String ENDPOINT11 = "http://130.158.76.11:8890/sparql";
    public static final String ENDPOINT22 = "http://130.158.76.22:8890/sparql";
    public static final String ENDPOINT30 = "http://130.158.76.30:8890/sparql";
    public static final String LOCAL91 = "http://192.168.0.91:8890/sparql";
    public static final String LOCAL92 = "http://192.168.0.92:8890/sparql";

    private void setSpeed() {
        if(this.endpoint.equals(ENDPOINT30)) {
            this.speedKB = 40960;
        } else if(this.endpoint.equals(ENDPOINT22)) {
            this.speedKB = 40960;
        } else if(this.endpoint.equals(ENDPOINT11)) {
            this.speedKB = 40960;
        } else if(this.endpoint.equals(LOCAL91)) {
            this.speedKB = 512;
        } else if(this.endpoint.equals(LOCAL92)) {
            this.speedKB = 512;
        }
    }

    // fxxk off OOP
    public String viewname;
    public String endpoint;
    public String filename;
    public String queryString;
    public boolean flag;
    public int speedKB;

    public JsonArray getResult() {
        return result;
    }

    private JsonArray result;

    Thread thread;

    public ViewInfo(String viewname, String endpoint, String filename) {
        // パラメータをセット
        this.viewname = viewname;
        this.endpoint = endpoint;
        this.filename = filename;
        setSpeed();

        // クエリをファイルから読み込み
        this.queryString = Benry.ReadFile(filename);

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
        ViewInfo feature = new ViewInfo("feature", LOCAL91, "/Users/kumamoto/new/SPARQLJoin/viewqueries/feature.sparql");
        ViewInfo review = new ViewInfo("review", ENDPOINT22, "/Users/kumamoto/new/SPARQLJoin/viewqueries/review.sparql");
        //ViewInfo person = new ViewInfo("person", ENDPOINT22, "/Users/kumamoto/new/SPARQLJoin/viewqueries/person.sparql");
        HashMap<String,ViewInfo> ret = new HashMap<String,ViewInfo>();
        ret.put(review.viewname, review);
        ret.put(product.viewname, product);
        ret.put(feature.viewname, feature);
        //ret.put(person.viewname, person);
        return ret;
    }

    @Override
    public void run() {
        /*** 計測会死 ***/
        long start = System.nanoTime();

        // System.out.println("Sending to "+ this.viewname);

        this.result = Querying.QueryJSON(this);
        // System.out.println(this.viewname + ".length \t" + this.getResult().size());

        long end = System.nanoTime();
        System.out.println("Ready "+this.viewname+"\t" + (end - start) / 1000000f);
        /*** 計測終了 ***/

        this.flag = true;
        //System.out.println(bindings.get(0));

        //joinCallback(this.dokomade);
        return;
    }
}