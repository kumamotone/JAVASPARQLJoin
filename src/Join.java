import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 結合演算に関するやつ
 */
public class Join {
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
