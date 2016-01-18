import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * 色々やる便利なクラス
 */
class Benry {

    static void WriteFile(String path, String text) {
        try {
            PrintWriter out = new PrintWriter(path);
            out.println(text);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    static String ReadFile(String path)
    {
        byte[] encoded = {};
        try {
            encoded = Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(encoded, StandardCharsets.UTF_8);
    }

}
