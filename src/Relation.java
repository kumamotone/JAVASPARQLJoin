import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * CSV をリレーションとして持っておくための型
 */
class Relation {
    String[] attr;
    ArrayList<String[]> table;

    static String ReadFile(String path) {
        byte[] encoded = {};
        try {
            encoded = Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(encoded, StandardCharsets.UTF_8);
    }

    static ArrayList<String> Split(char s[], char delim) {
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
        ArrayList<String> rows = Split(s, '\n');
        table = new ArrayList<String[]>();
        int m = 0;
        for (int i = 0; i < rows.size(); i++) {
            ArrayList<String> cols = Split(rows.get(i).toCharArray(), ',');
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
