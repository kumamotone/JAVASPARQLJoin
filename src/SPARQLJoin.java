import com.eclipsesource.json.JsonArray;
import java.util.*;

/**
 * メイン的位置合い
 */
public class SPARQLJoin {
    public static void exp1_prop() throws Exception {

        long スタート = System.nanoTime();
        String[][] plan = {
                {"product", "prdctft", "feature", "ft"}
                //        ,
                //         {"review", "rvwprsn", "person", "prsn"}
                ,
                {"product", "prdct", "review", "rvwfr"}

        };

        HashMap<String,ViewInfo> viewinfos = ViewInfo.ViewInfoFactory();

        while(!viewinfos.get(plan[0][0]).flag || !viewinfos.get(plan[0][2]).flag) {
            //System.out.print("w");
            Thread.sleep(30);
        }

        long start = System.nanoTime();
        JsonArray result = Join.hashJoinJSONArrays(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
//        Relation result = new Relation(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
        long end = System.nanoTime();
        System.out.printf("%s vs %s\t%f\n", plan[0][0], plan[0][2] ,((end - start) / 1000000f));
        while(!viewinfos.get(plan[1][0]).flag || !viewinfos.get(plan[1][2]).flag) {
            Thread.sleep(30);
        }

        start = System.nanoTime();
        JsonArray result2 = Join.hashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
//        Relation result2 = new Relation(result, viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
        end = System.nanoTime();
        System.out.printf("%s vs %s\t%f\n", plan[1][0], plan[1][2] ,((end - start) / 1000000f));


//        start = System.nanoTime();
//        JsonArray result3 = Join.HashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
//        Relation result3 = new Relation(result2, viewinfos.get(plan[2][2]).getResult(),  plan[2][1],plan[2][3]);
//        end = System.nanoTime();
//        System.out.printf("%s vs %s, %f\n", plan[2][0], plan[2][2] ,((end - start) / 1000000f));


        long 真end = System.nanoTime();
        System.out.printf("total\t%f\n",((真end - スタート) / 1000000f));
    }


    public static void exp1_exis() throws Exception {

        long スタート = System.nanoTime();
        String[][] plan = {
                {"product", "prdct", "review", "rvwfr"}
                //        ,
                //         {"review", "rvwprsn", "person", "prsn"}
                ,
                {"product", "prdctft", "feature", "ft"}

        };

        HashMap<String,ViewInfo> viewinfos = ViewInfo.ViewInfoFactory();

        while(!viewinfos.get(plan[0][0]).flag || !viewinfos.get(plan[0][2]).flag) {
            //System.out.print("w");
            Thread.sleep(100);
        }

        long start = System.nanoTime();
        JsonArray result = Join.hashJoinJSONArrays(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
//        Relation result = new Relation(viewinfos.get(plan[0][0]).getResult(), viewinfos.get(plan[0][2]).getResult(), plan[0][1],plan[0][3]);
        long end = System.nanoTime();
        System.out.printf("%s vs %s\t%f\n", plan[0][0], plan[0][2] ,((end - start) / 1000000f));
        while(!viewinfos.get(plan[1][0]).flag || !viewinfos.get(plan[1][2]).flag) {
            Thread.sleep(100);
        }

        start = System.nanoTime();
        JsonArray result2 = Join.hashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
//        Relation result2 = new Relation(result, viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
        end = System.nanoTime();
        System.out.printf("%s vs %s\t%f\n", plan[1][0], plan[1][2] ,((end - start) / 1000000f));


//        start = System.nanoTime();
//        JsonArray result3 = Join.HashJoinJSONArrays(result , viewinfos.get(plan[1][2]).getResult(),  plan[1][1],plan[1][3]);
//        Relation result3 = new Relation(result2, viewinfos.get(plan[2][2]).getResult(),  plan[2][1],plan[2][3]);
//        end = System.nanoTime();
//        System.out.printf("%s vs %s, %f\n", plan[2][0], plan[2][2] ,((end - start) / 1000000f));


        long 真end = System.nanoTime();
        System.out.printf("total\t %f\n",((真end - スタート) / 1000000f));
    }

    public static void main(String[] args) throws Exception {
        //System.out.println("test\ttest");
        for(int i=0; i<5; i++) {
            System.out.printf("======1 Prop %d=======\n",i);
            exp1_prop();
        }
        for(int i=0; i<5; i++) {
            System.out.printf("======1 Exis %d=======\n",i);
            exp1_exis();
        }
    }
}