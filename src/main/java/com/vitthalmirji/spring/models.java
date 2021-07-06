package com.vitthalmirji.spring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class models {

    private SparkSession sparkSession;

    public Dataset<Row> DataFrame;
    static ArrayList<String> final_list = new ArrayList<String>();
    static Map<String, Integer> mapp = new HashMap();

    public models(SparkSession sparkSession) {
//        sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[*]").getOrCreate();
        DataFrame = sparkSession.read()
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("delimiter", ",")
                .csv("src/main/resources/Wuzzuf_Jobs.csv");
        DataFrame.dropDuplicates();
        DataFrame.na().drop();

    }
    /*
    public StructType print_schema(){
        return DataFrame.schema();
    }

     */

    public Dataset<Row> Summary1(){
        return DataFrame.summary();
    }
/*
    public Dataset<Row> Counting_job() {
        DataFrame.createOrReplaceTempView("count_jobs");
        final Dataset<Row> Counting = sparkSession
                .sql("SELECT Company, COUNT(DISTINCT Title) as var FROM count_jobs GROUP BY(Company) ORDER BY(var) DESC LIMIT 10");
        return Counting;
    }

    public PieChart display_piechart(String title) {
        Map<String, Integer> joblist = new HashMap<>();

        DataFrame.collectAsList().stream().forEach(i -> joblist.put(i.get(0).toString().trim(), Integer.valueOf(i.get(1).toString().trim())));
        piechart piechar = new piechart();

        return piechar.piecha(joblist, title, 10);
    }

    public Dataset<Row> Count_title() {
        DataFrame.createOrReplaceTempView("count_title");
        final Dataset<Row> Counting = sparkSession
                .sql("SELECT Title, COUNT(Title) as var FROM count_title GROUP BY(Title) ORDER BY(var)DESC LIMIT(10)");
        return Counting;
    }

    public CategoryChart display_barchart(String title) {
        ArrayList<String> jobs = new ArrayList<>();
        ArrayList<Long> jobsc = new ArrayList<>();

        DataFrame.collectAsList().stream().forEach((i -> jobs.add((String) i.get(0))));
        DataFrame.collectAsList().stream().forEach((i -> jobsc.add((Long) i.get(1))));
        System.out.println(jobs);
        barchart barch = new barchart();
        return barch.b(jobs,jobsc, title);
    }
    public Dataset<Row> Counting_areas()
    {
        DataFrame.createOrReplaceTempView("count_Areas");
        final Dataset<Row> Counting = sparkSession
                .sql ("SELECT Location, COUNT(Location) as var FROM count_Areas GROUP BY(Location) ORDER BY(var)DESC LIMIT(10)");
        return Counting ;
    }
    public Map<String,Integer> Splitting_Skills(){
        Dataset<Row> s = DataFrame.select("Skills");
        s.foreach((ForeachFunction<Row>) row -> {
            StringTokenizer st = new StringTokenizer(row.toString(), ",[]");

            //System.out.println(row);//first
            while (st.hasMoreTokens()) {
                String temp = st.nextToken();
                if (temp.charAt(0) == ' ') {
                    temp = temp.substring(1);
                }
                // System.out.println(temp);
                final_list.add(temp) ;
                int occurrences = Collections.frequency(final_list, temp);
                mapp.put(temp ,occurrences);
            }

        });
        //System.out.println(final_list);

        Map<String, Integer> sorted =
                mapp.entrySet()
                        .stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(),(e1, e2) -> e2,LinkedHashMap::new));
     return sorted ;
    }

     */
}
