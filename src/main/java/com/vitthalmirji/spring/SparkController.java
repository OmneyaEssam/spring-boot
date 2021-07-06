package com.vitthalmirji.spring;
import org.apache.commons.io.FileUtils;
import org.knowm.xchart.CategoryChart;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.BitmapEncoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.apache.spark.api.java.function.ForeachFunction ;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors ;

//@RequestMapping("spark-context")
@Controller
public class SparkController {
    @Autowired
    private SparkSession sparkSession;
    public Dataset<Row> dataset = null;
    static ArrayList<String> final_list = new ArrayList<String>();
    static Map<String, Integer> mapp = new HashMap();

//----------------------------------Read Dataframe-------------------------------
    public ResponseEntity<String> convert_tojason(Dataset<Row> data) {

        Dataset<String> df = data.toJSON();
        String s = df.showString(10000, 30000, false);
        String[] _ = s.split("\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < _.length; i += 2)
            try {
                sb.append(_[i].split("\\|")[1] + "<br>");
            } catch (Exception e) {

            }
        return ResponseEntity.ok(sb.toString());
    }
    public void readit() {
        if (dataset != null)
            return;
        dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
    }

    @RequestMapping("read-csv")
    public ResponseEntity<Object> ReadData() {

        readit();
        dataset = dataset.na().drop();
        dataset = dataset.dropDuplicates();
        Dataset<String> df = dataset.toJSON();
        String s = df.showString(10000, 30000, false);
        String[] _ = s.split("\n");
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i < _.length; i += 2)
            try {
                sb.append(_[i].split("\\|")[1] + "<br>");
            } catch (Exception e) {

            }

        return ResponseEntity.ok(sb.toString());
    }

//-------------------------------Summary-----------------------------------------
    @RequestMapping("summaryy")
    public ResponseEntity<String> get_summaryy() {
        readit();
        Dataset<Row> df = dataset.summary();
        return convert_tojason(df);
    }
//----------------------------------Job count------------------------------------
    public Dataset<Row> get_jobcount() {
    readit();
    dataset.createOrReplaceTempView("count_jobs");
    Dataset<Row> data_count = sparkSession.sql("SELECT Company, COUNT (Title) as var FROM count_jobs GROUP BY(Company) ORDER BY(var) DESC LIMIT 10");
    // Counting.show();
    return data_count;
}

    @RequestMapping("jobcount")
    public ResponseEntity<String> getjobcount() {
        Dataset<Row> data = get_jobcount();
        return convert_tojason(data);
    }

    @Autowired
    public piechart displaypie() throws IOException {
        Dataset<Row> data = get_jobcount();
        Map<String, Integer> joblist = new HashMap<>();
        data.collectAsList().stream().forEach(i -> joblist.put(i.get(0).toString().trim(), Integer.valueOf(i.get(1).toString().trim())));
        piechart piechar = new piechart();
        BitmapEncoder.saveBitmap(piechar.piecha(joblist, "Jobs per Company", 10), "./public/PieChart", BitmapEncoder.BitmapFormat.PNG);
        return piechar;
    }

    @RequestMapping("/pie")
    public ResponseEntity<String> display_image() {
        return ResponseEntity.ok("<img src=\"PieChart.png\" />");
    }

    //-----------------------------------Title count----------------------------
    public Dataset<Row> titlecount1() {
        readit();
        dataset.createOrReplaceTempView("count_title");
        final Dataset<Row> Counting = sparkSession
                .sql("SELECT Title, COUNT(Title) as var FROM count_title GROUP BY(Title) ORDER BY(var)DESC LIMIT(10)");
        return Counting;
    }

    @RequestMapping("titlecount")
    public ResponseEntity<String> get_titles() {
        readit();
        Dataset<Row> title_count = titlecount1();
        return convert_tojason(title_count);
    }

    @Autowired
    public barchart bartitle_chart() throws IOException {
        Dataset<Row> titles = titlecount1();
        ArrayList<String> jobs = new ArrayList<>();
        ArrayList<Long> jobsc = new ArrayList<>();
        titles.collectAsList().stream().forEach((i -> jobs.add((String) i.get(0))));
        titles.collectAsList().stream().forEach((i -> jobsc.add((Long) i.get(1))));
        barchart barch = new barchart();
        BitmapEncoder.saveBitmap(barch.b(jobs, jobsc, "Jobs Count"), "public/barchart1", BitmapEncoder.BitmapFormat.PNG);
        return barch;
    }

    @RequestMapping("/bar1")
    public ResponseEntity<String> display_image_bar() {
        return ResponseEntity.ok("<img src=\"barchart1.png\" />");
    }

    //-------------------------------------Areas Count---------------------------
    public Dataset<Row> display_areaas() {
        readit();
        dataset.createOrReplaceTempView("count_Areas");
        Dataset<Row> Counting = sparkSession
                .sql("SELECT Location, COUNT(Location) as var FROM count_Areas GROUP BY(Location) ORDER BY(var)DESC LIMIT(10)");
        return Counting;

    }

    @RequestMapping("areascount")
    public ResponseEntity<String> get_areascount() {
        Dataset<Row> counting_A = display_areaas();
        return convert_tojason(counting_A);
    }

    @Autowired
    public barchart bararea_chart() throws IOException {
        Dataset<Row> areas = display_areaas();
        ArrayList<String> jobs = new ArrayList<>();
        ArrayList<Long> jobsc = new ArrayList<>();
        areas.collectAsList().stream().forEach((i -> jobs.add((String) i.get(0))));
        areas.collectAsList().stream().forEach((i -> jobsc.add((Long) i.get(1))));
        barchart barch = new barchart();
        BitmapEncoder.saveBitmap(barch.b(jobs, jobsc, "Areas"), "public/barchart2", BitmapEncoder.BitmapFormat.PNG);
        return barch;
    }

    @RequestMapping("/bar2")
    public ResponseEntity<String> display_image_bar2() {
        return ResponseEntity.ok("<img src=\"barchart2.png\" />");
    }

    //-----------------------------------Most Important skills---------------------
    @RequestMapping("skillscount")
    public ResponseEntity<String> get_skills() throws JsonProcessingException {
        readit();
        Dataset<Row> s = dataset.select("Skills");
        s.foreach((ForeachFunction<Row>) row -> {
            StringTokenizer st = new StringTokenizer(row.toString(), ",[]");

            //System.out.println(row);//first
            while (st.hasMoreTokens()) {
                String temp = st.nextToken();
                if (temp.charAt(0) == ' ') {
                    temp = temp.substring(1);
                }
                // System.out.println(temp);
                final_list.add(temp);
                int occurrences = Collections.frequency(final_list, temp);
                mapp.put(temp, occurrences);
            }

        });
        //System.out.println(final_list);
        Map<String, Integer> Skill_map =
                mapp.entrySet()
                        .stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));
        String jason = new ObjectMapper().writeValueAsString(Skill_map);
        //return new ResponseEntity<>(Skill_map, HttpStatus.OK);
        return ResponseEntity.ok(jason);
    }
    //------------------------- Factorizing YearsExp column------------------------
    @RequestMapping("factorizeYrsOfExp")
    public ResponseEntity<String> factorize() throws JsonProcessingException {
        readit();
        Dataset<Row> s = dataset.select("YearsExp");
        s.foreach((ForeachFunction<Row>) row -> {
            StringTokenizer st = new StringTokenizer(row.toString(), "Yrs of Exp[]");

            //System.out.println(row);//first
            while (st.hasMoreTokens()) {
                String temp = st.nextToken();
                if (temp.charAt(0) == ' ') {
                    temp = temp.substring(1);
                }
                //System.out.println(temp);

                final_list.add(temp);
            }

        });

        ArrayList<Integer> listt = new ArrayList<Integer>();
        // ArrayList<String[]> splitting = new ArrayList<>();
        String[] Splitting = new String[0];
        for (String e : final_list) {
            if (e.equals("null")) {
                listt.add(0);
            } else if (e.charAt(1) == '+') {
                Splitting = e.split("\\+");
                int x = Integer.parseInt(Splitting[0]);
                listt.add(x);
            } else if (e.charAt(1) == '-') {
                Splitting = e.split("-");
                int x = Integer.parseInt(Splitting[0]);
                int y = Integer.parseInt(Splitting[1]);
                for (int i = x; i <= y; i++) {
                    listt.add(i);
                }
            }
        }
        ;
        Map<Integer, Integer> map1 = new HashMap();
        for (int v : listt) {
            int occurrences = Collections.frequency(listt, v);
            map1.put(v, occurrences);
        }
        String jason = new ObjectMapper().writeValueAsString(map1);
        return ResponseEntity.ok(jason);

    }
    //-----------------------------------------------------------------------------------------
    //---------------------------------- Deleting Nulls from YearsExp--------------------------
    @RequestMapping("Null_inyears")
    public ResponseEntity<String> get_Nulls() {
        readit();
        dataset.createOrReplaceTempView("Data_Without_Nulls");
        final Dataset<Row> DataWithoutNull = sparkSession
                .sql("SELECT DISTINCT * FROM Data_Without_Nulls WHERE NOT YearsExp = 'null Yrs of Exp' ");
        return convert_tojason(DataWithoutNull);
    }

}