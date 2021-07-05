package com.aziz.wuzzuf;

import com.aziz.wuzzuf.Visualization.DrawChart;
import com.aziz.wuzzuf.info.DataInfo;
import com.aziz.wuzzuf.model.MainModel;
import com.aziz.wuzzuf.preprocess.DataCleaner;
import com.aziz.wuzzuf.preprocess.FileLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tech.tablesaw.api.Table;

import java.util.Iterator;

@RestController
public class MainController {

    @RequestMapping("/")
    public String index() {
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Welcome To Wuzzuf Dataset Api:</h1>");
        builder.append("<br><a href=http://localhost:8080/load-csv?" +
                "path=src/main/resources/Wuzzuf_Jobs.csv&header=true>Load Dataset</a> ==> " +
                "<b>http://localhost:8080/load-csv?" +
                "path=[dataset-path]&header=[is_header?]</b>");
        builder.append("<br><a href=http://localhost:8080/summary>Print Summary</a> ==> " +
                "<b>http://localhost:8080/summary</b>");
        builder.append("<br><a href=http://localhost:8080/count-null>Count Nulls</a> ==> " +
                "<b>http://localhost:8080/count-null</b>");
        builder.append("<br><a href=http://localhost:8080/drop-null>Remove Null Rows</a> ==> " +
                "<b>http://localhost:8080/drop-null</b>");
        builder.append("<br><a href=http://localhost:8080/remove-dup>Remove Duplicate Rows</a> ==> " +
                "<b>http://localhost:8080/remove-dup</b>");

        builder.append("<br><a href=http://localhost:8080/print-df>Print All Dataset</a> ==> " +
                "<b>http://localhost:8080/print-df</b>");
        builder.append("<br><a href=http://localhost:8080/most-demanded-jobs-pie>Draw Most Demanded Companies</a> ==> " +
                "<b>http://localhost:8080/most-demanded-jobs-pie</b>");
        builder.append("<br><a href=http://localhost:8080/most-jobs-title-bar>Draw Most Jobs Titles</a> ==> " +
                "<b>http://localhost:8080/most-jobs-title-bar</b>");

        builder.append("<br><a href=http://localhost:8080/most-pop-areas-bar>Draw Most Popular Areas</a> ==> " +
                "<b>http://localhost:8080/most-pop-areas-bar</b>");

        builder.append("<br><a href=http://localhost:8080/most-pop-skills-bar>Draw Most Popular Skills</a> ==> " +
                "<b>http://localhost:8080/most-pop-skills-bar</b>");

        builder.append("<br><a href=http://localhost:8080/convert-years-exp>Factorize YearsExp</a> ==> " +
                "<b>http://localhost:8080/convert-years-exp</b>");

        builder.append("<br><a href=http://localhost:8080/pip-line?" +
                "path=src/main/resources/Wuzzuf_Jobs.csv&header=true>Run pip line</a> ==> " +
                "<b>http://localhost:8080/pip-line?path=[dataset-path]&header=[is_header?]</b>");
        return builder.toString();
    }

    @RequestMapping("/load-csv")
    public  String load_csv(@RequestParam(value = "path", required = true) String path,
                            @RequestParam(value = "header", required = true) String header)  {
        Dataset<Row> df = FileLoader.load_csv(MainModel.getSession(), path, Boolean.parseBoolean(header));
        MainModel.setMainDataframe(df);
        try {
            Table table = FileLoader.load_csv_table_saw(path);
//            System.out.println(table.print());
            MainModel.setMainTable(table);
        }catch (Exception exception){
            exception.printStackTrace();
        }

        return "<h1>CSV Loaded:)</h1>";

    }

    @RequestMapping("/print-df")
    public String printDataframe(){
        StringBuilder jobs = new StringBuilder();
        jobs.append("<h1>Dataset</h1>");
        jobs.append("<table style=\"width:100%\">");
        jobs.append(" <tr>")
                .append("<th>#</th>")
                .append("<th>Title</th>")
                .append("<th>Company</th>")
                .append("<th>Location</th>")
                .append("<th>Type</th>")
                .append("<th>Level</th>")
                .append("<th>YearsEXP</th>")
                .append("<th>Country</th>")
                .append("<th>Skills</th>")
                .append("</tr>");
        int i = 1;
        for (Iterator<Row> it = MainModel.getMainDataframe().toLocalIterator(); it.hasNext(); ) {
            Row row = it.next();
            jobs.append(" <tr>")
                    .append("<th>").append(i).append("</th>")
                    .append("<th>").append(row.getString(0)).append("</th>")
                    .append("<th>").append(row.getString(1)).append("</th>")
                    .append("<th>").append(row.getString(2)).append("</th>")
                    .append("<th>").append(row.getString(3)).append("</th>")
                    .append("<th>").append(row.getString(4)).append("</th>")
                    .append("<th>").append(row.getString(5)).append("</th>")
                    .append("<th>").append(row.getString(6)).append("</th>")
                    .append("<th>").append(row.getString(7)).append("</th>")
                    .append("</tr>");
            i++;
        }
        jobs.append("</table>");
        return jobs.toString();
    }

    @RequestMapping("/summary")
    public String displaySummary(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Summary</h1>");
        for(Iterator<String> it = MainModel.getMainDataframe().summary().toJSON().toLocalIterator(); it.hasNext();){
            String js = it.next();
            System.out.println(js);
            builder.append("<br>").append(js).append("</br>");
        }
        return builder.toString();
    }


    @RequestMapping("/most-demanded-jobs-pie")
    public String mostDemandJobsPie(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Most Demanded Jobs Pie Chart</h1>");
        String imageByte = DrawChart.drawPieChart(DataInfo.getMostDemandCompany(MainModel.getSession(),
                MainModel.getMainDataframe()), "Most Demanded Jobs");
        builder.append("<img src=\"data:image/jpg;base64, ").append(imageByte).append("\">");
        return builder.toString();
    }

    @RequestMapping("/most-jobs-title-bar")
    public String mostPopularJobsTitlesBar(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Most Popular Job Titles Bar Chart</h1>");
        String imageByte = DrawChart.drawBarChart(DataInfo.getMostPopularJobTitle(MainModel.getSession(),
                MainModel.getMainDataframe()), "Most Popular Jobs titles");
        builder.append("<img src=\"data:image/jpg;base64, ").append(imageByte).append("\">");
        return builder.toString();

    }

    @RequestMapping("/most-pop-areas-bar")
    public String mostPopularAreasBar(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Most Popular Areas Bar Chart</h1>");
        String imageByte = DrawChart.drawBarChart(DataInfo.getMostPopularAreas(MainModel.getSession(),
                MainModel.getMainDataframe()), "Most Popular Areas");
        builder.append("<img src=\"data:image/jpg;base64, ").append(imageByte).append("\">");
        return builder.toString();

    }

    @RequestMapping("/most-pop-skills-bar")
    public String mostPopularSkillsBar(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Most Popular Skills Bar Chart</h1>");
        String imageByte = DrawChart.drawBarChart(DataInfo.getMostPopularSills(MainModel.getSession(),
                MainModel.getMainDataframe()), "Most Popular Skills");
        builder.append("<img src=\"data:image/jpg;base64, ").append(imageByte).append("\">");
        return builder.toString();

    }

    @RequestMapping("count-null")
    public String countNull(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Count Of Nulls</h1>")
                .append(DataCleaner.getNullCount(MainModel.getSession(), MainModel.getMainDataframe()));
        return builder.toString();
    }

    @RequestMapping("drop-null")
    public String dropNulls(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Drop Nulls</h1>");
        MainModel.setMainDataframe(DataCleaner.dropNulls(MainModel.getSession(), MainModel.getMainDataframe()));
        builder.append(countNull());
        return builder.toString();
    }

    @RequestMapping("remove-dup")
    public String removeDuplicates(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Removing Duplicates</h1>");
        MainModel.setMainDataframe(DataCleaner.removeDuplicates(MainModel.getSession(), MainModel.getMainDataframe()));
        builder.append(displaySummary());
        return builder.toString();
    }
    @RequestMapping("pip-line")
    public String pipLine(@RequestParam(value = "path", required = true) String path,
                          @RequestParam(value = "header", required = true) String header){
        StringBuilder builder = new StringBuilder();
        builder.append(load_csv(path, header))
        .append(displaySummary())
        .append(countNull())
        .append(dropNulls())
        .append(removeDuplicates())
        .append(mostDemandJobsPie())
        .append(mostPopularJobsTitlesBar())
        .append(mostPopularAreasBar())
        .append(mostPopularSkillsBar())
        .append(convertYearsExp());
        return builder.toString();
    }

    @RequestMapping("convert-years-exp")
    public String convertYearsExp(){
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Convert YearsExp Column</h1>");
        Table table = DataInfo.convertExp(MainModel.getMainTable());
        builder.append("<h1>Dataset</h1>");
        builder.append("<table style=\"width:100%\">");
        builder.append(" <tr>")
                .append("<th>#</th>")
                .append("<th>Title</th>")
                .append("<th>Company</th>")
                .append("<th>Location</th>")
                .append("<th>Type</th>")
                .append("<th>Level</th>")
                .append("<th>YearsEXP</th>")
                .append("<th>Country</th>")
                .append("<th>Skills</th>")
                .append("<th>MinExp</th>")
                .append("<th>MaxExp</th>")
                .append("</tr>");
        int i = 1;
        for (Iterator<tech.tablesaw.api.Row> it = table.stream().iterator(); it.hasNext(); ) {
            tech.tablesaw.api.Row row = it.next();
            builder.append(" <tr>")
                    .append("<th>").append(i).append("</th>")
                    .append("<th>").append(row.getString(0)).append("</th>")
                    .append("<th>").append(row.getString(1)).append("</th>")
                    .append("<th>").append(row.getString(2)).append("</th>")
                    .append("<th>").append(row.getString(3)).append("</th>")
                    .append("<th>").append(row.getString(4)).append("</th>")
                    .append("<th>").append(row.getString(5)).append("</th>")
                    .append("<th>").append(row.getString(6)).append("</th>")
                    .append("<th>").append(row.getString(7)).append("</th>")
                    .append("<th>").append(row.getString(8)).append("</th>")
                    .append("<th>").append(row.getString(9)).append("</th>")
                    .append("</tr>");
            if(i == 10)
                break;
            i++;
        }
        builder.append("</table>");
        return builder.toString();
    }


}
