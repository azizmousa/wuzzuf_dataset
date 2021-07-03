package com.aziz.wuzzuf;

import com.aziz.wuzzuf.Visualization.DrawChart;
import com.aziz.wuzzuf.model.MainModel;
import com.aziz.wuzzuf.preprocess.FileLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.awt.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@RestController
public class MainController {

    @RequestMapping("/load-csv")
    public  String load_csv(@RequestParam(value = "path", required = true) String path,
                            @RequestParam(value = "header") String header)  {
        Dataset<Row> df = FileLoader.load_csv(MainModel.getSession(), path, Boolean.parseBoolean(header));
        MainModel.setMainDataframe(df);
        return "file loaded";

    }

    @RequestMapping("/print-df")
    public String printDataframe(){
        StringBuilder jobs = new StringBuilder();
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
        for(Iterator<String> it = MainModel.getMainDataframe().summary().toJSON().toLocalIterator(); it.hasNext();){
            String js = it.next();
            System.out.println(js);
            builder.append("<br>").append(js).append("</br>");
        }
        return builder.toString();
    }
    @RequestMapping("/pieChart")
    public StringBuilder  displayPieChart(){
       // ModelAndView modelAndView = new ModelAndView();
        //TODO please make a drawPieChart with parameters
//        modelAndView.setViewName("pie_chart");
//        modelAndView.addObject("pieChartPath",filePath);
        //return modelAndView;
        Dataset<Row> dataset = new Dataset<Row>();
        String filePath = DrawChart.drawPieChart(dataset);
        System.out.println(filePath);
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Jobs Pie Chart</h1>");
        builder.append("<img src='").append(filePath).append("'/>");
        return builder;

    }

    @RequestMapping("/barChart")
    public StringBuilder  displayBarChart(){
      //  ModelAndView modelAndView = new ModelAndView();
        //TODO please make a drawBarChart with parameters
        Dataset<Row> dataset = new Dataset<Row>();
        String filePath = DrawChart.drawBarChart(dataset);
        System.out.println(filePath);
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>Areas Bar Chart</h1>");
        builder.append("<img src='").append(filePath).append("'/>");
        return builder;
//        modelAndView.setViewName("bar_chart");
//        modelAndView.addObject("barChartPath",filePath);
//        return modelAndView;
    }
}
