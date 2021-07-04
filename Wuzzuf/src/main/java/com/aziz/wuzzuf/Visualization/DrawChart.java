package com.aziz.wuzzuf.Visualization;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DrawChart {

    public static String drawPieChart(Dataset<Row> dataset){
        PieChart chart = new PieChartBuilder().width(800).height(600).title("jobs for each company").build();
        List<Row> rows = dataset.select(dataset.columns()[0],dataset.columns()[1]).limit(10).collectAsList();
        for (Row row : rows){
           chart.addSeries(row.getString(0), row.getLong(1));
        }
        String imageNameAndPath = "/images/jobs_PieChart.png";
        try {
            BitmapEncoder.saveBitmap(chart, "./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
            System.out.println("./src/main/resources/static"+imageNameAndPath);
        } catch (IOException e) {
            e.printStackTrace(); // Or something more intellegent
        }
        return imageNameAndPath;
    }

    public static String drawBarChart(Dataset<Row> dataset){

        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Popular Areas Histogram").xAxisTitle("Popular Areas").yAxisTitle("Count").build();
        // Customize Chart
        //chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
        List<Row> rows = dataset.select(dataset.columns()[0],dataset.columns()[1]).limit(10).collectAsList();
        List<String> keys = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        for (Row row : rows){
            keys.add(row.getString(0));
            values.add(row.getLong(1));
        }
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries("Areas",keys,values);
        String imageNameAndPath = "/images/areas_BarChart.png";
        try {
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace(); // Or something more intellegent
        }
        return imageNameAndPath;
    }

}
