package com.aziz.wuzzuf.Visualization;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class DrawChart {

    public static String drawPieChart(Dataset<Row> dataset){
        PieChart chart = new PieChartBuilder().width(800).height(600).title("jobs for each company").build();
        for (Iterator<Row> it = dataset.toLocalIterator(); it.hasNext(); ) {
            Row row = it.next();
            chart.addSeries(row.getString(0), Integer.parseInt(row.getString(1)));
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
        chart.getStyler().setHasAnnotations(true);

        // Series
        //todo real data
        for (Iterator<Row> it = dataset.toLocalIterator(); it.hasNext(); ) {
            Row row = it.next();
            chart.addSeries(row.getString(0),Arrays.asList(new Integer[] { 0, 1, 2, 3, 4 }), Arrays.asList(new Integer[] { 4, 5, 9, 6, 5 }));

        }

        String imageNameAndPath = "/images/areas_BarChart.png";
        try {
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace(); // Or something more intellegent
        }
        return imageNameAndPath;
    }

}
