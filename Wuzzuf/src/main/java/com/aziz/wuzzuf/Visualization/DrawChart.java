package com.aziz.wuzzuf.Visualization;

import org.knowm.xchart.*;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

import org.knowm.xchart.style.markers.SeriesMarkers;
import java.io.IOException;
import java.util.*;
import java.lang.Integer;
public class DrawChart {

    public static String drawPieChart(){
        PieChart chart = new PieChartBuilder().width(800).height(600).title("jobs for each company").build();
        //Todo iterate over real data
        for(int i=1 ; i < 10 ; i++) {
            chart.addSeries("i : "+String.format("%d", i), i*i);
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

    public static String drawBarChart(){

        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Popular Areas Histogram").xAxisTitle("Popular Areas").yAxisTitle("Count").build();
        // Customize Chart
        //chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        // Series
        //todo real data
        chart.addSeries("test 1", Arrays.asList(new Integer[] { 0, 1, 2, 3, 4 }), Arrays.asList(new Integer[] { 4, 5, 9, 6, 5 }));

        String imageNameAndPath = "/images/areas_BarChart.png";
        try {
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace(); // Or something more intellegent
        }
        return imageNameAndPath;
    }

}
