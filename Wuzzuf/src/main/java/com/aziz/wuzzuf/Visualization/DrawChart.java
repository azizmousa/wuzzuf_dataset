package com.aziz.wuzzuf.Visualization;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import org.knowm.xchart.style.markers.SeriesMarkers;
import java.awt.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DrawChart {
//src/main/resources/Wuzzuf_Jobs.csv file path
    public static String drawPieChart(){

        PieChart chart = new PieChartBuilder().width(800).height(600).title("jobs for each company").build();
        // Series
        for(int i=1 ; i < 10 ; i++) {
            chart.addSeries("i : "+String.format("%d", i), i*i);
        }
        try {
            BitmapEncoder.saveBitmap(chart, "./Sample_PieChart",BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            System.out.println("Can't create image"); // Or something more intellegent
        }
        return "./Sample_PieChart";
    }

}
