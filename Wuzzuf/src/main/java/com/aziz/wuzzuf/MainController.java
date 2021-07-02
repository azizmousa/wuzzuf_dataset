package com.aziz.wuzzuf;
import org.apache.commons.io.FileUtils;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import com.aziz.wuzzuf.model.MainModel;
import com.aziz.wuzzuf.Visualization.DrawChart;
import com.aziz.wuzzuf.preprocess.FileLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class MainController {

    @RequestMapping("/say-hello")
    public  String sayHello()  {
        SparkSession session = SparkSession.builder()
                .appName("Wuzzuf")
                .master("local[3]")
                .getOrCreate();

        Dataset<Row> df = session.read()
                .option("header",true)
                .csv("src/main/resources/Wuzzuf_Jobs.csv");
        df.printSchema();
        return "Hello there!";

    }
    @RequestMapping("/pieChart")
    public String displayPieChart(){

        String filePath = DrawChart.drawPieChart();
       System.out.println(filePath);
       return filePath;
    }
}
