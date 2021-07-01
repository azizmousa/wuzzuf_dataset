package com.aziz.wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

}
