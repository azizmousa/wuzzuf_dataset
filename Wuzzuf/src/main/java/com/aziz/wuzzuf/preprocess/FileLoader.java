package com.aziz.wuzzuf.preprocess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileLoader {

    public static Dataset<Row> load_csv(SparkSession session, String path, boolean header){
        System.out.println(header);
        return session.read()
                .option("header", header)
                .csv(path);
    }

}
