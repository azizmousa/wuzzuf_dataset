package com.aziz.wuzzuf.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainModel {

    private static SparkSession mainSession;
    private static Dataset<Row> mainDataframe;

    public static void initializeSession(String appName, String master){
        mainSession = SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
    }

    public static SparkSession getSession(){
        return mainSession;
    }

    public static void stopSession(){
        if (mainSession != null)
            mainSession.stop();
    }

    public static void setMainDataframe(Dataset<Row> dataframe){
        mainDataframe = dataframe;
    }

    public static Dataset<Row> getMainDataframe(){
        return mainDataframe;
    }
}
