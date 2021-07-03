package com.aziz.wuzzuf.info;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataInfo {

    public static Dataset<Row> getMostDemandCompany(SparkSession session, Dataset<Row> df){
        df.createOrReplaceTempView("wuzzuf");
        String sql = "select Company, count(Title) as jobs from wuzzuf  group by Company order by jobs";
        return session.sql(sql);
    }

    public static Dataset<Row> getMostPopularJobTitle(SparkSession session, Dataset<Row> df){
        df.createOrReplaceTempView("wuzzuf");
        String sql = "select Title, count(Title) as jobs from wuzzuf  group by Title order by jobs desc";
        return session.sql(sql);
    }

}
