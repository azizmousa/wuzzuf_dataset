package com.aziz.wuzzuf;

import com.aziz.wuzzuf.model.MainModel;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WuzzufApplication {

    public static void main(String[] args) {
        MainModel.initializeSession("wuzzuf", "local[3]");
        SpringApplication.run(WuzzufApplication.class, args);
    }

}
