package com.mplatform.manalytics.mstore.datamatcher;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import com.mplatform.manalytics.mstore.datamatcher.entities.KafkaResponse;

@SpringBootApplication
public class Application {
  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }
  
  @KafkaListener(topics = "${kafka.topic}")
  public void listen(KafkaResponse message) {
      System.out.println("Received Messasge in group foo: " + message.getDataCategory());
  }
  
}
