package com.kafatest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Client {
  private final Logger logger = LoggerFactory.getLogger(CreateTopicTest.class.getName());
  String consumerPropertiesConfigFileName = "consumer_qa.properties";
  private Properties consumerProperties = loadConfig(consumerPropertiesConfigFileName);

  public Properties loadConfig(String filePath) {
    Properties config = new Properties();
    InputStream input = CreateTopicTest.class.getClassLoader().getResourceAsStream(filePath);

    if (input != null) {
      try {
        config.load(input);
        input.close();

      } catch (IOException e) {
        logger.error(ExceptionUtils.getStackTrace(e));
      }
    } else
      logger.error("Unable to find config file {}", filePath);

    return config;
  }

  public Boolean createTopic(final String topicName, final int partitions) {
    short replicationFactor = 3;
    try (final AdminClient adminClient = KafkaAdminClient.create(consumerProperties)) {
      try {
        final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        final CreateTopicsResult createTopicsResult =
          adminClient.createTopics(Collections.singleton(newTopic));

        // Since the call is Async, Lets wait for it to complete.
        createTopicsResult.values().get(topicName).get();
      } catch (InterruptedException | ExecutionException | UnknownServerException e) {
          logger.error("Error occurred in creating topic: {} .Exception:  {}", topicName,
            ExceptionUtils.getStackTrace(e));
        return false;
      }
      adminClient.close();
      return true;
    }
  }

  public Boolean deleteTopic(String topicName) {
    try (final AdminClient adminClient = KafkaAdminClient.create(consumerProperties)) {
      try {
        final DeleteTopicsResult deleteTopicsResult =
          adminClient.deleteTopics(Collections.singleton(topicName));

        // Since the call is Async, Lets wait for it to complete.
        deleteTopicsResult.values().get(topicName).get();
      } catch (Exception e) {
        logger.error("Error occurred in deleting topic: {} .Exception:  {}", topicName,
          ExceptionUtils.getStackTrace(e));
        return false;
      }
      adminClient.close();
      return true;
    }
  }


}

/*
The same code works on local kafka (kafka_2.11-2.0.0)
Moreover if we reduce the no of threads to 3 , probability of occurrence of exception decreases.
 */

public class CreateTopicTest {
  public static void main(String[] args) throws InterruptedException {
    int count = 15;
    int[] arr = {0};
    final Logger logger = LoggerFactory.getLogger(CreateTopicTest.class.getName());
    Client c = new Client();

    Thread[] threads = new Thread[count];
    for (int i = 0; i < count; i++) {
      String topicName = "local-test-topic" + i;
      threads[i] = new Thread() {
        public void run(){
          final int id = arr[0]++;
          final Logger logger = LoggerFactory.getLogger("Thread" + id);
          setName("ClientThread" + id);
          if (c.createTopic(topicName, 4)) {
            logger.info("Create topic success for " + topicName);
            if (c.deleteTopic(topicName)) {
              logger.info("Delete topic success for " + topicName);
            } else {
              logger.error("Delete topic failed for " + topicName);
            }
          } else {
            logger.error("Create topic failed for " + topicName);
          }
          logger.info("\n\n");
        }
      };
      threads[i].start();
    }
    for(Thread t : threads){
      t.join();
    }
  }

}
