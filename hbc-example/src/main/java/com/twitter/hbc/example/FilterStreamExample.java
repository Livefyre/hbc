/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.twitter.hbc.example;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FilterStreamExample {

  private static final Logger log = LoggerFactory.getLogger(FilterStreamExample.class);

  private static volatile boolean connected = false;
  private static volatile boolean stopped = false;

  public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    // add some track terms
//    endpoint.trackTerms(Lists.newArrayList("twitterapi", "#yolo"));
    endpoint.trackTerms(Lists.newArrayList("uiweyuiryweuiryweuiryiuweyriu"));
//    endpoint.trackTerms(Lists.newArrayList("lol"));

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    // Authentication auth = new BasicAuth(username, password);

    // Create a new BasicClient. By default gzip is enabled.
    Client client = new ClientBuilder()
            .name("hosebird.fyre.co")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();
    while (client.getStatsTracker().getNumConnects() == 0) {
      Thread.sleep(250);
    }
    log.debug("Connected!");
    Thread.sleep(2_500);
    connected = true;

    // Do whatever needs to be done with messages
    while (!FilterStreamExample.stopped) {
      String msg = queue.poll(250, TimeUnit.MILLISECONDS);
      if (msg != null) log.debug(msg);
    }

    log.debug("Stopping!");
    client.stop();
    log.debug("Stopped!");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    String accessToken = "44814480-TC8mxI543KL1wVhZc4bOB1hVFr6zkLS9F5qwGguoV";
    String accessTokenSecret = "hgWWuGoKx0ekIHpUPKRJKOCJJL38CwkyRsU9XmOnHcq4R";
    String apiKey = "66wjQMCyEaMrfCDycjIry8Tlh";
    String apiSecret = "Ie4tNWMttvrwgFeCZKmksZpazfxUOgKraLyZlb1RwDYvDqh9HP";

    final ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(() -> {
      try {
        FilterStreamExample.run(apiKey, apiSecret, accessToken, accessTokenSecret);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

//    log.debug("Press enter to initiate connection shutdown");
    while(!connected) {
      Thread.sleep(1_000);
    }
    log.debug("Set stopping signal");
    stopped = true;
    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    log.debug("Scheduler stopped, waiting at least 5s for hbc to timeout, and assert the hosebird IO Thread is gone!");
    Thread.sleep(7_000);
    final Optional<Thread> hosebird = Thread.getAllStackTraces().keySet().stream().filter((k) -> k.getName().contains("hosebird")).findFirst();
    log.warn("Hosebird Thread: {}, if it's there ohhh noes boeg!!", hosebird.map(Thread::getName).orElse("NOBIRD"));
    if (hosebird.isPresent()) {
      Stream.of(hosebird.get().getStackTrace()).forEach((e) -> log.debug("{}.{} ({}:{})", e.getClassName(), e.getMethodName(), e.getFileName(), e.getLineNumber()));
    }
    assert !hosebird.isPresent();
  }
}
