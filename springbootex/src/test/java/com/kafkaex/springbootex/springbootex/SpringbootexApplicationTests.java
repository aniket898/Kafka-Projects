package com.kafkaex.springbootex.springbootex;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import javax.validation.constraints.AssertTrue;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootexApplicationTests {

	@Test
	public void contextLoads() {
	}

	private static String BOOT_TOPIC = "boot.t";

	@Autowired
	private Sender sender;

	@Autowired
	private Receiver receiver;

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, BOOT_TOPIC);

	@Test
	public void testReceive() throws Exception {
		sender.send(BOOT_TOPIC, "Hello Boot!");
		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
		Assert.assertEquals(receiver.getLatch().getCount(),0);
	}
}
