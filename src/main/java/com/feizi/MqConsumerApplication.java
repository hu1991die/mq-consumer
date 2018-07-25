package com.feizi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class MqConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MqConsumerApplication.class, args);
	}
}
