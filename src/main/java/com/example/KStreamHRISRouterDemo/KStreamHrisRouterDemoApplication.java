package com.example.KStreamHRISRouterDemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@EnableKafkaStreams
@SpringBootApplication
public class KStreamHrisRouterDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KStreamHrisRouterDemoApplication.class, args);
	}

	@Bean
	public KafkaStreams kafkaStreams(ObjectMapper mapper, StreamsBuilder builder,
									 @Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
											 ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider) {

		String[] kronosEvents = new String[]{"absenceEvent", "employee", "employeeSchedule",
				"schedule", "labourLevel", "holiday", "holSched", "region", "division", "dept",
				"busUnit", "workArea", "subAccount"};
		String[] datacomEvents = new String[]{"absenceEvent", "employeeBankDist", "job",
				"payslips.get", "leaveBalances.get"};
		String[] enboarderEvents = new String[]{"job.hire", "job.update"};
		String[] bwiseEvents = new String[]{"empLngLve", "deptEvent"};

		KeyValueMapper kvMapper = (key, value) -> {
			try {
				return new KeyValue<>(key, mapper.writeValueAsString(value));
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		};

		KStream<String, String> sourceStream =
				builder.stream("test.hris.eventrouter",
						Consumed.with(Serdes.String(), Serdes.String()));

		KStream<String, Map<String,Object>> events = sourceStream.flatMapValues((key, value) ->
				(List<Map<String,Object>>)JsonPath.parse(value).read("$.*", List.class)
		);

		events.filter((key, message) -> isMessageInEvents(message, kronosEvents)).map(kvMapper).
				to("test.hris.kronos", Produced.with(Serdes.String(), Serdes.String()));
		events.filter((key, message) -> isMessageInEvents(message, datacomEvents)).map(kvMapper)
				.to("test.hris.datacom", Produced.with(Serdes.String(), Serdes.String()));
		events.filter((key, message) -> isMessageInEvents(message, enboarderEvents)).map(kvMapper)
				.to("test.hris.enboarder", Produced.with(Serdes.String(), Serdes.String()));
		events.filter((key, message) -> isMessageInEvents(message, bwiseEvents)).map(kvMapper)
				.to("test.hris.bwise", Produced.with(Serdes.String(), Serdes.String()));

		//just for demo sake, we can listen to sinks
		builder.stream("test.hris.kronos", Consumed.with(Serdes.String(), Serdes.String()))
				.peek((key, value) -> log.info("test.hris.kronos - " + value));
		builder.stream("test.hris.datacom", Consumed.with(Serdes.String(), Serdes.String()))
				.peek((key, value) -> log.info("test.hris.datacom - " + value));
		builder.stream("test.hris.enboarder", Consumed.with(Serdes.String(), Serdes.String()))
				.peek((key, value) -> log.info("test.hris.enboarder - " + value));
		builder.stream("test.hris.bwise", Consumed.with(Serdes.String(), Serdes.String()))
				.peek((key, value) -> log.info("test.hris.bwise - " + value));


		KafkaStreamsConfiguration streamsConfig = streamsConfigProvider.getIfAvailable();
		Topology topology = builder.build(streamsConfig.asProperties());
		KafkaStreams streams = new KafkaStreams(topology, streamsConfig.asProperties());

		streams.start();
		return streams;
	}

	private static boolean isMessageInEvents(Map<String, Object> message, String[] events) {
		String event = message.get("event").toString();
		boolean bool = Arrays.stream(events).anyMatch(ev -> ev.contentEquals(event));
		return bool;
	}
}
