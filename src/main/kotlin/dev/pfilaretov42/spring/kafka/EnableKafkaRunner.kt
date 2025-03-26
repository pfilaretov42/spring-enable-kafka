package dev.pfilaretov42.spring.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.stereotype.Component


@Configuration
class KafkaProducerConfig {

    @Value(value = "\${spring.kafka.bootstrap-servers}")
    private val bootstrapServers: String? = null

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val configProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        )
        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<String, String>) =
        KafkaTemplate(producerFactory)
}

//@EnableKafka
@Configuration
class KafkaConsumerConfig {

    @Value(value = "\${spring.kafka.bootstrap-servers}")
    private val bootstrapServers: String? = null

    @Value(value = "\${spring.kafka.consumer.group-id}")
    private val groupId: String? = null

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        )
        return DefaultKafkaConsumerFactory(props)
    }

//    @Bean
//    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
//        val factory =
//            ConcurrentKafkaListenerContainerFactory<String, String>()
//        factory.consumerFactory = consumerFactory()
//        return factory
//    }
}

private val logger = KotlinLogging.logger {}

@Component
class EnableKafkaRunner(
    private val kafkaTemplate: KafkaTemplate<String, String>,
) : CommandLineRunner {
    override fun run(vararg args: String?) {
        logger.info { "Sending message to Kafka..." }
        kafkaTemplate.send("my-topic", "Hello from Kafka Runner").get()
        logger.info { "Message sent" }
    }
}

@Component
class MyKafkaListener {
    @KafkaListener(topics = ["my-topic"])
    fun consume(message: String) {
        logger.info { "Message received: $message" }
    }
}