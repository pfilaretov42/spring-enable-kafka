package dev.pfilaretov42.spring.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SpringEnableKafkaApplication

fun main(args: Array<String>) {
    runApplication<SpringEnableKafkaApplication>(*args)
}
