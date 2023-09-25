package com.advandata.streaming.utils

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

object StreamsConfigLoader {

  final val SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url"
  def load(config: Config): Properties = {
    val propMap = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> config.getString("app.name"),
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> config.getString("kafka.bootstrap-servers.url"),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> config.getString("kafka.offset-reset"),
      SCHEMA_REGISTRY_URL_CONFIG -> config.getString("kafka.schema-registry.url")
    )
    val props = new Properties()
    propMap.foreach({ case (k, v) => props.put(k, v)})
    props
  }
}
