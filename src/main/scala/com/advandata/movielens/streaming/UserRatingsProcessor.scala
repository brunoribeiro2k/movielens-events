package com.advandata.movielens.streaming

import model._
import com.advandata.movielens.streaming.utils.StreamsConfigLoader
import com.advandata.movielens.streaming.utils.Logger
import com.moleike.kafka.streams.avro.generic.Serdes._
import org.apache.kafka.streams.kstream.{SessionWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import com.typesafe.config
import kafka.server.RequestLocal.NoCaching.close
import org.apache.kafka.streams.scala.serialization.Serdes.intSerde

import java.util.Properties
import java.time.Duration

object UserRatingsProcessor extends Logger {

  def main(args: Array[String]): Unit = {

    logger.info("Starting streams app...")

    // Load configuration
    val appConfig: config.Config = config.ConfigFactory.load
    val streamsConfig: Properties = StreamsConfigLoader.load(appConfig)
    implicit val conf: com.moleike.kafka.streams.avro.generic.Serdes.Config =
      Map(StreamsConfigLoader.SCHEMA_REGISTRY_URL_CONFIG -> appConfig.getString("kafka.schema-registry.url"))

    // Set window
    val propSessionInactivityMinutes = appConfig.getInt("app.session-inactivity.minutes")
    val sessionWindowInactivityGap = Duration.ofMinutes(propSessionInactivityMinutes)
    val sessionWindow = SessionWindows.ofInactivityGapWithNoGrace(sessionWindowInactivityGap)

    // Set stream builder
    val streamsBuilder = new StreamsBuilder

    // Set base streams with necessary repartitioning
    val ratingStream: KStream[User, UserRating] = streamsBuilder
      .stream[User, UserRating](UserRating.record.topic)

    ratingStream.foreach((k, v) => logger.info(s"ratingStream [$k] -> $v"))

    // Process stream

    val historyStream: KTable[User, UserRatingHistory] = ratingStream
      .mapValues((r: UserRating) => UserRatingHistory.addToHistory(r, UserRatingHistory.empty))
      .groupByKey
      .reduce(UserRatingHistory.reduce)

    historyStream.toStream.to(UserRatingHistory.record.topic)

    // Build and start streams
    val streamsTopology = streamsBuilder.build
    val streams = new KafkaStreams(streamsTopology, streamsConfig)
    Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))
    streams.start()

  }

}