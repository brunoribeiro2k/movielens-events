package com.advandata.movielens.streaming

import com.advandata.movielens.model.{UserRating, UserRatingHistory}
import com.advandata.streaming.utils.{Logger, StreamsConfigLoader}
import com.moleike.kafka.streams.avro.generic.Serdes.{Config => SRConfig, _}
import com.typesafe.config
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._

import java.time.Duration
import java.util.Properties

object UserRatingsProcessor extends Logger {


  def main(args: Array[String]): Unit = {

    logger.info("Starting streams app...")

    // Load configuration
    val appConfig: config.Config = config.ConfigFactory.load
    val streamsConfig: Properties = StreamsConfigLoader.load(appConfig)
    implicit val srConfig: SRConfig =
      Map(StreamsConfigLoader.SCHEMA_REGISTRY_URL_CONFIG -> appConfig.getString("kafka.schema-registry.url"))

    // Set window
    val windowSizeMinutes = appConfig.getInt("kafka.window.size.minutes")
    val windowSlideMinutes = appConfig.getInt("kafka.window.sliding.minutes")
    val windowSize = Duration.ofMinutes(windowSizeMinutes)
    val windowSlide = Duration.ofMinutes(windowSlideMinutes)
    val window = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSlide)

    // Set stream builder
    val streamsBuilder = new StreamsBuilder

    // Set base streams with necessary repartitioning
    implicit val stringSerde: StringSerde = new StringSerde
    val ratingStream: KStream[String, UserRating] = streamsBuilder
      .stream[String, UserRating]("user-ratings-4")
      .map { case (_, v) => (v.userId.toString, v) }

    // ratingStream.foreach((k, v) => logger.debug(s"ratingStream [$k] -> $v"))

    val windowedRatingStream: TimeWindowedKStream[String, UserRating] = ratingStream
      .groupByKey
      .windowedBy(window)

    val historyTable = windowedRatingStream
      .aggregate(UserRatingHistory.empty) { case (_, newRating, userRatingHistory) =>
        userRatingHistory.addRating(newRating)
      }

    val historyStream = historyTable
      .toStream
      .map { case (k, v) => (k.key, v) }

    historyStream.to("user-ratings-history-4")

    // Build and start streams
    val streamsTopology = streamsBuilder.build
    val streams = new KafkaStreams(streamsTopology, streamsConfig)
    Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))
    streams.start()

  }

}