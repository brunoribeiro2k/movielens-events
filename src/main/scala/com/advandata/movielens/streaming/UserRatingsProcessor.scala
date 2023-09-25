package com.advandata.movielens.streaming

import com.advandata.movielens.model.{AggHistory, MovieRating, UserRating}
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

    ratingStream.foreach((k, v) => logger.info(s"ratingStream [$k] -> $v"))

    val historyStream: KTable[String, AggHistory] = ratingStream
      .groupByKey
      .aggregate(AggHistory.empty) { case (_, v, vr) =>
        val newRating = MovieRating(v.movieId, v.rating, v.timestamp)
        val newRatingsSet = vr.movieRatings + newRating
        val nrNonEmpty = newRatingsSet.nonEmpty
        val avgRating = if (nrNonEmpty) newRatingsSet.map(_.rating).sum / newRatingsSet.size else 0
        val topRating = if (nrNonEmpty) Some(newRatingsSet.maxBy(_.rating)) else None
        val lastRating = if (nrNonEmpty) Some(newRatingsSet.maxBy(_.timestamp)) else None
        AggHistory(
          processingCount = vr.processingCount + 1,
          userId = v.userId,
          movieRatings = newRatingsSet,
          ratingsCount = newRatingsSet.size,
          avg = avgRating,
          top = topRating,
          last = lastRating
        )
      }


    historyStream.toStream.to("user-ratings-history-4")

    // Build and start streams
    val streamsTopology = streamsBuilder.build
    val streams = new KafkaStreams(streamsTopology, streamsConfig)
    Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))
    streams.start()

  }

}