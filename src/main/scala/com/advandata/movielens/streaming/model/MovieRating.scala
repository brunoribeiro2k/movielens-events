package com.advandata.movielens.streaming.model

case class MovieRating(movieId: Int, rating: Float, timestamp: Long)

object MovieRating {
  def empty: MovieRating = MovieRating(movieId = 0, rating = 0, timestamp = 0)
}
