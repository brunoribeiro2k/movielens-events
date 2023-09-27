package com.advandata.movielens.model

import java.time.Instant

case class MovieRating(movieId: Int, rating: Float, timestamp: Instant)

object MovieRating {
  def apply(ur: UserRating): MovieRating =
    MovieRating(
      movieId = ur.movieId,
      rating = ur.rating,
      timestamp = ur.timestamp
    )
}
