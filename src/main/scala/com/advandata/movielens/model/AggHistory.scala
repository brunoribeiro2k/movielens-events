package com.advandata.movielens.model

case class AggHistory(userId: Int,
                      processingCount: Int,
                      ratingsCount: Int,
                      avg: Float,
                      top: Option[MovieRating],
                      last: Option[MovieRating],
                      movieRatings: Set[MovieRating])

object AggHistory {
  def empty: AggHistory = AggHistory(
    processingCount = 0,
    userId = 0,
    movieRatings = Set.empty[MovieRating],
    ratingsCount = 0,
    avg = 0,
    top = None,
    last = None)
}
