package com.advandata.movielens.model

case class UserRatingHistory(userId: Int,
                             processingCount: Int,
                             ratingsCount: Int,
                             secondsBetweenRatings: Option[Long],
                             avg: Option[Float],
                             top: Option[MovieRating],
                             last: Option[MovieRating],
                             movieRatings: Set[MovieRating]) {
  def addRating(ur: UserRating): UserRatingHistory = {
    val newRating = MovieRating(ur)
    val newRatingsSet = movieRatings + newRating
    val newProcessingCount = processingCount + 1
    val nrNonEmpty = newRatingsSet.nonEmpty
    val newAvg = if (nrNonEmpty) Some(newRatingsSet.map(_.rating).sum / newRatingsSet.size) else None
    val newTop = if (nrNonEmpty) Some(newRatingsSet.maxBy(_.rating)) else None
    val newLast = if (nrNonEmpty) Some(newRatingsSet.maxBy(_.timestamp)) else None
    val newFirst = if (nrNonEmpty) Some(newRatingsSet.minBy(_.timestamp)) else None
    val newSecondsBetweenRatings = (newFirst, newLast) match {
      case (Some(r1), Some(r2)) => Some(r2.timestamp.getEpochSecond - r1.timestamp.getEpochSecond)
      case _ => None
    }
    UserRatingHistory(
      userId = ur.userId,
      processingCount = newProcessingCount,
      ratingsCount = newRatingsSet.size,
      secondsBetweenRatings = newSecondsBetweenRatings,
      avg = newAvg,
      top = newTop,
      last = newLast,
      movieRatings = newRatingsSet
    )
  }
}

object UserRatingHistory {
  def empty: UserRatingHistory = UserRatingHistory(
    processingCount = 0,
    userId = null.asInstanceOf[Int],
    movieRatings = Set.empty[MovieRating],
    secondsBetweenRatings = None,
    ratingsCount = 0,
    avg = None,
    top = None,
    last = None)

}
