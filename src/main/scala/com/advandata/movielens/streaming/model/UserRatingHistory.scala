package com.advandata.movielens.streaming.model
import java.time.Instant

case class UserRatingHistory(userId: Int,
                             movieRatings: Set[MovieRating],
                             ratingsCount: Int,
                             avg: Float,
                             top: MovieRating,
                             last: MovieRating) {
  val key: User = UserRatingHistory.record.key(this)
  val instant: Instant = UserRatingHistory.record.instant(this)
}

object UserRatingHistory {
  implicit val record: Record[User, UserRatingHistory] = new Record[User, UserRatingHistory] {
    def topic: String = "user-rating-history"
    def key(value: UserRatingHistory): User = User(value.userId)
    def instant(value: UserRatingHistory): Instant = Instant.ofEpochMilli(value.last.timestamp)
  }

  def empty: UserRatingHistory = UserRatingHistory(userId = 0,
    movieRatings = Set.empty[MovieRating],
    ratingsCount = 0,
    avg = 0,
    top = MovieRating.empty,
    last = MovieRating.empty)
  def addToHistory(r: UserRating, h: UserRatingHistory): UserRatingHistory = {
    val newRating = MovieRating(movieId = r.movieId, rating = r.rating, timestamp = r.timestamp)
    val newMovieRatings = h.movieRatings + newRating
    val newRatingsCount = h.ratingsCount + 1
    val newAvg = (h.ratingsCount * h.avg + r.rating)/newRatingsCount
    val newTop = {
      if (newRating.rating > h.top.rating) newRating
      else if (h.top.rating > newRating.rating) h.top
      else if (newRating.timestamp > h.top.timestamp) newRating
      else h.top
    }
    val newLast = if (newRating.timestamp > h.last.timestamp) newRating else h.last
    UserRatingHistory(
      userId = h.userId,
      movieRatings = newMovieRatings,
      ratingsCount = newRatingsCount,
      avg = newAvg,
      top = newTop,
      last = newLast)
  }

  def reduce(h1: UserRatingHistory, h2: UserRatingHistory): UserRatingHistory = {
    require(h1.userId == h2.userId, "UserRatingHistory instances must belong to the same user")

    val mergedMovieRatings = h1.movieRatings ++ h2.movieRatings
    val newRatingsCount = mergedMovieRatings.size
    val newAvg = mergedMovieRatings.map(_.rating).sum / newRatingsCount
    val newTop = mergedMovieRatings.maxBy(_.rating)
    val newLast = mergedMovieRatings.maxBy(_.timestamp)

    UserRatingHistory(
      userId = h1.userId,
      movieRatings = mergedMovieRatings,
      ratingsCount = newRatingsCount,
      avg = newAvg,
      top = newTop,
      last = newLast)
  }

}
