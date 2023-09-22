package com.advandata.movielens.streaming.model
import java.time.Instant

case class UserRating(userId: Int, movieId: Int, rating: Float, timestamp: Long) {
  val key: User = UserRating.record.key(this)
  val instant: Instant = UserRating.record.instant(this)
}

object UserRating {
  implicit val record: Record[User, UserRating]= new Record[User, UserRating] {
    def topic: String = "user-ratings"
    def key(value: UserRating): User = User(value.userId)
    def instant(value: UserRating): Instant = Instant.ofEpochMilli(value.timestamp)
  }
}
