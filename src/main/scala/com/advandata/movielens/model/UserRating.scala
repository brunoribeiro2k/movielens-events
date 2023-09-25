package com.advandata.movielens.model

import java.time.Instant

case class UserRating(userId: Int, movieId: Int, rating: Float, timestamp: Instant)