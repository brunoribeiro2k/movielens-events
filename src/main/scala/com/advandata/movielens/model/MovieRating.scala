package com.advandata.movielens.model

import java.time.Instant

case class MovieRating(movieId: Int, rating: Float, timestamp: Instant)
