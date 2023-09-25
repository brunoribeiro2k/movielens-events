package com.advandata.streaming.model

import java.time.Instant

trait Record[K, V] {
  def topic: String
  def key(value: V): K
  def instant(value: V): Instant
}