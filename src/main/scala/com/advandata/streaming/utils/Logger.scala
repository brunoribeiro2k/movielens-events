package com.advandata.streaming.utils

trait Logger {
  @transient
  lazy val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)
}
