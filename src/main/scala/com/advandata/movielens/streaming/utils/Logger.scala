package com.advandata.movielens.streaming.utils

trait Logger {

  @transient
  lazy val logger: org.apache.log4j.Logger = org.apache.log4j.Logger.getLogger(getClass.getName)

}