/*
 * Package object for the protobuf backport.  Defines a lock object used
 * by Scala reflection in a manner consistent with Spark’s conventions.  See
 * the original Spark implementation for further details.  Although unused
 * directly in this codebase, it is kept here for future compatibility.
 */

package org.apache.spark.sql.protobuf

package object backport {
  protected[backport] object ScalaReflectionLock
}