package org.finra.msd.customExceptions

final case class ColumnNullException(private val message: String = "",
                                     private val cause: Throwable = None.orNull) extends Exception(message, cause) {

}