package org.finra.msd.containers

import org.apache.spark.sql.DataFrame

import scala.beans.BeanProperty

case class DiffResult (@BeanProperty inLeftNotInRight :DataFrame, @BeanProperty inRightNotInLeft :DataFrame) {


}
