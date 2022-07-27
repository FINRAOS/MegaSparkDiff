package org.finra.msd.containers

import scala.beans.BeanProperty

case class CountResult(@BeanProperty leftCount: Long, @BeanProperty rightCount: Long) {

}
