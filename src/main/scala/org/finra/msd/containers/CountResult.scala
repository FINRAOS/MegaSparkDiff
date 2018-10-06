package org.finra.msd.containers

import scala.beans.BeanProperty

case class CountResult(@BeanProperty letCount: Long, @BeanProperty rightCount: Long) {

}
