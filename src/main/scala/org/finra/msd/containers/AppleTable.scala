/*
 * Copyright 2017 MegaSparkDiff Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.msd.containers


import org.apache.spark.sql.DataFrame
import org.finra.msd.enums.SourceType

import scala.beans.BeanProperty


/**
  * Custom container class that stores all data gotten from any source
  * @param sourceType
  * @param dataFrame
  * @param delimiter
  * @param tempViewName
  */
case class AppleTable(@BeanProperty sourceType: SourceType, @BeanProperty dataFrame: DataFrame, @BeanProperty delimiter: String
                     , @BeanProperty tempViewName: String) {

}

