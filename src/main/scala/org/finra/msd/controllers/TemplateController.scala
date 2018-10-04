package org.finra.msd.controllers

import scala.io.Source

object TemplateController {


  lazy val horizontalTableTemplateLocation = {
    this.getClass.getClassLoader.getResource("htmltemplates/horizontalTableTemplate.html").getPath
  }

  lazy val horizontalTableTemplate = {
    Source.fromFile(horizontalTableTemplateLocation).getLines.mkString
  }
}
