package org.finra.msd.controllers

import scala.io.Source

/**
  * This class is intended to hold locations and actual values of html templates that will be used for visualizations
  * whether reports that are saved to disc or templates for use inside DataBricks or Jyputer
  */
object TemplateController {

  /**
    * location path of the resource file having the HTML template
    */
  lazy val horizontalTableTemplateLocation: String = {
    this.getClass.getClassLoader.getResource("htmltemplates/horizontalTableTemplate.html").getPath
  }

  /**
    * Actual HTML value as String for the horizontal table HTML template. The marker is #tableContent which should
    * be replaced by the actual HTML table
    */
  lazy val horizontalTableTemplate: String = {
    Source.fromFile(horizontalTableTemplateLocation).getLines.mkString
  }
}
