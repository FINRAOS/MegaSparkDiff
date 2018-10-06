package org.finra.msd.controllers

import scala.io.Source

/**
  * This class is intended to hold locations and actual values of html templates that will be used for visualizations
  * whether reports that are saved to disc or templates for use inside DataBricks or Jyputer
  */
object TemplateController {

  /**
    * Actual HTML value as String for the horizontal table HTML template. The marker is #tableContent which should
    * be replaced by the actual HTML table
    */
  lazy val horizontalTableTemplate: String = {
    s"""
       |<!DOCTYPE html>
       |<html>
       |<head>
       |    <meta charset="utf-8">
       |    <style>
       |                body {
       |                  font: 13px Lato, sans-serif;
       |                  font-weight: lighter;
       |                  transform: scale(1) !important;
       |                }
       |
 |                table, tr, th, td {
       |                  margin: 0 auto;
       |                  max-width: 95%;
       |                  border:1px solid black;
       |                  padding: 5px;
       |                  text-align: center;
       |                }
       |
 |                th, td {
       |                  min-width: 60px;
       |                }
       |
 |                table {
       |                  margin-top: 30px;
       |                  border-collapse: collapse;
       |                }
       |
 |                th {
       |                  background: #65BDF0;
       |                  font: 12px Lato, sans-serif;
       |                }
       |
 |                td.different {
       |                  background: yellow;
       |                }
       |
 |                td.same td {
       |                  background: white;
       |                }
       |
 |    </style>
       |</head>
       |<body>
       |#tableContent
       |</body>
       |</html>
     """.stripMargin
  }
}
