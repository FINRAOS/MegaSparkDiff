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

package org.finra.msd.controllers

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
