package org.finra.msd.controllers

import org.finra.msd.basetestclasses.SparkFunSuite


class TemplateControllerSuite extends SparkFunSuite {

  test("html template loading") {
    val html = TemplateController.horizontalTableTemplate
    assert(html.contains("</html>"))
  }
}
