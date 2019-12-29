#!/bin/bash

filepath=`dirname $0`
HERE=`echo $(cd $filepath; pwd)`

spark-submit ${HERE}/mega-spark-diff-0.1.jar $@
#java -jar ${HERE}/mega-spark-diff-0.1.jar $@