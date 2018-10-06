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

package org.finra.msd.sparkcompare;


import org.finra.msd.basetestclasses.BaseJunitForSparkCompare;
import org.finra.msd.containers.AppleTable;
import org.finra.msd.containers.DiffResult;
import org.finra.msd.sparkfactory.SparkFactory;
import org.junit.Assert;
import org.junit.Test;


public class FileToFileTest extends BaseJunitForSparkCompare {
    public FileToFileTest() {
    }


    private DiffResult returnDiff(String fileName1, String fileName2) {
        String file1Path = this.getClass().getClassLoader().
                getResource(fileName1 + ".txt").getPath();
        AppleTable leftAppleTable = SparkFactory.parallelizeTextSource(file1Path, "table1");

        String file2Path = this.getClass().getClassLoader().
                getResource(fileName2 + ".txt").getPath();
        AppleTable rightAppleTable = SparkFactory.parallelizeTextSource(file2Path, "table2");

        return SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable);
    }

    @Test
    public void testCompareEqualFiles() {
        DiffResult diffResult = returnDiff("Test1", "Test2");

        //the expectation is that both tables are equal
        if (diffResult.inLeftNotInRight().count() != 0)
            Assert.fail("Expected 0 differences coming from left file." +
                    "  Instead, found " + diffResult.inLeftNotInRight().count() + ".");

        if (diffResult.inRightNotInLeft().count() != 0)
            Assert.fail("Expected 0 differences coming from right file." +
                    "  Instead, found " + diffResult.inRightNotInLeft().count() + ".");
    }

    @Test
    public void testCompareCompletelyDifferentFiles() {
        DiffResult diffResult = returnDiff("Test4", "Test5");

        //the expectation is that both tables are completely different
        if (diffResult.inLeftNotInRight().count() != 5)
            Assert.fail("Expected 5 differences coming from left table." +
                    "  Instead, found " + diffResult.inLeftNotInRight().count() + ".");


        if (diffResult.inRightNotInLeft().count() != 4)
            Assert.fail("Expected 4 differences coming from right table." +
                    "  Instead, found " + diffResult.inRightNotInLeft().count() + ".");
    }

    @Test
    public void testCompareAFewDifferences() {
        DiffResult diffResult = returnDiff("Test1", "Test3");

        //the expectation is that there are only a few differences
        if (diffResult.inLeftNotInRight().count() != 2)
            Assert.fail("Expected 2 differences coming from left table." +
                    "  Instead, found " + diffResult.inLeftNotInRight().count() + ".");

        if (diffResult.inRightNotInLeft().count() != 2)
            Assert.fail("Expected 2 differences coming from right table." +
                    "  Instead, found " + diffResult.inRightNotInLeft().count() + ".");
    }

    @Test
    public void testCompareTable1IsSubset() {
        DiffResult diffResult = returnDiff("Test4", "Test1");

        //the expectation is that table1 is a complete subset of table2
        Long leftCount = diffResult.inLeftNotInRight().count();
        Long rightCount = diffResult.inRightNotInLeft().count();
        if (leftCount != 0)
            Assert.fail("Expected 0 differences coming from left table." +
                    "  Instead, found " + leftCount + ".");

        if (rightCount != 4)
            Assert.fail("Expected 4 differences coming from right table." +
                    "  Instead, found " + rightCount + ".");
    }

    @Test
    public void testCompareTable2IsSubset() {
        DiffResult diffResult = returnDiff("Test1", "Test5");

        //the expectation is that table2 is a complete subset of table1
        if (diffResult.inLeftNotInRight().count() != 5)
            Assert.fail("Expected 5 differences coming from left table." +
                    "  Instead, found " + diffResult.inLeftNotInRight().count() + ".");

        if (diffResult.inRightNotInLeft().count() != 0)
            Assert.fail("Expected 0 differences coming from right table." +
                    "  Instead, found " + diffResult.inRightNotInLeft().count() + ".");
    }

}
