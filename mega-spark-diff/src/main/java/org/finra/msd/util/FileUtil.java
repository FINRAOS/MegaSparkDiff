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

package org.finra.msd.util;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Contains file handling operations
 */
public class FileUtil
{
    /**
     * Returns a file as a list of its lines
     * @param fileName
     * @return file contents per line
     */
    public static List<String> fileToStringList(File fileName)
    {
        BufferedReader br;
        List<String> fileContent = new ArrayList<String>();

        try
        {
            br = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = br.readLine()) != null)
                fileContent.add(line.trim().replaceAll(" +"," "));
            br.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return fileContent;
    }

    /**
     * Creates a directory
     * @param path Location where the directory has to be created
     */
    public static void createDirectory(String path)
    {
        File theDir = new File(path);
        // if the directory does not exist, create it
        if (!theDir.exists()) {
            theDir.mkdir();
        }
    }

    /**
     * Return the decoded contents of a file encoded in base 64
     * @param absoluteFileLocation
     * @return
     */
    public static String decodeBase64File(String absoluteFileLocation)
    {
        String decoded = "";
        try {
            decoded = new String(Base64.getDecoder().decode(FileUtils.readFileToString(new File(absoluteFileLocation)).trim())).trim();
        } catch (IOException e) {
            System.out.println(e);
        }
        return decoded;
    }
}
