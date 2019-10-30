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

package org.finra.msd.enums;

/**
 * List of different type of sources that MegaSparkDiff supports
 */
public enum SourceType {
    JDBC("JDBC"),
    HIVE("HIVE"),
    FILE("FILE"),
    DYNAMODB("DYNAMODB"),
    JSON("JSON");

    /**
     * Represents the source type
     */
    private final String text;

    /**
     * Initializes the constructor with the source type provided
     * @param text
     */
    private SourceType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
