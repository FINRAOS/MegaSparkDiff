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

package org.finra.msd.helpers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class JsonHelper {

  public static Type type = new TypeToken<List<Map<String, String>>>() {}.getType();

  public static Gson gson = new GsonBuilder().registerTypeAdapter(
      type,
      new JsonDeserializerStringMap()
  ).create();

  public static List<Map<String, String>> jsonToMapList(String jsonString) {
    return gson.fromJson(jsonString, type);
  }
}
