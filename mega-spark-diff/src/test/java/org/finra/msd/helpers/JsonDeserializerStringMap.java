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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class JsonDeserializerStringMap implements JsonDeserializer<List<Map<String, String>>> {

  @Override
  public List<Map<String, String>> deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    List<Map<String, String>> list = new ArrayList<>();
    for (JsonElement element : json.getAsJsonArray()) {
      list.add(element.getAsJsonObject()
          .entrySet()
          .stream()
          .filter(x -> !x.getValue().isJsonNull())
          .collect(
              Collectors.toMap(
                  Entry::getKey,
                  x -> x.getValue().toString()
              )));
    }

    return list;
  }
}
