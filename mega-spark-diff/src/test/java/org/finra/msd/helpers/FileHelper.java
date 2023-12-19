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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;

public class FileHelper {
  public static Set<String> getFilenames(String directory, String prefix, String postfix) throws URISyntaxException {
    Path path = Paths.get(Objects.requireNonNull(FileHelper.class.getResource("/" + directory)).toURI());
    try (Stream<Path> stream = Files.list(path)) {
      return stream.filter(x -> !Files.isDirectory(x) &&
              x.getFileName().toString().startsWith(prefix) &&
              x.toString().endsWith(postfix))
          .map(Path::getFileName)
          .map(Path::toString)
          .map(x -> directory + "/" + x)
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getStringFromResource(String resource) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(FileHelper.class.getResourceAsStream(resource)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
