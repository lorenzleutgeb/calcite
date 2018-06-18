/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.openapi;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * A cache for OpenAPI responses.
 */
public class Cache {
  private static Path tempDirectory;

  static {
    try {
      tempDirectory = Files.createTempDirectory("calcite-openapi");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Cache() {}

  static File getFile(String url) throws IOException {
    final Pattern schemePattern = Pattern.compile("(https?|wss?|HTTPS?|WSS?):.*");
    if (!schemePattern.matcher(url).matches()) {
      return new File(url);
    }

    String target;
    try {
      target = URLEncoder.encode(url, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    final Path path = tempDirectory.resolve(target);
    if (!Files.exists(path)) {
      FileUtils.copyURLToFile(new URL(url), path.toFile());
    }
    return path.toFile();
  }
}

// End Cache.java
