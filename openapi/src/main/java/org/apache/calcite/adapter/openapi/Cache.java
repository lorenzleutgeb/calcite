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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * A cache for OpenAPI responses.
 */
public class Cache {
  protected Cache() {}

  static File getFile(String url) {
    final String cacheName = "cache";
    final Pattern schemePattern = Pattern.compile("(https?|wss?|HTTPS?|WSS?):.*");
    if (!schemePattern.matcher(url).matches()) {
      return new File(url);
    }

    final File cacheDir = new File(cacheName);
    if (!cacheDir.exists()) {
      cacheDir.mkdir();
    }

    String target = null;
    try {
      target = URLEncoder.encode(url, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    final Path path = Paths.get(cacheName, target);
    final File file = path.toFile();
    if (!file.exists()) {
      try {
        FileUtils.copyURLToFile(new URL(url), file);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return file;
  }
}

// End Cache.java
