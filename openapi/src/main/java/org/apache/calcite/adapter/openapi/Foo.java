package org.apache.calcite.adapter.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.parser.OpenAPIV3Parser;

import java.util.Map;

public class Foo {
  public static void main(String[] args) {
    OpenAPI openAPI;
    openAPI = new OpenAPIV3Parser().read("/home/lorenz/Downloads/github.yaml");

    System.out.println(openAPI);

    for (Map.Entry<String, PathItem> e : openAPI.getPaths().entrySet()) {
      final PathItem pi = e.getValue();
      pi.getGet().getRequestBody();
      System.out.println("Got path " + e.getKey() + " with " + e.getValue());
    }

    for (Map.Entry<String, Schema> e : openAPI.getComponents().getSchemas().entrySet()) {
      System.out.println("Got schema: " + e.getKey() + " with title " + e.getValue().getTitle());
    }
  }
}
