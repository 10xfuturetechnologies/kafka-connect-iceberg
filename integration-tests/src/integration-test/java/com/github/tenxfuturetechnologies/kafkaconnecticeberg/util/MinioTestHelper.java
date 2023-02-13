package com.github.tenxfuturetechnologies.kafkaconnecticeberg.util;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_ACCESS_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_BUCKET;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_REGION_NAME;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_SECRET_KEY;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class MinioTestHelper {

  private final MinioClient client;

  public MinioTestHelper(String s3url) {
    client = MinioClient.builder()
        .endpoint(s3url)
        .credentials(S3_ACCESS_KEY, S3_SECRET_KEY)
        .build();
  }

  public void createDefaultBucket() {
    try {
      client.ignoreCertCheck();
      client.makeBucket(MakeBucketArgs.builder()
          .region(S3_REGION_NAME)
          .bucket(S3_BUCKET)
          .build());
    } catch (Exception e) {
      throw new RuntimeException("Unable to create bucket");
    }
  }

  public void deleteObjects(String prefix) {
    try {
      client.ignoreCertCheck();
      var objects = client.listObjects(ListObjectsArgs.builder()
          .region(S3_REGION_NAME)
          .bucket(S3_BUCKET)
          .prefix(prefix)
          .build());
      client.removeObjects(RemoveObjectsArgs.builder()
          .region(S3_REGION_NAME)
          .bucket(S3_BUCKET)
          .objects(
              StreamSupport.stream(objects.spliterator(), false)
                  .map(this::toDeleteObject)
                  .collect(Collectors.toList()))
          .build());
    } catch (Exception e) {
      throw new RuntimeException("Unable to create bucket");
    }
  }

  public void getObject(String path, OutputStream out) {
    var key = URI.create(path).getPath();
    try {
      var response = client.getObject(GetObjectArgs.builder()
          .region(S3_REGION_NAME)
          .bucket(S3_BUCKET)
          .object(key)
          .build());
      response.transferTo(out);
    } catch (Exception e) {
      throw new RuntimeException("Unable to read object " + key, e);
    }
  }

  public List<String> listObjects(String key) {
    try {
      var response = client.listObjects(ListObjectsArgs.builder()
          .region(S3_REGION_NAME)
          .bucket(S3_BUCKET)
          .prefix(key)
          .build());
      return StreamSupport.stream(response.spliterator(), false)
          .map(this::toObjectName)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Unable to read object " + key, e);
    }
  }

  private DeleteObject toDeleteObject(Result<Item> itemResult) {
    try {
      return new DeleteObject(itemResult.get().objectName());
    } catch (Exception e) {
      throw new RuntimeException("Unable to delete item", e);
    }
  }

  private String toObjectName(Result<Item> itemResult) {
    try {
      return itemResult.get().objectName();
    } catch (Exception e) {
      throw new RuntimeException("Unable to get object name from item " + itemResult, e);
    }
  }
}
