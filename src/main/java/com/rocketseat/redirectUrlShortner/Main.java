package com.rocketseat.redirectUrlShortner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.builder().build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {

        String pathParameters = input.get("rawPath").toString();
        String shortUrlCode = pathParameters.replace("/", "");

        if (shortUrlCode == null || shortUrlCode.isEmpty()){
            throw new IllegalArgumentException("Invalid input: 'shortUrlCode' is required.");
        }

        InputStream s3ObjectStream;

        try{
            s3ObjectStream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket("url-shortener-get")
                    .key(shortUrlCode + ".json")
                    .build());

        } catch (Exception ex){
            throw new RuntimeException("Error fetching URL data from S3: " + ex.getMessage() + ex);
        }

        UrlData urlData;

        try{
            urlData = objectMapper.readValue(s3ObjectStream, UrlData.class);
        } catch (Exception ex){
            throw new RuntimeException("Error deserializing URL data: " + ex.getMessage() + ex);
        }

        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        Map<String, Object> response = new HashMap<>();

        //Cenario onde a URL expirou
        if (urlData.getExpirationTime() < currentTimeSeconds){
            response.put("Status Code", 410);
            response.put("body", "This URL has Expired!!");
            return response;
        }

        //Cenario onde a URL ainda é válida
        response.put("Status Code", 302);
        Map<String, String> headers = new HashMap<>();
        headers.put("Location", urlData.getOriginalUrl());
        response.put("Headers", headers);

        return response;
    }
}