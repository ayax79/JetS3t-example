package com.nike.example.s3;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Example {

    private static final Logger log = Logger.getLogger(S3Example.class.getName());

    private static final String awsAccessKey = "YOUR_AWS_ACCESS_KEY";
    private static final String awsSecretKey = "YOUR_AWS_SECRET_KEY";
    private static final String bucketName = "YOUR_S3_BUCKET";

    private final S3Service service;
    private final S3Bucket bucket;

    public S3Example() throws S3ServiceException {
        final AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
        service = new RestS3Service(awsCredentials);
        final S3Bucket b = service.getBucket(bucketName);
        if (b == null) {
            bucket = service.createBucket(b);
        } else {
            bucket = b;
        }
    }

    public void put(String key, String value) {
        try {
            final S3Object obj = new S3Object(key, value);
            service.putObject(bucket, obj);
        } catch (NoSuchAlgorithmException | IOException | S3ServiceException e) {
            log.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public Optional<String> get(String key) {
        return getObject(key).transform(new Function<S3Object, String>() {
            @Override
            public String apply(S3Object input) {
                try {
                    return writeToString(input.getDataInputStream());
                } catch (IOException | ServiceException e) {
                    log.log(Level.SEVERE, e.getMessage(), e);
                    return null;
                }
            }
        });
    }

    private Optional<S3Object> getObject(final String key) {
        try {
            return Optional.of(service.getObject(bucketName, key));
        } catch (S3ServiceException e) {
            log.log(Level.SEVERE, e.getMessage(), e);
            return Optional.absent();
        }
    }

    private String writeToString(final InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        return new String(out.toByteArray());
    }


    public static void main(String[] args) throws S3ServiceException {
        final S3Example example = new S3Example();
        example.put("your key", "your value");
        final Optional<String> value = example.get("your key");
        if (value.isPresent()) {
            for (String s : value.asSet()) {
                log.info("The value for your key is: " + s);
            }
        }
        else {
            log.warning("Value not found for key");
        }
    }

}
