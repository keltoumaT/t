import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.List;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {

        UUID uuid = UUID.randomUUID();
        String tempFolder = "working-temp__" + uuid + "/";
        String bucketName = "test-s3script";

        try {
            AWSCredentials basicAWSCredentials = new BasicAWSCredentials("AKIAU657T2WKEHBDNIJ2", "iVFmaQ1Ru4ibX+1G4WbmMrn7t+tVzbvy6H8Kw9GY");
            AmazonS3 amazonS3 = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                    .withRegion("ca-central-1")
                    .build();

            ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("/").withPrefix("");
            ListObjectsV2Result result;

            do {
                result = amazonS3.listObjectsV2(listObjectsV2Request);

                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
                    S3Object object = amazonS3.getObject(bucketName, objectSummary.getKey());
                    InputStream inputStream = object.getObjectContent();
                    // amazonS3.copyObject(bucketName, objectSummary.getKey(), bucketName, tempFolder + objectSummary.getKey());
                    // CSVReader reader = new CSVReader(new InputStreamReader(inputStream, "UTF-8"));
                    //String [] nextLine;
                    //while(reader.readNext() != null){
                    //  System.out.println();
                    //}
                    String s = inputStream.toString().replaceAll(",", "|");
                    FileWriter writer = new FileWriter(object.getKey());
                    BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
                    String line;
                    while ((line = reader1.readLine()) != null) {
                        line.replaceAll(",", "|");
                        writer.append(line);
                        writer.append("\\n");
                    }
                    System.out.println(s.toString());
                    //File file = new File(writer.toString());

                    writer.flush();
                    writer.close();

                }
                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                System.out.println("Next Continuation Token: " + token);
                listObjectsV2Request.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (
                AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (
                SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}




