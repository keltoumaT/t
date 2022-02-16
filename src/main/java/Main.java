import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {

        UUID uuid = UUID.randomUUID();
        String tempFolder = "working-temp__" + uuid + "/";
        String bucketName = "f-test-script";
       // String pref = "level-t/";
        List<String> prefs = new ArrayList<>();
        prefs.add("level-t/");
        prefs.add("level-e/");

        try {


            //  create array of all "folders" and loop through it from here (with prefix will contains the array value at current index)

            for (String pref : prefs) {

            ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("/").withPrefix(pref);
            ListObjectsV2Result result;

            do {
                result = amazonS3.listObjectsV2(listObjectsV2Request);

                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
                    // S3Object object = amazonS3.getObject(bucketName, objectSummary.getKey());
                    //   InputStream inputStream = object.getObjectContent();
                    List<String> commonPrefixes = result.getCommonPrefixes();
                    for (String prefix : commonPrefixes) {

                        String value = prefix.substring(pref.length());
                        System.out.println(value);
                        if (value.matches("\\d{4}-\\d{2}-\\d{2}\\/")) {
                            System.out.println("value " + value);
                            //then loop new request where prefix is this one
                            ListObjectsV2Request listObjectsV2Request2 = new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("/").withPrefix(prefix);
                            ListObjectsV2Result result2;
                            result2 = amazonS3.listObjectsV2(listObjectsV2Request2);
                            for (S3ObjectSummary objectSummary1 : result2.getObjectSummaries()) {
                                System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
                                S3Object object1 = amazonS3.getObject(bucketName, objectSummary1.getKey());
                                InputStream inputStream1 = object1.getObjectContent();

                                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                                StringWriter s = new StringWriter();
                                CSVWriter writer = new CSVWriter(s, '|',
                                        CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\n");

                                CSVReader reader = new CSVReader(new InputStreamReader(inputStream1, "UTF-8"));
                                String[] nextLine;
                                while ((nextLine = reader.readNext()) != null) {
                                    writer.writeNext(nextLine);
                                }
                                ObjectMetadata metadata = new ObjectMetadata();

                                //writer.flush();
                                writer.close();
                                String finalString = s.toString();
                                metadata.setContentLength(finalString.getBytes().length);

                                System.out.println("Actual data:- {} " + finalString);
                                amazonS3.putObject(bucketName, object1.getKey(), new ByteArrayInputStream(finalString.getBytes()), metadata);

                                System.out.println(String.valueOf(writer));
                            }
                        }
                    }
                }
                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                System.out.println("Next Continuation Token: " + token);
                listObjectsV2Request.setContinuationToken(token);
            } while (result.isTruncated());
        }
        } catch (
                SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }// Amazon S3 couldn't be contacted for a response, or the client
// couldn't parse the response from Amazon S3.
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        }
    }
}




