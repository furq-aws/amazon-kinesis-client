package software.amazon.kinesis.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.kinesis.retrieval.KinesisClientFacade;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamARNUtil {
    
    // this cached streamARN is only used when a customer provides a single-stream by name
    private static Arn cachedStreamARN;

    // can/should we remove region argument and call KCF.region from here (remove completely from SI)?
    public static Arn toArn(String streamName, String accountId, String region) {
        return Arn.builder()
                .partition("aws")
                .service("kinesis")
                .region(region.toString())
                .accountId(accountId)
                .resource("stream/" + streamName)
                .build();
    }
    
    public static Arn getStreamARN(String streamName) {
        if (cachedStreamARN != null) {
            log.info("furq123-returning cached ARN {}", cachedStreamARN);
            return cachedStreamARN;
        }

        log.info("furq123- calling dss");
        final DescribeStreamSummaryResponse dss = KinesisClientFacade.describeStreamSummaryWithStreamName(streamName);
        if (dss == null) {
            log.info("furqError- dss was null, returning null ARN");
            return null;
        }
        
        cachedStreamARN = Arn.fromString(dss.streamDescriptionSummary().streamARN());
        log.info("furq1233-retrieved arn: {}.", cachedStreamARN);
        
        return cachedStreamARN;
    }

}
