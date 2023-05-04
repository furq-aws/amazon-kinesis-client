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
    
    private static final FunctionCache<String, Arn> STREAM_ARN_CACHE = new FunctionCache<String, Arn>(streamName -> {
        final DescribeStreamSummaryResponse response = KinesisClientFacade.describeStreamSummaryWithStreamName(streamName);
        if (response == null) return null;
        return Arn.fromString(response.streamDescriptionSummary().streamARN());
    }
    );
    
    // can/should we remove region argument and call KCF.region from here (remove completely from SI)?
    public static Arn toArn(String streamName, String accountId, String region) {
        return Arn.builder()
                .partition("aws")
                .service("kinesis")
                .region(region)
                .accountId(accountId)
                .resource("stream/" + streamName)
                .build();
    }
    
    public static Arn getStreamARN(String streamName) {
        return STREAM_ARN_CACHE.get(streamName);
    }

}
