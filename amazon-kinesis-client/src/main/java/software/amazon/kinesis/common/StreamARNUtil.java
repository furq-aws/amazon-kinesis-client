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

    /**
     * Caches an {@link Arn} from a {@link KinesisClientFacade#describeStreamSummaryWithStreamName(String)} call.
     */
    private static final FunctionCache<String, Arn> STREAM_ARN_CACHE = new FunctionCache<String, Arn>(streamName -> {
        final DescribeStreamSummaryResponse response = KinesisClientFacade.describeStreamSummaryWithStreamName(streamName);
        if (response == null) return null;
        return Arn.fromString(response.streamDescriptionSummary().streamARN());
    });

    /**
     * Constructs a stream ARN using the stream name, accountId, and region.
     *
     * @param streamName stream name
     * @param accountId account id
     * @param region region
     */
    public static Arn toARN(String streamName, String accountId, String region) {
        return Arn.builder()
                .partition("aws")
                .service("kinesis")
                .region(region)
                .accountId(accountId)
                .resource("stream/" + streamName)
                .build();
    }

    /**
     * Retrieves the stream ARN from Kinesis using the stream name.
     *
     * @param streamName stream name
     * @return an {@link Arn} from cached Kinesis call response,
     *         may be null if {@link KinesisClientFacade} has not yet been initialized
     */
    public static Arn getStreamARN(String streamName) {
        return STREAM_ARN_CACHE.get(streamName);
    }

}
