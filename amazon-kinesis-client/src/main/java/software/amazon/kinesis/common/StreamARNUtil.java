package software.amazon.kinesis.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.kinesis.retrieval.KinesisClientFacade;

import java.util.regex.Pattern;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamARNUtil {

    /**
     * Caches an {@link Arn} from a {@link KinesisClientFacade#describeStreamSummary(String)} call.
     */
    private static final FunctionCache<String, Arn> STREAM_ARN_CACHE = new FunctionCache<>(streamName -> {
        final DescribeStreamSummaryResponse response = KinesisClientFacade.describeStreamSummary(streamName);
        if (response == null) return null;
        return Arn.fromString(response.streamDescriptionSummary().streamARN());
    });

    /**
     * Pattern for a stream ARN. The valid format is
     * {@code arn:aws:kinesis:<region>:<accountId>:stream:<streamName>}
     * where {@code region} is the id representation of a {@link Region}.
     */
    public static final Pattern STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws:kinesis:(?<region>[-a-z0-9]+):(?<accountId>[0-9]{12}):stream/(?<streamName>.+)");

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
