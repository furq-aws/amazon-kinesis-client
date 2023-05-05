package software.amazon.kinesis.common;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.retrieval.KinesisClientFacade;
import software.amazon.kinesis.common.FunctionCache;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static software.amazon.kinesis.retrieval.KinesisClientFacade.region;
import static software.amazon.kinesis.common.StreamARNUtil.toARN;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamARNUtil.class, KinesisClientFacade.class})
public class StreamIdentifierTest {
    private static final String STREAM_NAME = "stream-name";
    private static final Region KINESIS_REGION = Region.US_WEST_1;
    private static final String TEST_ACCOUNT_ID = "123456789012";
    private static final long EPOCH = 1680616058L;
    private static final Arn DEFAULT_ARN = buildArn(KINESIS_REGION);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StreamARNUtilTest.setUpFunctionCache(new FunctionCache<String, Arn>((streamName) -> DEFAULT_ARN));
    }

    @Before
    public void setup() {
        mockStatic(KinesisClientFacade.class);
        when(region())
                .thenReturn(KINESIS_REGION);
    }

    /**
     * Test patterns that should match a serialization regex.
     */
    @Test
    public void testMultiStreamDeserializationSuccess() {
        final StreamIdentifier siSerialized = StreamIdentifier.multiStreamInstance(serialize());
        assertEquals(Optional.of(EPOCH), siSerialized.streamCreationEpochOptional());
        assertActualStreamIdentifierExpected(DEFAULT_ARN, siSerialized);
        verifyGetRegionStatic(1);
    }

    /**
     * Test patterns that <b>should not</b> match a serialization regex.
     */
    @Test
    public void testMultiStreamDeserializationFail() {
        for (final String pattern : Arrays.asList(
                // arn examples
                "arn:aws:kinesis::123456789012:stream/stream-name", // missing region
                "arn:aws:kinesis:region::stream/stream-name", // missing account id
                "arn:aws:kinesis:region:123456789:stream/stream-name", // account id not 12 digits
                "arn:aws:kinesis:region:123456789abc:stream/stream-name", // 12char alphanumeric account id
                "arn:aws:kinesis:region:123456789012:stream/", // missing stream-name
                // serialization examples
                ":stream-name:123", // missing account id
//                "123456789:stream-name:123", // account id not 12 digits
                "123456789abc:stream-name:123", // 12char alphanumeric account id
                "123456789012::123", // missing stream name
                "123456789012:stream-name", // missing delimiter and creation epoch
                "123456789012:stream-name:", // missing creation epoch
                "123456789012:stream-name:abc", // non-numeric creation epoch
                ""
        )) {
            try {
                StreamIdentifier.multiStreamInstance(pattern);
                Assert.fail(pattern + " should not have created a StreamIdentifier");
            } catch (final IllegalArgumentException iae) {
                // expected; ignore
            }
        }
    }

    @Test
    public void testInstanceFromArn() {
        final Arn arn = buildArn(KINESIS_REGION);
        final StreamIdentifier single = StreamIdentifier.singleStreamInstance(arn.toString());
        final StreamIdentifier multi = StreamIdentifier.multiStreamInstance(arn.toString());

        assertEquals(single, multi);
        assertEquals(Optional.empty(), single.streamCreationEpochOptional());
        assertActualStreamIdentifierExpected(arn, single);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInstanceWithoutEpochOrArn() {
        mockStatic(StreamARNUtil.class);
        when(toARN(STREAM_NAME, TEST_ACCOUNT_ID, KINESIS_REGION.toString()))
                .thenReturn(null);
        try {
            StreamIdentifier.singleStreamInstance(DEFAULT_ARN.toString());
        }
        finally {
            verifyToArnStatic(1);
        }
    }

    @Test
    public void testSingleStreamInstanceWithName() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME);
        assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        assertFalse(actualStreamIdentifier.accountIdOptional().isPresent());
        assertEquals(DEFAULT_ARN, actualStreamIdentifier.getStreamARN());
        assertEquals(STREAM_NAME, actualStreamIdentifier.streamName());
    }

    @Test
    public void testSingleStreamInstanceWithNameAndRegion() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME);
        assertFalse(actualStreamIdentifier.streamCreationEpochOptional().isPresent());
        assertFalse(actualStreamIdentifier.accountIdOptional().isPresent());
        assertEquals(STREAM_NAME, actualStreamIdentifier.streamName());
        assertEquals(DEFAULT_ARN, actualStreamIdentifier.getStreamARN());
    }

    @Test
    public void testMultiStreamInstanceWithIdentifierSerialization() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(serialize());
        assertActualStreamIdentifierExpected(DEFAULT_ARN, actualStreamIdentifier);
        verifyGetRegionStatic(1);
        assertEquals(Optional.of(EPOCH), actualStreamIdentifier.streamCreationEpochOptional());
    }

    @Test
    public void testMultiStreamInstanceWithoutRegionSerialized() {
        StreamIdentifier actualStreamIdentifier = StreamIdentifier.multiStreamInstance(
                serialize());
        assertActualStreamIdentifierExpected(actualStreamIdentifier);
        verifyGetRegionStatic(1);
    }

    private void assertActualStreamIdentifierExpected(StreamIdentifier actual) {
        assertActualStreamIdentifierExpected(DEFAULT_ARN, actual);
    }

    private void assertActualStreamIdentifierExpected(Arn expectedArn, StreamIdentifier actual) {
        assertEquals(STREAM_NAME, actual.streamName());
        assertEquals(Optional.of(TEST_ACCOUNT_ID), actual.accountIdOptional());
        assertEquals(expectedArn, actual.getStreamARN());
    }

    private void verifyGetRegionStatic(int count) {
        verifyStatic(times(count));
        region();
    }

    private void verifyToArnStatic(int count) {
        verifyStatic(times(count));
        toARN(STREAM_NAME, TEST_ACCOUNT_ID, KINESIS_REGION.toString());
    }

    /**
     * Creates a pattern that matches {@link StreamIdentifier} serialization.
     */
    private static String serialize() {
        return String.join(":", TEST_ACCOUNT_ID, STREAM_NAME, Long.toString(EPOCH));
    }

    private static Arn buildArn(final Region region) {
        return Arn.builder().partition("aws").service("kinesis")
                .accountId(TEST_ACCOUNT_ID)
                .resource("stream/" + STREAM_NAME)
                .region(region.toString())
                .build();
    }
}
