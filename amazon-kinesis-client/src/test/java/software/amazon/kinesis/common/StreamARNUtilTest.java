package software.amazon.kinesis.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.retrieval.KinesisClientFacade;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ StreamARNUtil.class, KinesisClientFacade.class })
public class StreamARNUtilTest {
    private static final String KINESIS_STREAM_ARN_FORMAT = "arn:aws:kinesis:us-east-1:%s:stream/%s";
    private static final String ACCOUNT_ID = "12345";
    private static final String STREAM_NAME = StreamARNUtilTest.class.getSimpleName();

    /**
     * Original {@link FunctionCache} that is constructed on class load.
     */
    private static final FunctionCache<String, Arn> ORIGINAL_CACHE = Whitebox.getInternalState(
            StreamARNUtil.class, "STREAM_ARN_CACHE");
    private static final Arn defaultArn = toArn(KINESIS_STREAM_ARN_FORMAT, ACCOUNT_ID, STREAM_NAME);

    private FunctionCache<String, Arn> spyFunctionCache;

    @Before
    public void setUp() throws Exception {
        mockStatic(KinesisClientFacade.class);

        spyFunctionCache = spy(ORIGINAL_CACHE);
        setUpFunctionCache(spyFunctionCache);
        
        doReturn(defaultArn).when(spyFunctionCache).get(STREAM_NAME);
    }

    /**
     * Wrap and embed the original {@link FunctionCache} with a spy to avoid
     * one-and-done cache behavior, provide each test precise control over
     * return values, and enable the ability to verify interactions via Mockito.
     */
    public static void setUpFunctionCache(final FunctionCache<String, Arn> cache) throws Exception {
        final Field f = StreamARNUtil.class.getDeclaredField("STREAM_ARN_CACHE");
        f.setAccessible(true);
        f.set(null, cache);
        f.setAccessible(false);
    }

    @Test
    public void testGetStreamARNHappyCase() {
        getStreamArn();

        verify(spyFunctionCache).get(STREAM_NAME);
    }

    @Test
    public void testGetStreamARNFromCache() {
        // bypass the spy so KinesisClientFacade is called
        Mockito.when(spyFunctionCache.get(STREAM_NAME)).thenCallRealMethod();
        when(KinesisClientFacade.describeStreamSummaryWithStreamName(any(String.class)))
                .thenReturn(describeStreamSummaryResponse(STREAM_NAME));
        
        final Arn actualStreamARN1 = getStreamArn();
        final Arn actualStreamARN2 = getStreamArn();

        verify(spyFunctionCache, times(2)).get(STREAM_NAME);
        assertEquals(actualStreamARN1, actualStreamARN2);

        verifyKinesisClientFacadeStaticCall(1);
    }
    
    @Test
    public void testGetStreamARNReturnsNullWhenKinesisClientNotInitialized() {
        when(spyFunctionCache.get(STREAM_NAME)).thenCallRealMethod();
        final Arn actualStreamARN = StreamARNUtil.getStreamARN(STREAM_NAME);
        
        assertNull(actualStreamARN);

        verifyKinesisClientFacadeStaticCall(1);
    }

    @Test
    public void testGetStreamARNAfterKinesisClientInitiallyReturnsNull() {
        when(spyFunctionCache.get(STREAM_NAME)).thenCallRealMethod();
        when(KinesisClientFacade.describeStreamSummaryWithStreamName(any(String.class))).thenReturn(null);
        final Arn actualStreamARN1 = StreamARNUtil.getStreamARN(STREAM_NAME);
        assertNull(actualStreamARN1);

        when(KinesisClientFacade.describeStreamSummaryWithStreamName(any(String.class)))
                .thenReturn(describeStreamSummaryResponse(STREAM_NAME));
        getStreamArn();

        verifyKinesisClientFacadeStaticCall(2);
    }
    
    @Test
    public void testToARN() {
        final Arn actualArn = StreamARNUtil.toARN(STREAM_NAME, ACCOUNT_ID, Region.US_EAST_1.id());
        final Arn expectedArn = toArn(KINESIS_STREAM_ARN_FORMAT, ACCOUNT_ID, STREAM_NAME);

        assertEquals(expectedArn, actualArn);
    }

    private void verifyKinesisClientFacadeStaticCall(int count) {
        verifyStatic(times(count));
        KinesisClientFacade.describeStreamSummaryWithStreamName(STREAM_NAME);
    }

    private static Arn getStreamArn() {
        final Arn actualArn = StreamARNUtil.getStreamARN(STREAM_NAME);
        final Arn expectedArn = toArn(KINESIS_STREAM_ARN_FORMAT, ACCOUNT_ID, STREAM_NAME);

        assertNotNull(actualArn);
        assertEquals(expectedArn, actualArn);

        return actualArn;
    }

    private static Arn toArn(final String format, final Object... params) {
        return Arn.fromString(String.format(format, params));
    }

    private static DescribeStreamSummaryResponse describeStreamSummaryResponse(String streamName) {
        final StreamDescriptionSummary streamDescriptionSummary = StreamDescriptionSummary.builder()
                .streamARN(toArn(KINESIS_STREAM_ARN_FORMAT, ACCOUNT_ID, streamName).toString()).build();
        return DescribeStreamSummaryResponse.builder().streamDescriptionSummary(streamDescriptionSummary).build();
    }

}
