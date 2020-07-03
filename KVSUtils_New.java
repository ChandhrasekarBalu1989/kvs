package com.amazonaws.kvstranscribestreaming;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.kinesisvideo.parser.ebml.MkvTypeInfos;
import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import com.amazonaws.kinesisvideo.parser.mkv.MkvDataElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitor;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTag;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTrackMetadata;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoClientBuilder;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMedia;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMediaClientBuilder;
import com.amazonaws.services.kinesisvideo.model.APIName;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaResult;
import com.amazonaws.services.kinesisvideo.model.StartSelector;
import com.amazonaws.services.kinesisvideo.model.StartSelectorType;
import com.amazonaws.util.StringUtils;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KVSUtils {
  public enum TrackName {
    AUDIO_FROM_CUSTOMER("AUDIO_FROM_CUSTOMER"),
    AUDIO_TO_CUSTOMER("AUDIO_TO_CUSTOMER");
    
    private String name;
    
    TrackName(String name) {
      this.name = name;
    }
    
    public String getName() {
      return this.name;
    }
  }
  
  private static final Logger logger = LoggerFactory.getLogger(KVSUtils.class);
  
  private static String getTagFromStream(FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor, String tagName) {
    Iterator<MkvTag> iter = tagProcessor.getTags().iterator();
    while (iter.hasNext()) {
      MkvTag tag = iter.next();
      if (tagName.equals(tag.getTagName()))
        return tag.getTagValue(); 
    } 
    return null;
  }
  
  public static ByteBuffer getByteBufferFromStream(StreamingMkvReader streamingMkvReader, FragmentMetadataVisitor fragmentVisitor, FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor, String contactId, String track) throws MkvElementVisitException {
    while (streamingMkvReader.mightHaveNext()) {
      Optional<MkvElement> mkvElementOptional = streamingMkvReader.nextIfAvailable();
      if (mkvElementOptional.isPresent()) {
        MkvElement mkvElement = mkvElementOptional.get();
        mkvElement.accept((MkvElementVisitor)fragmentVisitor);
        if (MkvTypeInfos.EBML.equals(mkvElement.getElementMetaData().getTypeInfo())) {
          if (mkvElement instanceof com.amazonaws.kinesisvideo.parser.mkv.MkvStartMasterElement) {
            String contactIdFromStream = getTagFromStream(tagProcessor, "ContactId");
            if (contactIdFromStream != null && !contactIdFromStream.equals(contactId))
              return ByteBuffer.allocate(0); 
          } 
          continue;
        } 
        if (MkvTypeInfos.SIMPLEBLOCK.equals(mkvElement.getElementMetaData().getTypeInfo())) {
          MkvDataElement dataElement = (MkvDataElement)mkvElement;
          Frame frame = (Frame)dataElement.getValueCopy().getVal();
          ByteBuffer audioBuffer = frame.getFrameData();
          String isStopStreaming = getTagFromStream(tagProcessor, "STOP_STREAMING");
          if ("true".equals(isStopStreaming))
            return ByteBuffer.allocate(0); 
          long trackNumber = frame.getTrackNumber();
          MkvTrackMetadata metadata = fragmentVisitor.getMkvTrackMetadata(trackNumber);
          if (track.equals(metadata.getTrackName()))
            return audioBuffer; 
          if ("Track_audio/L16".equals(metadata.getTrackName()) && TrackName.AUDIO_FROM_CUSTOMER.getName().equals(track))
            return audioBuffer; 
        } 
      } 
    } 
    return ByteBuffer.allocate(0);
  }
  
  public static ByteBuffer getByteBufferFromStream(StreamingMkvReader streamingMkvReader, FragmentMetadataVisitor fragmentVisitor, FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor, String contactId, int chunkSizeInKB, String track) throws MkvElementVisitException {
    List<ByteBuffer> byteBufferList = new ArrayList<>();
    for (int i = 0; i < chunkSizeInKB; ) {
      ByteBuffer byteBuffer = getByteBufferFromStream(streamingMkvReader, fragmentVisitor, tagProcessor, contactId, track);
      if (byteBuffer.remaining() > 0) {
        byteBufferList.add(byteBuffer);
        i++;
      } 
    } 
    int length = 0;
    for (ByteBuffer bb : byteBufferList)
      length += bb.remaining(); 
    if (length == 0)
      return ByteBuffer.allocate(0); 
    ByteBuffer combinedByteBuffer = ByteBuffer.allocate(length);
    for (ByteBuffer bb : byteBufferList)
      combinedByteBuffer.put(bb); 
    combinedByteBuffer.flip();
    return combinedByteBuffer;
  }
  
  public static InputStream getInputStreamFromKVS(String streamName, Regions region, String startFragmentNum, AWSCredentialsProvider awsCredentialsProvider, String startSelectorType) {
    StartSelector startSelector;
    Validate.notNull(streamName);
    Validate.notNull(region);
    Validate.notNull(startFragmentNum);
    Validate.notNull(awsCredentialsProvider);
    AmazonKinesisVideo amazonKinesisVideo = (AmazonKinesisVideo)AmazonKinesisVideoClientBuilder.standard().build();
    String endPoint = amazonKinesisVideo.getDataEndpoint((new GetDataEndpointRequest()).withAPIName(APIName.GET_MEDIA).withStreamName(streamName)).getDataEndpoint();
    AmazonKinesisVideoMediaClientBuilder amazonKinesisVideoMediaClientBuilder = (AmazonKinesisVideoMediaClientBuilder)((AmazonKinesisVideoMediaClientBuilder)AmazonKinesisVideoMediaClientBuilder.standard().withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, region.getName()))).withCredentials(awsCredentialsProvider);
    AmazonKinesisVideoMedia amazonKinesisVideoMedia = (AmazonKinesisVideoMedia)amazonKinesisVideoMediaClientBuilder.build();
    startSelectorType = StringUtils.isNullOrEmpty(startSelectorType) ? "EARLIEST" : startSelectorType;
    switch (startSelectorType) {
      case "FRAGMENT_NUMBER":
        startSelector = (new StartSelector()).withStartSelectorType(StartSelectorType.FRAGMENT_NUMBER).withAfterFragmentNumber(startFragmentNum);
        logger.info("StartSelector set to FRAGMENT_NUMBER: " + startFragmentNum);
        break;
      default:
        startSelector = (new StartSelector()).withStartSelectorType(StartSelectorType.EARLIEST);
        logger.info("StartSelector set to EARLIEST");
        break;
    } 
    GetMediaResult getMediaResult = amazonKinesisVideoMedia.getMedia((new GetMediaRequest())
        .withStreamName(streamName)
        .withStartSelector(startSelector));
    logger.info("GetMedia called on stream {} response {} requestId {}", new Object[] { streamName, 
          Integer.valueOf(getMediaResult.getSdkHttpMetadata().getHttpStatusCode()), getMediaResult
          .getSdkResponseMetadata().getRequestId() });
    return getMediaResult.getPayload();
  }
}
