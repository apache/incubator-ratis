package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.CompositeByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToByteEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

public class RequestEncoder
    extends MessageToMessageEncoder<RequestData> {

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        RequestData requestData, List<Object> list) throws Exception {
    ByteBuffer bb = ByteBuffer.allocateDirect(8);
    bb.putInt(requestData.getDataId());
    bb.putInt(requestData.getBuff().capacity());
    bb.flip();
    list.add(Unpooled.wrappedBuffer(bb, requestData.getBuff()));
  }
}