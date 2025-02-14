package com.aliyun.odps.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.sink.SinkRecord;

public class SinkStatusContext {

  private long consumedOffsets;// 已经被tunnel成功写入的offset
  private final ConcurrentSkipListMap<Long, Long> offsetIntervalSet;// 记录多线程写入的offset区间值集合,需要保证线程安全
  private List<SinkRecord> recordQueue;// 当前分区拉取到的记录数
  private AtomicLong totalBytesSentByWriter; // 当前分区写入mc的字节数

  public SinkStatusContext(long consumedOffsets, List<SinkRecord> recordQueue) {
    this.consumedOffsets = consumedOffsets;
    this.recordQueue = recordQueue;
    offsetIntervalSet = new ConcurrentSkipListMap<>();
    totalBytesSentByWriter = new AtomicLong(0);
  }

  public void addOffsetInterval(long left, long right) {
    offsetIntervalSet.put(left, right);
  }

  public void addTotalBytesSentByWriter(Long bytes) {
    totalBytesSentByWriter.addAndGet(bytes);
  }

  public Long getTotalBytesSentByWriter() {
    return totalBytesSentByWriter.get();
  }

  public boolean containsOffset(long offset) {
    // 判断offset区间是否包含某个record的offset
    Long floorKey = offsetIntervalSet.floorKey(offset);
    return (floorKey != null && offset >= floorKey && offset <= offsetIntervalSet.get(floorKey));
  }

  public void removeFirst() {
    offsetIntervalSet.pollFirstEntry();
  }

  public long getConsumedOffsets() {
    return consumedOffsets;
  }

  public List<SinkRecord> getRecordQueue() {
    return recordQueue;
  }

  public void resetRecordQueue() {
    this.recordQueue = new ArrayList<>();
  }

  public void mergeOffset() {
    // 针对offset的区间更新consumedOffset
    while (!offsetIntervalSet.isEmpty() && consumedOffsets + 1 == offsetIntervalSet.firstKey()) {
      consumedOffsets = offsetIntervalSet.firstEntry().getValue();
      removeFirst();
    }
  }

  public boolean intervalOffsetEmpty() {
    return offsetIntervalSet.isEmpty();
  }
}
