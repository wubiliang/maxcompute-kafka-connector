/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.kafka.connect;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.kafka.KafkaWriter;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class MaxComputeSinkTask extends SinkTask {


  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkTask.class);

  private Odps odps;
  private String project;
  private String table;
  private RecordConverterBuilder converterBuilder;
  private PartitionWindowType partitionWindowType;
  private TimeZone tz;
  private Map<TopicPartition, MaxComputeSinkWriter> writers = new ConcurrentHashMap<>();
  private KafkaWriter runtimeErrorWriter = null;
  private ExecutorService executor; // used to execute multi-thread sinkWriter
  private boolean multiWriteMode = false;
  private boolean needSyncCommit = false;
  private boolean skipErrorRecords = false;
  private final Map<TopicPartition, SinkStatusContext> sinkStatus = new ConcurrentHashMap<>();
  private final Queue<Future<Boolean>> writerTasks = new LinkedList<>();
  // record the offset consumed by tunnel writer

  /*
    Performance metrics
   */
  private long totalBytesSentByClosedWriters = 0;
  private long startTimestamp;
  private final Map<TopicPartition, Long> partitionRecordsNumPerEpoch = new ConcurrentHashMap<>();
  /**
   * For Account
   */
  private MaxComputeSinkConnectorConfig config;
  private long odpsCreateLastTime;
  private long timeout;
  private String accountType;
  private String tunnelEndpoint;

  private boolean useStreamTunnel;

  private int bufferSizeKB = 64 * 1024;

  private int batchSize; // max_records in queue when sink writer can do run ;

  private int retryTimes;


  @Override
  public String version() {
    return VersionUtil.getVersion();
  }


  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter OPEN");
    for (TopicPartition partition : partitions) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") OPEN (topic: " +
                  partition.topic() + ", partition: " + partition.partition() + ")");
    }

    initOrRebuildOdps();
    writers.clear();
    sinkStatus.clear();
    writerTasks.clear();
    for (TopicPartition partition : partitions) {
      // TODO: Consider a way to resume when running in key or value mode
//      resumeCheckPoint(partition);
      MaxComputeSinkWriter writer = new MaxComputeSinkWriter(
          config,
          project,
          table,
          converterBuilder.build(),
          bufferSizeKB,
          partitionWindowType,
          tz, useStreamTunnel,
          retryTimes,
          tunnelEndpoint);
      writers.put(partition, writer);
      partitionRecordsNumPerEpoch.put(partition, 0L);
      LOGGER.info("Thread(" + Thread.currentThread().getId() +
                  ") Initialize writer successfully for (topic: " + partition.topic() +
                  ", partition: " + partition.partition() + ", failRetry: " + retryTimes + ")");
    }
  }

  /**
   * Start from last committed offset
   */
  private void resumeCheckPoint(TopicPartition partition) {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT MAX(offset) as offset ")
        .append("FROM ").append(table).append(" ")
        .append("WHERE topic=\"").append(partition.topic()).append("\" ")
        .append("AND partition=").append(partition.partition()).append(";");

    try {
      Instance findLastCommittedOffset = SQLTask.run(odps, queryBuilder.toString());
      findLastCommittedOffset.waitForSuccess();
      ResultSet res = SQLTask.getResultSet(findLastCommittedOffset);
      Long lastCommittedOffset = res.next().getBigint("offset");
      LOGGER.info("Thread(" + Thread.currentThread().getId() +
                  ") Last committed offset for (topic: " + partition.topic() + ", partition: "
                  + partition.partition() + "): " + lastCommittedOffset);
      if (lastCommittedOffset != null) {
        // Offset should be reset to the last committed plus one, otherwise the last committed
        // record will be duplicated
        context.offset(partition, lastCommittedOffset + 1);
      }
    } catch (OdpsException | IOException e) {
      LOGGER
          .error("Thread(" + Thread.currentThread().getId() + ") Resume from checkpoint failed", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start(Map<String, String> map) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter START");

    startTimestamp = System.currentTimeMillis();

    config = new MaxComputeSinkConnectorConfig(map);
    accountType =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCOUNT_TYPE.getName());
    timeout =
        config.getLong(MaxComputeSinkConnectorConfig.BaseParameter.CLIENT_TIMEOUT_MS.getName());
    bufferSizeKB =
        config.getInt(MaxComputeSinkConnectorConfig.BaseParameter.BUFFER_SIZE_KB.getName());
    retryTimes =
        config.getInt(MaxComputeSinkConnectorConfig.BaseParameter.FAIL_RETRY_TIMES.getName());

    String
        endpoint =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_ENDPOINT.getName());
    project =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_PROJECT.getName());
    table =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_TABLE.getName());
    tunnelEndpoint =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.TUNNEL_ENDPOINT.getName());
    batchSize =
        config.getInt(MaxComputeSinkConnectorConfig.BaseParameter.RECORD_BATCH_SIZE.getName());
    Integer
        poolSize =
        config.getInt(MaxComputeSinkConnectorConfig.BaseParameter.POOL_SIZE.getName());
    skipErrorRecords =
        config.getBoolean(MaxComputeSinkConnectorConfig.BaseParameter.SKIP_ERROR.getName());
    if (poolSize > 1) {
      executor = Executors.newFixedThreadPool(poolSize); // multi-thread to run record sink to MC
      multiWriteMode = true; // use new mode;
    }
    // Init odps
    odps = OdpsUtils.getOdps(config);
    odpsCreateLastTime = System.currentTimeMillis();
    odps.setEndpoint(endpoint);
    // Init converter builder
    RecordConverterBuilder.Format format = RecordConverterBuilder.Format.valueOf(
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.FORMAT.getName()));
    RecordConverterBuilder.Mode mode = RecordConverterBuilder.Mode.valueOf(
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.MODE.getName()));
    converterBuilder = new RecordConverterBuilder();
    converterBuilder.format(format).mode(mode);
    converterBuilder.schema(odps.tables().get(table).getSchema());

    // Parse partition window size
    partitionWindowType = PartitionWindowType.valueOf(
        config.getString(
            MaxComputeSinkConnectorConfig.BaseParameter.PARTITION_WINDOW_TYPE.getName()));
    // Parse time zone
    tz =
        TimeZone.getTimeZone(
            config.getString(MaxComputeSinkConnectorConfig.BaseParameter.TIME_ZONE.getName()));
    useStreamTunnel =
        config.getBoolean(MaxComputeSinkConnectorConfig.BaseParameter.USE_STREAM_TUNNEL.getName());

    if (useStreamTunnel) {
      LOGGER.info("MAXCOMPUTE STREAMING TUNNEL ENABLED.");
    }

    if (!StringUtils.isNullOrEmpty(
        config.getString(
            MaxComputeSinkConnectorConfig.BaseParameter.RUNTIME_ERROR_TOPIC_NAME.getName()))
        && !StringUtils.isNullOrEmpty(
        config.getString(
            MaxComputeSinkConnectorConfig.BaseParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName()))) {

      runtimeErrorWriter = new KafkaWriter(config);
      LOGGER.info(
          "Thread(" + Thread.currentThread().getId() + ") new runtime error kafka writer done");
    }

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Start MaxCompute sink task done");
  }

  /**
   * 从writer队列中获取一个新的可用的writer
   *
   * @param partition
   * @return MaxComputeSinkWriter
   */
  private MaxComputeSinkWriter genSinkWriter(TopicPartition partition) {
    return new MaxComputeSinkWriter(config,
                                    project,
                                    table,
                                    converterBuilder.build(),
                                    bufferSizeKB,
                                    partitionWindowType,
                                    tz, useStreamTunnel,
                                    retryTimes,
                                    tunnelEndpoint);
  }

  /**
   * 将sinkRecord的缓冲区队列中剩余的数据发送出去
   *
   * @return List<Future < MaxComputeSinkWriter> >
   */
  private void flushRecordBuffer() {
    for (Entry<TopicPartition, SinkStatusContext> item : sinkStatus.entrySet()) {
      TopicPartition partition = item.getKey();
      SinkStatusContext curContext = item.getValue();
      List<SinkRecord> left = curContext.getRecordQueue();
      if (!left.isEmpty()) {
          try {
              MaxComputeSinkWriter curWriter = genSinkWriter(partition);
              curWriter.setRecordBuffer(left);
              curWriter.setErrorReporter(runtimeErrorWriter);
              curWriter.setSinkStatusContext(curContext);
              curWriter.setSkipError(skipErrorRecords);
              writerTasks.add(executor.submit(curWriter));
              sinkStatus.get(partition).resetRecordQueue();
          }catch (Throwable e){
              LOGGER.error("flush buffer error,partition:{},error:{} ", partition, e.getMessage(), e);
          }
      }
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (multiWriteMode) {
      flushRecordBuffer();
      if (writerTasks.isEmpty()) {
        LOGGER.info("no data written to tunnel!");
      }
      while (!writerTasks.isEmpty()) {
        try {
          boolean result = writerTasks.poll().get();
          if (!result) {
            needSyncCommit = true;
          }
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.error("unrecoverable error happens: {} ,the task will exit! ", e.getMessage());
          throw new RuntimeException(e);
        }
      }
      if (LOGGER.isDebugEnabled()) {
        totalBytesSentByClosedWriters = 0;
        sinkStatus.forEach(
            (pt, cxt) -> totalBytesSentByClosedWriters += cxt.getTotalBytesSentByWriter());
        LOGGER.debug("Total bytes written by multi-writer :{}", totalBytesSentByClosedWriters);
      }
      // 本地的offset 和 currentOffsets 进行比较; 告诉其哪些partition 哪些地方已经消费了可以提交
      Set<TopicPartition> errorPartitions = new HashSet<>();
      for (Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
        TopicPartition partition = entry.getKey();
        OffsetAndMetadata offsetAndMetadata = entry.getValue();
        SinkStatusContext curStatus = sinkStatus.get(partition);
        if (curStatus != null) {
          curStatus.mergeOffset();
          // 需要在下次put操作后立刻提交
          needSyncCommit = !curStatus.intervalOffsetEmpty();
          long curOffset = curStatus.getConsumedOffsets();
          if (curOffset != -1) {
            currentOffsets.put(partition,
                               new OffsetAndMetadata(curOffset + 1, offsetAndMetadata.metadata()));
            LOGGER.info("partiton:{} consumed offset: {}", partition, curOffset);
          } else {
            // 这里存在一种情况，空的分区可能会被分配配task,没有数据，因此，innerOffsets需要在有数据进来的时候，才创建分区类型
            errorPartitions.add(partition);
            LOGGER.warn("something error in consumedOffset partition: {}", partition);
          }
        } else {
          errorPartitions.add(partition);
          LOGGER.warn("no partition exist in innerOffsets");
        }
      }
      errorPartitions.forEach(currentOffsets::remove);
    }
    flush(currentOffsets);
    return currentOffsets;
  }

  private void executeMultiWrite(Collection<SinkRecord> collection)
      throws IOException {
    // use multi-thread to sink data to MC ; decouple from origin mode
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Enter multiWriterExecute!~");
    }
    for (SinkRecord r : collection) {
      TopicPartition partition = new TopicPartition(r.topic(), r.kafkaPartition());
      if (!sinkStatus.containsKey(partition)) {
        sinkStatus.put(partition,
                       new SinkStatusContext(r.kafkaOffset() - 1, new ArrayList<>()));
      }
      if (sinkStatus.get(partition).containsOffset(r.kafkaOffset())) {
        continue;
      }
      List<SinkRecord> cur = sinkStatus.get(partition).getRecordQueue();
      // 需要记录当前收到的分区 offset 最小值
      cur.add(r);
      if (cur.size() >= batchSize) {
        // 从writerPool中获得writer
        MaxComputeSinkWriter curWriter = genSinkWriter(partition);
        curWriter.setRecordBuffer(cur);
        curWriter.setErrorReporter(runtimeErrorWriter);
        curWriter.setSinkStatusContext(sinkStatus.get(partition));
        curWriter.setSkipError(skipErrorRecords);
        writerTasks.add(executor.submit(curWriter));
        sinkStatus.get(partition).resetRecordQueue();
      }
    }
    if (needSyncCommit) {
      context.requestCommit();
    }
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter PUT");
    // Epoch second
    long time = System.currentTimeMillis() / 1000;
    initOrRebuildOdps();
    if (collection.isEmpty()) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter empty put records");
      return;
    } else {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") putted records size "
                  + collection.size());
    }
    if (multiWriteMode) {
      try {
        executeMultiWrite(collection);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return;
    }
    for (SinkRecord r : collection) {
      TopicPartition partition = new TopicPartition(r.topic(), r.kafkaPartition());
      MaxComputeSinkWriter writer = writers.get(partition);
      try {
        writer.write(r, time);
        partitionRecordsNumPerEpoch.put(partition, partitionRecordsNumPerEpoch.get(partition) + 1);
      } catch (IOException e) {
        reportRuntimeError(r, e);
      }
    }
  }

  private void reportRuntimeError(SinkRecord record, IOException e) {
    if (runtimeErrorWriter != null) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") skip runtime error", e);
      runtimeErrorWriter.write(record);
    } else {
      if (!skipErrorRecords) {
        throw new RuntimeException(e);
      }
      LOGGER.warn("Runtime error when handling: {}, skip the record", record.toString());
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter FLUSH");
      for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
        LOGGER.info("Thread(" + Thread.currentThread().getId() + ") FLUSH "
                    + "(topic: " + entry.getKey().topic() +
                    ", partition: " + entry.getKey().partition() + ")");
      }
    }
    initOrRebuildOdps();
    if (multiWriteMode) {
      // 新模式下已经在其他地方进行了writer的close and reset;
      return;
    }
    for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
      TopicPartition partition = entry.getKey();
      MaxComputeSinkWriter writer = writers.get(partition);

      // If the writer is not initialized, there is nothing to commit
      // fast return
      if (writer == null || partitionRecordsNumPerEpoch.get(partition) == 0) {
        LOGGER.debug(String.format("There is %d records to write! continue!",
                                   partitionRecordsNumPerEpoch.get(partition)));
        continue;
      }
      writer.flush();

      // Close writer
      try {
        writer.close();
        partitionRecordsNumPerEpoch.put(partition, 0L);
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        resetOffset(partition, writer);
      }

      // Update bytes sent
      totalBytesSentByClosedWriters += writer.getTotalBytes();

      // reset tunnel session = null, it will be initialized the next time coming data.
      writer.reset();

//      // Create new writer
//      MaxComputeSinkWriter newWriter = new MaxComputeSinkWriter(
//          odps,
//          project,
//          table,
//          converterBuilder.build(),
//          64,
//          partitionWindowType,
//          tz);
//      writers.put(partition, newWriter);
    }

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Total bytes sent: " +
                totalBytesSentByClosedWriters +
                ", elapsed time: " + ((System.currentTimeMillis()) - startTimestamp));
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter CLOSE");
    for (TopicPartition partition : partitions) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() +
                  ") CLOSE (topic: " + partition.topic() +
                  ", partition: " + partition.partition() + ")");
    }

    for (TopicPartition partition : partitions) {
      MaxComputeSinkWriter writer = writers.get(partition);

      // If the writer is not initialized, there is nothing to commit
      if (writer == null) {
        continue;
      }

      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close writer " + partition.toString() + ":" + e.getMessage(), e);
        resetOffset(partition, writer);
      }

      writers.remove(partition);
      totalBytesSentByClosedWriters += writer.getTotalBytes();
    }
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Total bytes sent: " +
                totalBytesSentByClosedWriters +
                ", elapsed time: " + ((System.currentTimeMillis()) - startTimestamp));
  }

  @Override
  public void stop() {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter STOP");

    for (Entry<TopicPartition, MaxComputeSinkWriter> entry : writers.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error("Failed to close writer " + entry.getKey().toString() + ":" + e.getMessage(),
                     e);
        resetOffset(entry.getKey(), entry.getValue());
      }
    }

    writers.values().forEach(w -> totalBytesSentByClosedWriters += w.getTotalBytes());
    writers.clear();
    if (executor != null) {
      executor.shutdown();
    }
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Total bytes sent: " +
                totalBytesSentByClosedWriters +
                ", elapsed time: " + ((System.currentTimeMillis()) - startTimestamp));
  }

  /**
   * Reset the offset of given partition.
   */
  private void resetOffset(TopicPartition partition, MaxComputeSinkWriter writer) {
    if (writer == null) {
      StringWriter s = new StringWriter();
      PrintWriter p = new PrintWriter(s, true);
      p.flush();
      LOGGER.error("Thread(" + Thread.currentThread().getId() + ") Reset offset but null " +
                   ", topic: " + partition.topic() +
                   ", partition: " + partition.partition() +
                   ", stack: " + p.toString());
      return;
    }

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Reset offset to " +
                writer.getMinOffset() + ", topic: " + partition.topic() +
                ", partition: " + partition.partition());
    // Reset offset
    Long minOffset = writer.getMinOffset();
    if(minOffset!=null){
        context.offset(partition, writer.getMinOffset());
    }
  }

  protected static TableSchema parseSchema(String json) {
    TableSchema schema = new TableSchema();
    try {
      JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

      if (tree.has("columns") && tree.get("columns") != null) {
        JsonArray columnsNode = tree.get("columns").getAsJsonArray();
        for (int i = 0; i < columnsNode.size(); ++i) {
          JsonObject n = columnsNode.get(i).getAsJsonObject();
          schema.addColumn(parseColumn(n));
        }
      }

      if (tree.has("partitionKeys") && tree.get("partitionKeys") != null) {
        JsonArray columnsNode = tree.get("partitionKeys").getAsJsonArray();
        for (int i = 0; i < columnsNode.size(); ++i) {
          JsonObject n = columnsNode.get(i).getAsJsonObject();
          schema.addPartitionColumn(parseColumn(n));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return schema;
  }

  private static Column parseColumn(JsonObject node) {
    String name = node.has("name") ? node.get("name").getAsString() : null;

    if (name == null) {
      throw new IllegalArgumentException("Invalid schema, column name cannot be null");
    }

    String typeString = node.has("type") ? node.get("type").getAsString().toUpperCase() : null;

    if (typeString == null) {
      throw new IllegalArgumentException("Invalid schema, column type cannot be null");
    }

    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeString);

    return new Column(name, typeInfo, null, null, null);
  }

  private void initOrRebuildOdps() {
    LOGGER.debug("Enter initOrRebuildOdps!");
    if (odps == null) {
      this.odps = OdpsUtils.getOdps(config);
    }

    // Exit fast
    if (!Account.AccountProvider.STS.name().equals(accountType)) {
      return;
    }

    long current = System.currentTimeMillis();
    if (current - odpsCreateLastTime > timeout) {
      LOGGER.info("STS AK timed out. Last: {}, current: {}", odpsCreateLastTime, current);
      this.odps = OdpsUtils.getOdps(config);
      LOGGER.info("Account refreshed. Creation time: {}", current);
      for (Map.Entry<TopicPartition, MaxComputeSinkWriter> eachWriter : writers.entrySet()) {
        eachWriter.getValue().refresh(odps);
      }
      LOGGER.info("Writers refreshed.");
      odpsCreateLastTime = current;
    }
  }
}
