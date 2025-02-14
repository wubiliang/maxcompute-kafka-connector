package com.aliyun.odps.kafka.connect.account.impl;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;
import com.aliyun.odps.kafka.connect.account.AccountGenerator;

public class AliyunAccountGenerator implements AccountGenerator<AliyunAccount> {

  @Override
  public AliyunAccount generate(MaxComputeSinkConnectorConfig config) {
    String
        accessId =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCESS_ID.getName());
    String
        accessKey =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCESS_KEY.getName());
    return new AliyunAccount(accessId, accessKey);
  }
}
