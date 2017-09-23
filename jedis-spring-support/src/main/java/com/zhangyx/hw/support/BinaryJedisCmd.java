package com.zhangyx.hw.support;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.MultiKeyBinaryCommands;

/**
 * 用于支持二进制类型的数据存取
 */
public interface BinaryJedisCmd extends BinaryJedisCommands, MultiKeyBinaryCommands {

}
