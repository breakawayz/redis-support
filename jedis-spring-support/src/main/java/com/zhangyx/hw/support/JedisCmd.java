package com.zhangyx.hw.support;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyCommands;

/**
 *
 * 用于支持string类型的数据存取
 *
 */
public interface JedisCmd extends JedisCommands, MultiKeyCommands {
}
