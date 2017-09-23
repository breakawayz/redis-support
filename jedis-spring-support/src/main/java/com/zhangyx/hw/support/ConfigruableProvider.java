package com.zhangyx.hw.support;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import redis.clients.jedis.*;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 用于解析和处理redis配置文件
 */
@Slf4j
@Data
public class ConfigruableProvider implements InitializingBean, DisposableBean {
    private JedisPool pool = null;   //多个地址时用shared分片
    private ShardedJedisPool shardedPool = null;// 只有一个地址
    private String configFileName;

    private static final Set<String> types = ImmutableSet.of(
            "Boolean", "Character", "Byte", "Short", "Long", "Integer", "Byte", "Float", "Double", "Void", "String");

    private final CharMatcher spilter = CharMatcher.anyOf(", ;|");  // 分割符

    private JedisCluster jedisCluster = null;

    /**
     * 读取配置文件,在bean初始化时
     * TODO 配置变更时热加载
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Configuration config = new PropertiesConfiguration(configFileName);
        // TODO 这里需要配置监听，用于热加载
        loadCofig(config);
    }

    private void loadCofig(Configuration config) {
        String configServers = config.getString("redis.servers");
        if (Strings.isNullOrEmpty(configServers)) {
            return;
        }
        // 该配置不启动
        if (!config.getBoolean("startup", true)) {
            close(shardedPool);
            shardedPool = null;
            return;
        }
        JedisPoolConfig poolConfig = createJedisPoolConfig(config);
        if (config.getBoolean("cluster_enable", false)) {

            Set<HostAndPort> clusterNodes = genClusterNode(configServers);
            int connTimeout = config.getInt("redis.connTimeout", 5000);
            int soTimeout = config.getInt("redis.soTimeout", 5000);
            String password = config.getString("redis.password");
            int maxAttempts = config.getInt("pool.maxAttempts", 3);

            if (!Strings.isNullOrEmpty(password)) {
                jedisCluster = new JedisCluster(clusterNodes, connTimeout, soTimeout, maxAttempts, password, poolConfig);
            } else {
                jedisCluster = new JedisCluster(clusterNodes, connTimeout, soTimeout, maxAttempts, poolConfig);
            }
        } else {
            if (spilter.countIn(configServers) > 0) {
                List<JedisShardInfo> servers = getJedisShardInfos(config);
                ShardedJedisPool shardedJedisPool = new ShardedJedisPool(poolConfig, servers);
                // 说明启动了热加载，这里待补充 TODO 热加载处理
                if (shardedPool != null) {
                    ShardedJedisPool old = shardedPool;
                    shardedPool = shardedJedisPool;
                    close(old);
                } else {
                    shardedPool = shardedJedisPool;
                }
            } else {
                JedisPool jedisPool = createJedisPool(poolConfig, config);
                if (pool != null) {
                    JedisPool old = pool;
                    pool = jedisPool;
                    close(old);
                } else {
                    pool = jedisPool;
                }
            }
        }
    }

    private List<JedisShardInfo> getJedisShardInfos(Configuration config) {
        List<JedisShardInfo> servers = Lists.newArrayList();
        String configServers = config.getString("redis.servers");
        // server list字符串拆分
        List<String> items = Splitter.on(spilter).trimResults().omitEmptyStrings().splitToList(configServers);

        int connTimeout = config.getInt("redis.connTimeout", 5000);
        int soTimeout = config.getInt("redis.soTimeout", 5000);
        String password = config.getString("redis.password");
        int defaultDbIndex = config.getInt("redis.dbIndex", 0);
        items.forEach(server -> {
            // formate 127.0.0.1:6379 拆分
            List<String> paths = Splitter.on(CharMatcher.anyOf(":/")).splitToList(server);
            String host = paths.get(0);
            int port = 6379;  // 默认端口号
            int dbIndex = defaultDbIndex; // 数据库
            if (paths.size() > 1) {
                port = Integer.parseInt(paths.get(1));
            }

            if (paths.size() > 2) {
                dbIndex = Integer.parseInt(paths.get(2));
            }
            URI redisUri = create(host, port, password, dbIndex);
            JedisShardInfo info = new JedisShardInfo(redisUri);
            info.setConnectionTimeout(connTimeout);
            info.setSoTimeout(soTimeout);
            if (!Strings.isNullOrEmpty(password) && password.contains("@")) {
                info.setPassword(password);
            }
            servers.add(info);
        });

        return servers;
    }

    private JedisPool createJedisPool(JedisPoolConfig poolConfig, Configuration config) {
        int timeout = config.getInt("redis.timeout", 5000);
        String password = config.getString("redis.password");
        int defaultDbIndex = config.getInt("redis.dbIndex", 0);
        String server = config.getString("redis.servers");
        List<String> paths = Splitter.on(CharMatcher.anyOf(":/")).splitToList(server);
        String host = paths.get(0);
        int port = 6379;
        int dbIndex = defaultDbIndex;
        if (paths.size() > 1) {
            port = Integer.parseInt(paths.get(1));
        }
        if (paths.size() > 2) {
            dbIndex = Integer.parseInt(paths.get(2));
        }

        password = Strings.isNullOrEmpty(password) ? null : password;
        return new JedisPool(poolConfig, host, port, timeout, password, dbIndex);
    }

    /**
     * 获取集群服务器
     *
     * @param servers
     * @return
     */
    private Set<HostAndPort> genClusterNode(String servers) {
        Set<HostAndPort> clusterNodes = new HashSet<HostAndPort>();

        // server list字符串拆分
        List<String> items = Splitter.on(spilter).trimResults().omitEmptyStrings().splitToList(servers);
        items.forEach(server -> {
            List<String> paths = Splitter.on(CharMatcher.anyOf(":/")).splitToList(server);
            String host = paths.get(0);
            int port = 6379;  // 默认端口号
            if (paths.size() > 1) {
                port = Integer.parseInt(paths.get(1));
            }

            clusterNodes.add(new HostAndPort(host, port));

        });
        return clusterNodes;
    }

    /**
     * 创建url
     *
     * @param host
     * @param port
     * @param password
     * @param dbIndex
     * @return
     */
    protected URI create(String host, int port, String password, int dbIndex) {
        StringBuilder sbd = new StringBuilder(32);
        sbd.append("redis://");
        if (!Strings.isNullOrEmpty(password) && !password.contains("@")) {
            sbd.append("user:").append(password).append('@');
        }
        sbd.append(host).append(':').append(port);
        if (dbIndex > 0) {
            sbd.append('/').append(dbIndex);
        }
        return URI.create(sbd.toString());
    }

    /**
     * 创建启动参数
     *
     * @param config
     * @return
     */
    public JedisPoolConfig createJedisPoolConfig(Configuration config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getInt("pool.maxActive", 100));
        poolConfig.setMinIdle(config.getInt("pool.minIdle", 5));
        poolConfig.setMaxIdle(config.getInt("pool.maxIdle", 25));
        poolConfig.setMaxWaitMillis(config.getInt("pool.maxWaitMillis", 3000));
        poolConfig.setTestOnBorrow(config.getBoolean("pool.testOnBorrow", false));
        poolConfig.setTestOnReturn(config.getBoolean("pool.testOnReturn", false));
        poolConfig.setTestWhileIdle(config.getBoolean("pool.testWhileIdle", true));
        return poolConfig;
    }

    @Override
    public void destroy() throws Exception {
        close(shardedPool);
    }

    /**
     * 关闭销毁资源
     *
     * @param c
     */
    private void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
                c = null;
            } catch (IOException e) {
                log.error("cannot close {}", c, e);
            }
        }
    }

    /**
     * 返回默认值
     *
     * @param method
     * @return
     */
    protected Object findDefault(Method method) {
        Class<?> clz = method.getReturnType();
        if (clz == String.class) {
            return "";
        } else if (clz == Long.class || clz == Double.class) {
            return 0L;
        } else if (clz == Boolean.class) {
            return Boolean.FALSE;
        } else if (clz.isArray()) {
            return new byte[0];
        } else if (clz.isAssignableFrom(Set.class)) {
            return ImmutableSet.of();
        } else if (clz.isAssignableFrom(List.class)) {
            return ImmutableList.of();
        } else if (clz.isAssignableFrom(Map.class)) {
            return ImmutableMap.of();
        } else {
            return null;
        }
    }
}
