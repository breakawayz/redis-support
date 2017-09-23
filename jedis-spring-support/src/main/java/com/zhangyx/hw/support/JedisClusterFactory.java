package com.zhangyx.hw.support;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.Reflection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.FactoryBean;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ShardedJedis;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.ConnectException;
import java.util.Set;

/**
 * 动态代理，拦截redis请求，用于监控
 */
@Slf4j
public class JedisClusterFactory<T> extends ConfigruableProvider implements FactoryBean<T> {
    private Set<String> names = ImmutableSet.of("hashCode", "toString", "equals");
    private Class<T> clazz;

    @SuppressWarnings("unchecked")
    public JedisClusterFactory() {
        this.clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Override
    public T getObject() throws Exception {
        return Reflection.newProxy(clazz, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if (names.contains(method.getName())) {
                    if (getJedisCluster() != null) {
                        return method.invoke(getJedisCluster(), args);
                    } else {
                        return findDefault(method);
                    }
                }

                // TODO 这里可以开始监控埋点，对业务方隐藏细节

                JedisCluster jedisCluster = getJedisCluster();
                if (jedisCluster == null) {
                    return findDefault(method);
                }

                return exchangeRedis(jedisCluster, method, args);

            }
        });
    }

    private <T> Object exchangeRedis(JedisCluster jedisCluster, Method method, Object[] args) throws ConnectException {
        Object ret = null;
        boolean fail = true;
        long start = System.currentTimeMillis();
        try {
            T redis;
            try {
                redis = (T) jedisCluster;
            } catch (Exception e) {
                log.error("cannot getResource from:{}", getConfigFileName(), e);
                return null;
            }
            if (redis != null) {
                try {
                    ret = method.invoke(redis, args);
                    fail = false;
                } catch (Exception e) {
                    log.error("cannot exchange Redis: " + getConfigFileName(), e);
                    return null;
                } finally {
                    Client client = null;
                    if (redis instanceof Jedis) {
                        client = ((Jedis) redis).getClient();
                    }
//                    else if (args.length > 0) {
//                        Object key = args[0];
//                        if (key instanceof String) {
//                            client = ((ShardedJedis) redis).getShard((String) key).getClient();
//                        } else if (key instanceof byte[]) {
//                            client = ((ShardedJedis) redis).getShard((byte[]) key).getClient();
//                        }
//                    }
                    if (client != null) {
                        String url = client.getHost() + ':' + client.getPort() + '/' + client.getDB();
                    }
                }
            }
        } finally {
            //TODO 最终结果处理
        }
        return ret;
    }

    @Override
    public Class<?> getObjectType() {
        return clazz;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

}