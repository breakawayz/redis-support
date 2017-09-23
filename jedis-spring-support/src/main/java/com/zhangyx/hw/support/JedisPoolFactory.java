package com.zhangyx.hw.support;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.Reflection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.FactoryBean;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.ConnectException;
import java.util.Set;

/**
 * 动态代理，拦截redis请求，用于监控
 */
@Slf4j
public class JedisPoolFactory<T> extends ConfigruableProvider implements FactoryBean<T> {
    private Set<String> names = ImmutableSet.of("hashCode", "toString", "equals");
    private Class<T> clazz;

    @SuppressWarnings("unchecked")
    public JedisPoolFactory() {
        this.clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Override
    public T getObject() throws Exception {
        return Reflection.newProxy(clazz, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if (names.contains(method.getName())) {
                    JedisPool pool = getPool();
                    if (pool != null) {
                        return method.invoke(pool.getResource(), args);
                    } else {
                        ShardedJedisPool sPool = getShardedPool();
                        if (sPool == null) {
                            return findDefault(method);
                        }
                        return method.invoke(sPool.getResource(), args);
                    }
                }

                // TODO 这里可以开始监控埋点，对业务方隐藏细节

                JedisPool pool = getPool();
                if (pool != null) {
                    return exchangeRedis(pool, method, args);
                } else {
                    ShardedJedisPool sPool = getShardedPool();
                    if (sPool == null) {
                        return findDefault(method);
                    }
                    return exchangeRedis(sPool, method, args);
                }
            }
        });
    }

    private <T> Object exchangeRedis(Pool<T> pool, Method method, Object[] args) throws ConnectException {
        Object ret = null;
        boolean fail = true;
        long start = System.currentTimeMillis();
        try {
            T redis;
            try {
                redis = pool.getResource();
            } catch (Exception e) {
                log.error("cannot getResource from:{}", getConfigFileName(), e);
                return null;
            }
            if (redis != null) {
                try {
                    ret = method.invoke(redis, args);
                    pool.returnResource(redis);
                    fail = false;
                } catch (Exception e) {
                    pool.returnBrokenResource(redis);
                    log.error("cannot exchange Redis: " + getConfigFileName(), e);
                    return null;
                } finally {
                    Client client = null;
                    if (redis instanceof Jedis) {
                        client = ((Jedis) redis).getClient();
                    } else if (args.length > 0) {
                        Object key = args[0];
                        if (key instanceof String) {
                            client = ((ShardedJedis) redis).getShard((String) key).getClient();
                        } else if (key instanceof byte[]) {
                            client = ((ShardedJedis) redis).getShard((byte[]) key).getClient();
                        }
                    }
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
