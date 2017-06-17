package org.mengyun.tcctransaction.repository.helper;


import redis.clients.jedis.JedisCluster;


/**
 * Created by changming.xie on 9/15/16.
 */
public interface JedisClusterCallback<T> {

    public T doInJedis(JedisCluster jedis);
}
