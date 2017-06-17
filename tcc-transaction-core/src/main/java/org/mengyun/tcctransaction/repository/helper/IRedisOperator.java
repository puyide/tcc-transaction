package org.mengyun.tcctransaction.repository.helper;

import java.util.Set;


import redis.clients.jedis.JedisCluster;
/**
 * Created by puyide.
 * 主要解决集群redis 没有keys 命令
 */
public interface IRedisOperator {
	 /** 
     * 根据pattern 获取所有的keys 
     * @param pattern 
     * @return 
     */  
	Set<String> keys(JedisCluster jedisCluster, String pattern); 
	Set<byte[]> keys(JedisCluster jedisCluster, byte[] pattern); 
}
