package org.mengyun.tcctransaction.repository.helper;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
/**
 * Created by puyide.
 * 主要解决集群redis 没有keys 命令 实现
 */
public class RedisOperator implements IRedisOperator {

	 
     
  
    @Override  
    public Set<String> keys(JedisCluster jedisCluster,String pattern){  
        
        Set<String> keys = new TreeSet<>();  
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();  
        for(String k : clusterNodes.keySet()){  
             
            JedisPool jp = clusterNodes.get(k);  
            Jedis connection = jp.getResource();  
            try {  
                keys.addAll(connection.keys(pattern));  
            } catch(Exception e){  
            	e.printStackTrace();
            } finally{  
                   
                connection.close();//用完一定要close这个链接！！！  
            }  
        }  
       
        return keys;  
    }

	@Override
	public Set<byte[]> keys(JedisCluster jedisCluster, byte[] pattern) {
		// TODO Auto-generated method stub
		Set<byte[]> keys = new TreeSet<>();  
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();  
        for(String k : clusterNodes.keySet()){  
             
            JedisPool jp = clusterNodes.get(k);  
            Jedis connection = jp.getResource();  
            try {  
                keys.addAll(connection.keys(pattern));  
            } catch(Exception e){  
            	e.printStackTrace();
            } finally{  
                   
                connection.close();//用完一定要close这个链接！！！  
            }  
        }  
       
        return keys;  
	}  

}
