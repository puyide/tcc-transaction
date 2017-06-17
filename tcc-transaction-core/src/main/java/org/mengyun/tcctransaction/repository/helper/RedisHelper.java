package org.mengyun.tcctransaction.repository.helper;

import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.serializer.ObjectSerializer;
import org.mengyun.tcctransaction.utils.ByteUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.*;

/**
 * Created by changming.xie on 9/15/16.
 * 修改 puyide 2017-6-17 增加集群操作函数 
 */
public class RedisHelper {


    public static byte[] getRedisKey(String keyPrefix, Xid xid) {
        byte[] prefix = keyPrefix.getBytes();
        byte[] globalTransactionId = xid.getGlobalTransactionId();
        byte[] branchQualifier = xid.getBranchQualifier();

        byte[] key = new byte[prefix.length + globalTransactionId.length + branchQualifier.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(globalTransactionId, 0, key, prefix.length, globalTransactionId.length);
        System.arraycopy(branchQualifier, 0, key, prefix.length + globalTransactionId.length, branchQualifier.length);
        return key;
    }

    public static byte[] getKeyValue(JedisPool jedisPool, final byte[] key) {
        return execute(jedisPool, new JedisCallback<byte[]>() {
                    @Override
                    public byte[] doInJedis(Jedis jedis) {

                        Map<byte[], byte[]> fieldValueMap = jedis.hgetAll(key);

                        List<Map.Entry<byte[], byte[]>> entries = new ArrayList<Map.Entry<byte[], byte[]>>(fieldValueMap.entrySet());
                        Collections.sort(entries, new Comparator<Map.Entry<byte[], byte[]>>() {
                            @Override
                            public int compare(Map.Entry<byte[], byte[]> entry1, Map.Entry<byte[], byte[]> entry2) {
                                return (int) (ByteUtils.bytesToLong(entry1.getKey()) - ByteUtils.bytesToLong(entry2.getKey()));
                            }
                        });

                        if (entries.isEmpty())
                            return null;

                        byte[] content = entries.get(entries.size() - 1).getValue();

                        return content;
                    }
                }
        );
    }

    public static byte[] getKeyValue(JedisCluster jedisPool, final byte[] key) {
    	
        return execute(jedisPool, new JedisClusterCallback<byte[]>() {
        	
              

					@Override
					public byte[] doInJedis(JedisCluster jedis) {
						 Map<byte[], byte[]> fieldValueMap = jedis.hgetAll(key);

	                        List<Map.Entry<byte[], byte[]>> entries = new ArrayList<Map.Entry<byte[], byte[]>>(fieldValueMap.entrySet());
	                        Collections.sort(entries, new Comparator<Map.Entry<byte[], byte[]>>() {
	                            @Override
	                            public int compare(Map.Entry<byte[], byte[]> entry1, Map.Entry<byte[], byte[]> entry2) {
	                                return (int) (ByteUtils.bytesToLong(entry1.getKey()) - ByteUtils.bytesToLong(entry2.getKey()));
	                            }
	                        });

	                        if (entries.isEmpty())
	                            return null;

	                        byte[] content = entries.get(entries.size() - 1).getValue();

	                        return content;
					}
                }
        );
    }
    public static byte[] getKeyValue(Jedis jedis, final byte[] key) {

        Map<byte[], byte[]> fieldValueMap = jedis.hgetAll(key);

        List<Map.Entry<byte[], byte[]>> entries = new ArrayList<Map.Entry<byte[], byte[]>>(fieldValueMap.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<byte[], byte[]>>() {
            @Override
            public int compare(Map.Entry<byte[], byte[]> entry1, Map.Entry<byte[], byte[]> entry2) {
                return (int) (ByteUtils.bytesToLong(entry1.getKey()) - ByteUtils.bytesToLong(entry2.getKey()));
            }
        });

        if (entries.isEmpty())
            return null;

        byte[] content = entries.get(entries.size() - 1).getValue();

        return content;
    }

    public static <T> T execute(JedisPool jedisPool, JedisCallback<T> callback) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return callback.doInJedis(jedis);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
    public static <T> T execute(JedisCluster jedisPool, JedisClusterCallback<T> callback) {
        
        try {
             
            
            return callback.doInJedis(jedisPool);
        } finally {
            if (jedisPool != null) {
            	try {
					jedisPool.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
    }


}
