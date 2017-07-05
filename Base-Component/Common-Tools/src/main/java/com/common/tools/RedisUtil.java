package com.common.tools;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.Pool;
import redis.clients.util.SafeEncoder;

/**
 * Redis 客户端工具类
 * 
 * @date 2016-10-13
 */
@Component
public class RedisUtil implements InitializingBean
{

    private Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    // sentinels中间件连接地址
    private Set<String> sentinels = null;
    private int maxTotal = 500;
    private int maxWaitMillis = 10000;
    private int maxIdle = 50;
    private int minIdle = 20;
    private String masterName = "master";
    private int timeOut = 7000;
    // 数据源连接池
    private JedisSentinelPool pool;

    /**
     * 初始化连接参数
     */
    public void init()
    {
        try
        {
            if (CollectionUtils.isEmpty(sentinels))
            {
                throw new NullPointerException("Redis Sentinels is empty! More than one sentinels must seperated by '|'");
            }

            // Set<String> sentinel = new HashSet<String>(Arrays.asList(sentinelArr));
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            // JedisPoolConfig poolConfig = new JedisPoolConfig();
            // 最大连接数
            poolConfig.setMaxTotal(maxTotal);
            poolConfig.setMaxWaitMillis(maxWaitMillis);
            // poolConfig.setTestOnBorrow(true);
            // 最大空闲连接数
            poolConfig.setMaxIdle(maxIdle);
            poolConfig.setMinIdle(minIdle);
            // 在空闲时检查有效性, 默认false
            poolConfig.setTestWhileIdle(false);
            // 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
            poolConfig.setBlockWhenExhausted(true);
            // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,
            // 默认-1
            // poolConfig.setMaxWaitMillis(-1);
            // 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
            poolConfig.setMinEvictableIdleTimeMillis(1800000);
            // 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
            poolConfig.setTimeBetweenEvictionRunsMillis(-1);
            // 对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数
            // 时直接逐出,不再根据MinEvictableIdleTimeMillis判断 (默认逐出策略)
            // poolConfig.setSoftMinEvictableIdleTimeMillis(1800000);
            // 读取超时时间
            pool = new JedisSentinelPool(masterName, this.sentinels, poolConfig, timeOut);
        }
        catch (Exception e)
        {
            logger.error("redis客服端连接初始化异常," + e.getMessage(), e);
        }
    }

    public RedisUtil()
    {

    }

    /**
     * 获取不到redis连接返回Null
     * 
     * @return
     */
    private Jedis getJedis()
    {
        Jedis jedis = null;
        if (null != pool)
        {
            jedis = pool.getResource();
        }
        return jedis;
    }

    /**
     * 清除所有key.
     * 
     * @Title: flushAll
     * @return
     * @throws @author:
     *             yong
     * @date: 2012-12-19下午01:21:08
     */
    public String flushAll()
    {
        String flash = "";
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            flash = jedis.flushAll();
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return flash;
    }

    /**
     * 更改key名
     * 
     * @param String
     *            oldkey
     * @param String
     *            newkey
     * @return 状态码
     */
    public String rename(String oldkey, String newkey)
    {
        return rename(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey));
    }

    /**
     * 更改key,仅当新key不存在时才执行
     * 
     * @param String
     *            oldkey
     * @param String
     *            newkey
     * @return 状态码
     */
    public long renamenx(String oldkey, String newkey)
    {
        long status = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            status = jedis.renamenx(oldkey, newkey);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return status;
    }

    /**
     * 更改key 名
     * 
     * @param String
     *            oldkey
     * @param String
     *            newkey
     * @return 状态码
     */
    public String rename(byte[] oldkey, byte[] newkey)
    {
        String status = "";
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            status = jedis.rename(oldkey, newkey);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return status;
    }

    /**
     * 设置key的过期时间，以秒为单位
     * 
     * @param String
     *            key
     * @param seconds
     *            过期时间,已秒为单位
     * @return 影响的记录数
     */
    public long expired(String key, int seconds)
    {
        long count = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.expire(key, seconds);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return count;
    }

    /**
     * 设置key的过期时间,它是距历元（即格林威治标准时间 1970 年 1 月 1 日的 00:00:00，格里高利历）的偏移量。
     * 
     * @param String
     *            key
     * @param 时间
     *            ,已秒为单位
     * @return 影响的记录数
     */
    public long expireAt(String key, long timestamp)
    {
        long count = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.expireAt(key, timestamp);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return count;
    }

    /**
     * 查询key的过期时间
     * 
     * @param String
     *            key
     * @return 以秒为单位的时间表示
     */
    public long ttl(String key)
    {
        long len = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.ttl(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 取消对key过期时间的设置
     * 
     * @param key
     * @return 影响的记录数
     */
    public long persist(String key)
    {
        long count = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.persist(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return count;
    }

    /**
     * 删除keys对应的记录,可以是多个key
     * 
     * @param String
     *            ... keys
     * @return 删除的记录数
     */
    public long del(String... keys)
    {
        long count = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.del(keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return count;
    }

    /**
     * 删除keys对应的记录,可以是多个key
     * 
     * @param String
     *            ... keys
     * @return 删除的记录数
     */
    public long del(byte[]... keys)
    {
        long count = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.del(keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return count;
    }

    /**
     * 判断key是否存在
     * 
     * @param String
     *            key
     * @return boolean
     */
    public boolean exists(String key)
    {
        boolean exis = false;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            exis = jedis.exists(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return exis;
    }

    /**
     * 对List,Set,SortSet进行排序,如果集合数据较大应避免使用这个方法
     * 
     * @param String
     *            key
     * @return List<String> 集合的全部记录
     **/
    public List<String> sort(String key)
    {
        List<String> list = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            list = jedis.sort(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return list;
    }

    /**
     * 对List,Set,SortSet进行排序或limit
     * 
     * @param String
     *            key
     * @param SortingParams
     *            parame 定义排序类型或limit的起止位置.
     * @return List<String> 全部或部分记录
     **/
    public List<String> sort(String key, SortingParams parame)
    {
        List<String> list = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            list = jedis.sort(key, parame);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return list;
    }

    /**
     * 返回指定key存储的类型
     * 
     * @param String
     *            key
     * @return String string|list|set|zset|hash
     **/
    public String type(String key)
    {
        String type = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            type = jedis.type(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return type;
    }

    /**
     * 查找所有匹配给定的模式的键
     * 
     * @param String
     *            key的表达式,*表示多个，？表示一个
     */
    public Set<String> keys(String pattern)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.keys(pattern);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 从hash中删除指定的存储
     * 
     * @param String
     *            key
     * @param String
     *            fieid 存储的名字
     * @return 状态码，1成功，0失败
     */
    public long hdel(String key, String fieid)
    {
        long s = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hdel(key, fieid);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 移除给定的一个key。
     * 
     * @Title: hdel
     * @param key
     * @return
     * @author: yong
     * @date: 2013-1-15下午03:20:58
     */
    public long hdel(String key)
    {
        long s = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.del(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return s;
    }

    /**
     * 测试hash中指定的存储是否存在
     * 
     * @param String
     *            key
     * @param String
     *            fieid 存储的名字
     * @return 1存在，0不存在
     */
    public boolean hexists(String key, String fieid)
    {
        boolean s = false;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hexists(key, fieid);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 返回hash中指定存储位置的值
     * 
     * @param String
     *            key
     * @param String
     *            fieid 存储的名字
     * @return 存储对应的值
     */
    public String hget(String key, String field)
    {

        String s = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hget(key, field);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 以Map的形式返回hash中的存储和值
     * 
     * @param String
     *            key
     * @return Map<Strinig,String>
     */
    public Map<String, String> hgetall(String key)
    {
        Map<String, String> map = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            map = jedis.hgetAll(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return map;
    }

    /**
     * 在指定的存储位置加上指定的数字，存储位置的值必须可转为数字类型
     * 
     * @param String
     *            key
     * @param String
     *            fieid 存储位置
     * @param String
     *            long value 要增加的值,可以是负数
     * @return 增加指定数字后，存储位置的值
     */
    public Long hincrby(String key, String fieid, long value)
    {
        Long s = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hincrBy(key, fieid, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 返回指定hash中的所有存储名字,类似Map中的keySet方法
     * 
     * @param String
     *            key
     * @return Set<String> 存储名称的集合
     */
    public Set<String> hkeys(String key)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.hkeys(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return set;
    }

    /**
     * 获取hash中存储的个数，类似Map中size方法
     * 
     * @param String
     *            key
     * @return long 存储的个数
     */
    public long hlen(String key)
    {
        long len = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.hlen(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return len;
    }

    /**
     * 根据多个key，获取对应的value，返回List,如果指定的key不存在,List对应位置为null
     * 
     * @param String
     *            key
     * @param String
     *            ... fieids 存储位置
     * @return List<String>
     */
    public List<String> hmget(String key, String... fieids)
    {
        List<String> list = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            list = jedis.hmget(key, fieids);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return list;
    }

    /**
     * 添加对应关系，如果对应关系已存在，则覆盖
     * 
     * @param Strin
     *            key
     * @param Map
     *            <String,String> 对应关系
     * @return 状态，成功返回OK
     */
    public String hmset(String key, Map<String, String> map)
    {
        String s = "";
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hmset(key, map);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return s;
    }

    /**
     * 添加一个对应关系
     * 
     * @param String
     *            key
     * @param String
     *            fieid
     * @param String
     *            value
     * @return 状态码 1成功，0失败，fieid已存在将更新，也返回0
     **/
    public long hset(String key, String fieid, String value)
    {
        long s = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hset(key, fieid, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            closeResource(jedis, broken);
        }
        return s;
    }

    /**
     * 添加对应关系，只有在fieid不存在时才执行
     * 
     * @param String
     *            key
     * @param String
     *            fieid
     * @param String
     *            value
     * @return 状态码 1成功，0失败fieid已存
     **/
    public long hsetnx(String key, String fieid, String value)
    {
        long s = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.hsetnx(key, fieid, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return s;
    }

    /**
     * 获取hash中value的集合
     * 
     * @param String
     *            key
     * @return List<String>
     */
    public List<String> hvals(String key)
    {
        List<String> list = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            list = jedis.hvals(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return list;
    }

    /**
     * List长度
     * 
     * @param String
     *            key
     * @return 长度
     */
    public long llen(String key)
    {
        long len = llen(SafeEncoder.encode(key));
        return len;
    }

    /**
     * List长度
     * 
     * @param byte[]
     *            key
     * @return 长度
     */
    public long llen(byte[] key)
    {
        long count = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.llen(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return count;
    }

    /**
     * 覆盖操作,将覆盖List中指定位置的值
     * 
     * @param byte[]
     *            key
     * @param int
     *            index 位置
     * @param byte[]
     *            value 值
     * @return 状态码
     */
    public String lset(byte[] key, int index, byte[] value)
    {
        String status = "";
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            status = jedis.lset(key, index, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return status;
    }

    /**
     * 覆盖操作,将覆盖List中指定位置的值
     * 
     * @param key
     * @param int
     *            index 位置
     * @param String
     *            value 值
     * @return 状态码
     */
    public String lset(String key, int index, String value)
    {
        String lset = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            lset = jedis.lset(key, index, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return lset;
    }

    /**
     * 在value的相对位置插入记录
     * 
     * @param key
     * @param LIST_POSITION
     *            前面插入或后面插入
     * @param String
     *            pivot 相对位置的内容
     * @param String
     *            value 插入的内容
     * @return 记录总数
     */
    public long linsert(String key, LIST_POSITION where, String pivot, String value)
    {
        long linsert = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            linsert = jedis.linsert(key, where, pivot, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return linsert;
    }

    /**
     * 在指定位置插入记录
     * 
     * @param String
     *            key
     * @param LIST_POSITION
     *            前面插入或后面插入
     * @param byte[]
     *            pivot 相对位置的内容
     * @param byte[]
     *            value 插入的内容
     * @return 记录总数
     */
    public long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value)
    {
        long count = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.linsert(key, where, pivot, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return count;
    }

    /**
     * 获取List中指定位置的值
     * 
     * @param String
     *            key
     * @param int
     *            index 位置
     * @return 值
     **/
    public String lindex(String key, int index)
    {
        String encode = SafeEncoder.encode(lindex(SafeEncoder.encode(key), index));
        return encode;
    }

    /**
     * 获取List中指定位置的值
     * 
     * @param byte[]
     *            key
     * @param int
     *            index 位置
     * @return 值
     **/
    public byte[] lindex(byte[] key, int index)
    {
        byte[] value = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            value = jedis.lindex(key, index);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return value;
    }

    /**
     * 将List中的第一条记录移出List
     * 
     * @param String
     *            key
     * @return 移出的记录
     */
    public String lpop(String key)
    {
        String encode = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            encode = jedis.lpop(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return encode;
    }

    /**
     * 将List中的第一条记录移出List
     * 
     * @param byte[]
     *            key
     * @return 移出的记录
     */
    public byte[] lpop(byte[] key)
    {
        byte[] value = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            value = jedis.lpop(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return value;
    }

    /**
     * 将List中最后第一条记录移出List
     * 
     * @param byte[]
     *            key
     * @return 移出的记录
     */
    public String rpop(String key)
    {
        String value = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            value = jedis.rpop(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return value;
    }

    /**
     * 向List尾部追加记录
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return 记录总数
     */
    public long lpush(String key, String value)
    {
        long lpush = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            lpush = jedis.lpush(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return lpush;
    }

    /**
     * 向List尾部追加记录
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return 记录总数
     */
    public long lpush(String key, String... value)
    {
        Long lpush = -1l;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            lpush = jedis.lpush(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return lpush;
    }

    /**
     * 向List头部追加记录
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return 记录总数
     */
    public long rpush(String key, String value)
    {
        long count = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.rpush(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return count;
    }

    /**
     * 向List中追加记录
     * 
     * @param byte[]
     *            key
     * @param byte[]
     *            value
     * @return 记录总数
     */
    public long lpush(byte[] key, byte[]... value)
    {
        long count = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.lpush(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return count;
    }

    /**
     * 获取指定范围的记录，可以做为分页使用
     * 
     * @param String
     *            key
     * @param long
     *            start
     * @param long
     *            end
     * @return List
     */
    public List<String> lrange(String key, long start, long end)
    {
        List<String> list = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            list = jedis.lrange(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return list;
    }

    /**
     * 获取指定范围的记录，可以做为分页使用
     * 
     * @param byte[]
     *            key
     * @param int
     *            start
     * @param int
     *            end 如果为负数，则尾部开始计算
     * @return List
     */
    public List<byte[]> lrange(byte[] key, int start, int end)
    {
        List<byte[]> list = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            list = jedis.lrange(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return list;
    }

    /**
     * 删除List中c条记录，被删除的记录值为value
     * 
     * @param byte[]
     *            key
     * @param int
     *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
     * @param byte[]
     *            value 要匹配的值
     * @return 删除后的List中的记录数
     */
    public long lrem(byte[] key, int c, byte[] value)
    {
        long count = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            count = jedis.lrem(key, c, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return count;
    }

    /**
     * 删除List中c条记录，被删除的记录值为value
     * 
     * @param String
     *            key
     * @param int
     *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
     * @param String
     *            value 要匹配的值
     * @return 删除后的List中的记录数
     */
    public long lrem(String key, int c, String value)
    {
        long lrem = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            lrem = lrem(SafeEncoder.encode(key), c, SafeEncoder.encode(value));
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return lrem;
    }

    /**
     * 算是删除吧，只保留start与end之间的记录
     * 
     * @param byte[]
     *            key
     * @param int
     *            start 记录的开始位置(0表示第一条记录)
     * @param int
     *            end 记录的结束位置（如果为-1则表示最后一个，-2，-3以此类推）
     * @return 执行状态码
     */
    public String ltrim(byte[] key, int start, int end)
    {
        String str = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.ltrim(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 算是删除，只保留start与end之间的记录
     * 
     * @param String
     *            key
     * @param int
     *            start 记录的开始位置(0表示第一条记录)
     * @param int
     *            end 记录的结束位置（如果为-1则表示最后一个，-2，-3以此类推）
     * @return 执行状态码
     */
    public String ltrim(String key, int start, int end)
    {
        String ltrim = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            ltrim = jedis.ltrim(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return ltrim;
    }

    /**
     * 向Set添加一条记录，如果member已存在返回0,否则返回1,-1失败
     * 
     * @param String
     *            key
     * @param String
     *            member
     * @return 操作码,0或1
     */
    public long sadd(String key, String member)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.sadd(key, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 获取给定key中元素个数
     * 
     * @param String
     *            key
     * @return 元素个数
     */
    public long scard(String key)
    {
        long len = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.scard(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 返回从第一组和所有的给定集合之间的差异的成员
     * 
     * @param String
     *            ... keys
     * @return 差异的成员集合
     */
    public Set<String> sdiff(String... keys)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.sdiff(keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return set;
    }

    /**
     * 这个命令等于sdiff,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
     * 
     * @param String
     *            newkey 新结果集的key
     * @param String
     *            ... keys 比较的集合
     * @return 新集合中的记录数
     **/
    public long sdiffstore(String newkey, String... keys)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.sdiffstore(newkey, keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 返回给定集合交集的成员,如果其中一个集合为不存在或为空，则返回空Set
     * 
     * @param String
     *            ... keys
     * @return 交集成员的集合
     **/
    public Set<String> sinter(String... keys)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.sinter(keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 这个命令等于sinter,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
     * 
     * @param String
     *            newkey 新结果集的key
     * @param String
     *            ... keys 比较的集合
     * @return 新集合中的记录数
     **/
    public long sinterstore(String newkey, String... keys)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.sinterstore(newkey, keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 确定一个给定的值是否存在
     * 
     * @param String
     *            key
     * @param String
     *            member 要判断的值
     * @return 存在返回1，不存在返回0
     **/
    public boolean sismember(String key, String member)
    {
        boolean s = false;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.sismember(key, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 返回集合中的所有成员
     * 
     * @param String
     *            key
     * @return 成员集合
     */
    public Set<String> smembers(String key)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.smembers(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 将成员从源集合移出放入目标集合 <br/>
     * 如果源集合不存在或不包哈指定成员，不进行任何操作，返回0<br/>
     * 否则该成员从源集合上删除，并添加到目标集合，如果目标集合中成员已存在，则只在源集合进行删除
     * 
     * @param String
     *            srckey 源集合
     * @param String
     *            dstkey 目标集合
     * @param String
     *            member 源集合中的成员
     * @return 状态码，1成功，0失败
     */
    public long smove(String srckey, String dstkey, String member)
    {
        long s = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.smove(srckey, dstkey, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 从集合中删除成员
     * 
     * @param String
     *            key
     * @return 被删除的成员
     */
    public String spop(String key)
    {
        String s = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.spop(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 从集合中删除指定成员
     * 
     * @param String
     *            key
     * @param String
     *            member 要删除的成员
     * @return 状态码，成功返回1，成员不存在返回0,-1失败
     */
    public long srem(String key, String member)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.srem(key, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 合并多个集合并返回合并后的结果，合并后的结果集合并不保存<br/>
     * 
     * @param String
     *            ... keys
     * @return 合并后的结果集合
     * @see sunionstore
     */
    public Set<String> sunion(String... keys)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.sunion(keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 合并多个集合并将合并后的结果集保存在指定的新集合中，如果新集合已经存在则覆盖
     * 
     * @param String
     *            newkey 新集合的key
     * @param String
     *            ... keys 要合并的集合
     **/
    public long sunionstore(String newkey, String... keys)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.sunionstore(newkey, keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 向集合中增加一条记录,如果这个值已存在，这个值对应的权重将被置为新的权重
     * 
     * @param String
     *            key
     * @param double
     *            score 权重
     * @param String
     *            member 要加入的值，
     * @return 状态码 1成功，0已存在member的值
     */
    public long zadd(String key, double score, String member)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.zadd(key, score, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 获取集合中元素的数量
     * 
     * @param String
     *            key
     * @return 如果返回0则集合不存在
     */
    public long zcard(String key)
    {
        long len = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.zcard(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 获取指定权重区间内集合的数量
     * 
     * @param String
     *            key
     * @param double
     *            min 最小排序位置
     * @param double
     *            max 最大排序位置
     */
    public long zcount(String key, double min, double max)
    {
        long len = 0;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.zcount(key, min, max);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 权重增加给定值，如果给定的member已存在
     * 
     * @param String
     *            key
     * @param double
     *            score 要增的权重
     * @param String
     *            member 要插入的值
     * @return 增后的权重
     */
    public Double zincrby(String key, double score, String member)
    {
        Double s = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.zincrby(key, score, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 返回指定位置的集合元素,0为第一个元素，-1为最后一个元素
     * 
     * @param String
     *            key
     * @param int
     *            start 开始位置(包含)
     * @param int
     *            end 结束位置(包含)
     * @return Set<String>
     */
    public Set<String> zrange(String key, int start, int end)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.zrange(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 返回指定权重区间的元素集合
     * 
     * @param String
     *            key
     * @param double
     *            min 上限权重
     * @param double
     *            max 下限权重
     * @return Set<String>
     */
    public Set<String> zrangeByScore(String key, double min, double max)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.zrangeByScore(key, min, max);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 获取指定值在集合中的位置，集合排序从低到高
     * 
     * @see zrevrank
     * @param String
     *            key
     * @param String
     *            member
     * @return long 位置
     */
    public long zrank(String key, String member)
    {
        long index = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            index = jedis.zrank(key, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return index;
    }

    /**
     * 获取指定值在集合中的位置，集合排序从低到高
     * 
     * @see zrank
     * @param String
     *            key
     * @param String
     *            member
     * @return long 位置
     */
    public long zrevrank(String key, String member)
    {
        long index = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            index = jedis.zrevrank(key, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return index;
    }

    /**
     * 从集合中删除成员
     * 
     * @param String
     *            key
     * @param String
     *            member
     * @return 返回1成功
     */
    public long zrem(String key, String member)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.zrem(key, member);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return s;
    }

    /**
     * 删除
     * 
     * @param key
     * @return
     */
    public long zrem(String key)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.del(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 删除给定位置区间的元素
     * 
     * @param String
     *            key
     * @param int
     *            start 开始区间，从0开始(包含)
     * @param int
     *            end 结束区间,-1为最后一个元素(包含)
     * @return 删除的数量
     */
    public long zremrangeByRank(String key, int start, int end)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.zremrangeByRank(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }
        return s;
    }

    /**
     * 删除给定权重区间的元素
     * 
     * @param String
     *            key
     * @param double
     *            min 下限权重(包含)
     * @param double
     *            max 上限权重(包含)
     * @return 删除的数量
     */
    public long zremrangeByScore(String key, double min, double max)
    {
        long s = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            s = jedis.zremrangeByScore(key, min, max);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return s;
    }

    /**
     * 获取给定区间的元素，原始按照权重由高到低排序
     * 
     * @param String
     *            key
     * @param int
     *            start
     * @param int
     *            end
     * @return Set<String>
     */
    public Set<String> zrevrange(String key, int start, int end)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.zrevrange(key, start, end);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 获取给定值在集合中的权重
     * 
     * @param String
     *            key
     * @param memeber
     * @return double 权重
     */
    public double zscore(String key, String memebr)
    {
        Double score = -1d;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            score = jedis.zscore(key, memebr);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return score;
    }

    /**
     * 根据key获取记录
     * 
     * @param String
     *            key
     * @return 值
     */
    public String get(String key)
    {
        String value = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            value = jedis.get(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            closeResource(jedis, broken);
        }

        return value;
    }

    /**
     * 根据key获取记录
     * 
     * @param byte[]
     *            key
     * @return 值
     */
    public byte[] get(byte[] key)
    {
        byte[] value = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            value = jedis.get(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return value;
    }

    /**
     * 添加有过期时间的记录
     * 
     * @param String
     *            key
     * @param int
     *            seconds 过期时间，以秒为单位
     * @param String
     *            value
     * @return String 操作状态
     */
    public String setEx(String key, int seconds, String value)
    {
        String str = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.setex(key, seconds, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 添加有过期时间的记录
     * 
     * @param String
     *            key
     * @param int
     *            seconds 过期时间，以秒为单位
     * @param String
     *            value
     * @return String 操作状态
     */
    public String setEx(byte[] key, int seconds, byte[] value)
    {
        String str = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.setex(key, seconds, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 添加一条记录，仅当给定的key不存在时才插入
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return long 状态码，1插入成功且key不存在，0未插入，key存在
     */
    public long setnx(String key, String value)
    {
        long str = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.setnx(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 添加记录,如果记录已存在将覆盖原有的value
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return 状态码
     */
    public String set(String key, String value)
    {
        String set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.set(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            closeResource(jedis, broken);
        }

        return set;
    }

    /**
     * 添加记录,如果记录已存在将覆盖原有的value
     * 
     * @param byte[]
     *            key
     * @param byte[]
     *            value
     * @return 状态码
     */
    public String set(byte[] key, byte[] value)
    {
        String status = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            status = jedis.set(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            closeResource(jedis, broken);
        }

        return status;
    }

    /**
     * 设置对象到指定key
     * 
     * @Title: setObject
     * @param keyStr
     * @param obj
     * @throws @author:
     *             yong
     * @date: 2012-12-20下午04:48:42
     */
    public String setObject(String keyStr, Object obj)
    {
        return this.set(keyStr.getBytes(), SerializeUtil.serialize(obj));
    }

    /**
     * 设置对象到指定key,并指定过期时间
     * 
     * @Title: setObject
     * @param keyStr
     * @param expire
     *            过期时间
     * @param obj
     * @throws @author:
     *             yong
     * @date: 2012-12-20下午04:47:09
     */
    public String setObject(String keyStr, int expire, Object obj)
    {
        return this.setEx(keyStr.getBytes(), expire, SerializeUtil.serialize(obj));
    }

    /**
     * 跟据key获取对象
     * 
     * @Title: getObject
     * @param keys
     * @return
     * @throws @author:
     *             yong
     * @date: 2012-12-20下午04:45:53
     */
    public Object getObject(String keyStr)
    {
        byte[] o = this.get(keyStr.getBytes());
        if (null == o)
        {
            return null;
        }
        return SerializeUtil.unserialize(o);
    }

    /**
     * 从指定位置开始插入数据，插入的数据会覆盖指定位置以后的数据<br/>
     * 例:String str1="123456789";<br/>
     * 对str1操作后setRange(key,4,0000)，str1="123400009";
     * 
     * @param String
     *            key
     * @param long
     *            offset
     * @param String
     *            value
     * @return long value的长度
     */
    public long setRange(String key, long offset, String value)
    {
        long len = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.setrange(key, offset, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 在指定的key中追加value
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return long 追加后value的长度
     **/
    public long append(String key, String value)
    {
        long len = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.append(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 将key对应的value减去指定的值，只有value可以转为数字时该方法才可用
     * 
     * @param String
     *            key
     * @param long
     *            number 要减去的值
     * @return long 减指定值后的值
     */
    public Long decrBy(String key, long number)
    {
        Long len = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.decrBy(key, number);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * <b>可以作为获取唯一id的方法</b><br/>
     * 将key对应的value加上指定的值，只有value可以转为数字时该方法才可用
     * 
     * @param String
     *            key
     * @param long
     *            number 要减去的值
     * @return long 相加后的值
     */
    public Long incrBy(String key, long number)
    {
        Long len = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.incrBy(key, number);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 对指定key对应的value进行截取
     * 
     * @param String
     *            key
     * @param long
     *            startOffset 开始位置(包含)
     * @param long
     *            endOffset 结束位置(包含)
     * @return String 截取的值
     */
    public String getrange(String key, long startOffset, long endOffset)
    {
        String value = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            value = jedis.getrange(key, startOffset, endOffset);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return value;
    }

    /**
     * 获取并设置指定key对应的value<br/>
     * 如果key存在返回之前的value,否则返回null
     * 
     * @param String
     *            key
     * @param String
     *            value
     * @return String 原始value或null
     */
    public String getSet(String key, String value)
    {
        String str = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.getSet(key, value);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 批量获取记录,如果指定的key不存在返回List的对应位置将是null
     * 
     * @param String
     *            keys
     * @return List<String> 值得集合
     */
    public List<String> mget(String... keys)
    {
        List<String> str = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.mget(keys);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 批量存储记录
     * 
     * @param String
     *            keysvalues 例:keysvalues="key1","value1","key2","value2";
     * @return String 状态码
     */
    public String mset(String... keysvalues)
    {
        String str = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            str = jedis.mset(keysvalues);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return str;
    }

    /**
     * 获取key对应的值的长度
     * 
     * @param String
     *            key
     * @return value值得长度
     **/
    public long strlen(String key)
    {
        long len = -1;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            len = jedis.strlen(key);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return len;
    }

    /**
     * 使用redis管道执行特定操作(常用于批量处理)
     * 
     * @Title: PipelineExecute
     * @param pipelineExecute
     * @return
     * @throws @author:
     *             yong
     * @date: 2012-12-21下午02:52:53
     */
    public List<Object> pipelineExecute(PipelineExecute pipelineExecute)
    {
        List<Object> backResult = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            Client client = jedis.getClient();
            pipelineExecute.setClient(client);
            try
            {
                pipelineExecute.execute();
                // pipelineExecute.sync();
                backResult = pipelineExecute.syncAndReturnAll();
            }
            catch (Exception ex)
            {
                broken = handleJedisException(ex);
                throw ex;
            }
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return backResult;
    }

    /**
     * 执行redis操作并对事务进行控制
     * 
     * @Title: execute
     * @param redisExecute
     * @return
     * @throws @author:
     *             yong
     * @date: 2012-12-20下午12:35:45
     */
    public List<Object> execute(RedisTransactionExecute redisExecute)
    {
        List<Object> backResult = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            Client client = jedis.getClient();
            redisExecute.setClient(client);
            try
            {
                client.multi();
                redisExecute.execute();
                backResult = redisExecute.exec();
            }
            catch (Exception ex)
            {
                backResult = null;
                redisExecute.discard();
                broken = handleJedisException(ex);
                throw ex;
            }
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return backResult;
    }

    /**
     * 开启对keys监控,并对key的操作进行事务管理
     * 
     * @Title: watchExecute
     * @param redisExecute
     * @param keys
     * @return
     * @throws @author:
     *             yong
     * @date: 2012-12-21上午01:33:19
     */
    public List<Object> watchExecute(RedisTransactionExecute redisExecute, byte[]... keys)
    {
        List<Object> backResult = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            Client client = jedis.getClient();
            redisExecute.setClient(client);
            try
            {
                client.watch(keys);
                client.multi();
                redisExecute.execute();
                backResult = redisExecute.exec();
            }
            catch (Exception ex)
            {
                backResult = null;
                redisExecute.discard();
                broken = handleJedisException(ex);
                throw ex;
            }
            finally
            {
                client.unwatch();
            }
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return backResult;
    }

    /**
     * 开启对keys监控,并对key的操作进行事务管理
     * 
     * @Title: watchExecute
     * @param redisExecute
     *            一个抽像类.实现execute方法
     * @param keys
     * @return
     * @throws @author:
     *             yong
     * @date: 2012-12-21上午01:35:59
     */
    public List<Object> watchExecute(RedisTransactionExecute redisExecute, String... keys)
    {
        List<Object> backResult = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            Client client = jedis.getClient();
            redisExecute.setClient(client);
            try
            {
                client.watch(keys);
                client.multi();
                redisExecute.execute();
                backResult = redisExecute.exec();
            }
            catch (Exception ex)
            {
                backResult = null;
                redisExecute.discard();
                broken = handleJedisException(ex);
                throw ex;
            }
            finally
            {
                client.unwatch();
            }
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return backResult;
    }

    /**
     * 关闭JEDIS数据库连接(返回连接池)
     * 
     * @param jedis
     */
    @SuppressWarnings(
    {
            "unchecked", "rawtypes"
    })
    public void closeJedis(Pool p, Object jedis)
    {
        if (null != p && null != jedis)
        {
            try
            {
                // 返回连接池
                p.returnResource(jedis);
            }
            catch (Exception e)
            {
                logger.error("关闭jedis数据库连接(返回连接池)异常，" + e.getMessage(), e);
            }
        }
    }

    /**
     * 释放redis对象
     * 
     * @param p
     * @param jedis
     * @throws @author:
     *             yong
     * @date: 2013-8-31下午02:12:21
     */
    @SuppressWarnings(
    {
            "unchecked", "rawtypes"
    })
    public void returnBrokenJedis(Pool p, Object jedis)
    {
        if (null != p && null != jedis)
        {
            try
            {
                // 释放redis对象
                p.returnBrokenResource(jedis);
            }
            catch (Exception e)
            {
                logger.error("释放redis对象异常," + e.getMessage(), e);
            }
        }
    }

    /** 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员 **/
    public Set<String> zrevrangeByScore(String key, Double max, Double min)
    {
        Set<String> set = null;
        Jedis jedis = null;
        boolean broken = false;
        try
        {
            jedis = getJedis();
            set = jedis.zrevrangeByScore(key, max, min);
        }
        catch (Exception e)
        {
            broken = handleJedisException(e);
            throw new RuntimeException(e);
        }
        finally
        {
            // returnJedis(jedis);
            closeResource(jedis, broken);
        }

        return set;
    }

    public Set<String> getSentinels()
    {
        return sentinels;
    }

    public void setSentinels(Set<String> sentinels)
    {
        this.sentinels = sentinels;
    }

    public int getMaxTotal()
    {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal)
    {
        this.maxTotal = maxTotal;
    }

    public int getMaxWaitMillis()
    {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(int maxWaitMillis)
    {
        this.maxWaitMillis = maxWaitMillis;
    }

    public int getMaxIdle()
    {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle)
    {
        this.maxIdle = maxIdle;
    }

    public String getMasterName()
    {
        return masterName;
    }

    public void setMasterName(String masterName)
    {
        this.masterName = masterName;
    }

    public int getTimeOut()
    {
        return timeOut;
    }

    public void setTimeOut(int timeOut)
    {
        this.timeOut = timeOut;
    }

    public int getMinIdle()
    {
        return minIdle;
    }

    public void setMinIdle(int minIdle)
    {
        this.minIdle = minIdle;
    }

    public void afterPropertiesSet() throws Exception
    {
        init();
    }

    /**
     * 判断jedis抛出的异常类型，网络异常或者一般异常
     * 
     * @param exception
     * @return
     */
    private boolean handleJedisException(Exception exception)
    {
        if (exception instanceof JedisConnectionException)
        {
            logger.error("Redis connection lost. Exception is : ", exception);
        }
        else if (exception instanceof JedisDataException)
        {
            if (exception.getMessage() != null && exception.getMessage().indexOf("READONLY") != -1)
            {
                logger.error("Redis connection are read-only slave. Exception is : ", exception);
            }
            else
            {
                return false;
            }
        }
        else
        {
            logger.error("Jedis exception happen. Exception is : ", exception);
        }
        return true;
    }

    /**
     * 关闭/是否连接池资源，若是网络异常，则断开连接，如果是一般异常，将直接抛出异常
     * 
     * @param jedis
     * @param conectionBroken
     */
    private void closeResource(Jedis jedis, boolean conectionBroken)
    {
        try
        {
            if (null != jedis && null != pool)
            {
                jedis.close();
            }
        }
        catch (Exception e)
        {
            logger.error("return back jedis failed, will force close the jedis.", e);
        }
    }

}
