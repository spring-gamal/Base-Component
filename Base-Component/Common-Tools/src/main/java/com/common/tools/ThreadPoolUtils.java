package com.common.tools;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.InitializingBean;

/**
 * 线程池
 * 
 * @author zfzhu
 */
public class ThreadPoolUtils implements InitializingBean
{
    /**
     * 线程池默认大小
     */
    private static final int HTREAD_POOL_DEFAULT_SIZE = 30;

    /**
     * 线程池服务类
     */
    private static ExecutorService executorService;

    public void afterPropertiesSet() throws Exception
    {
        executorService = Executors.newFixedThreadPool(HTREAD_POOL_DEFAULT_SIZE);

    }

    public static ExecutorService getExecutorService()
    {
        return executorService;
    }
}
