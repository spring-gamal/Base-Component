package com.common.tools;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.InitializingBean;

public class ThreadPoolUtils implements InitializingBean
{
    public static final int HTREAD_POOL_DEFAULT_SIZE = 30;

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
