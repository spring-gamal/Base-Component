package com.common.tools;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
@Component
public class ThreadLocalUtils<T>
{
    private ThreadLocal<T> loc = new ThreadLocal<T>();

    public void init(T t)
    {
        loc.set(t);
    }

    public Object get(String key)
    {
        T t = loc.get();
        if (t instanceof Map)
        {
            Map<Object, Object> m = (Map<Object, Object>) t;
            if (StringUtils.isNotBlank(key))
            {
                return m.get(key);
            }
            return m;
        }
        return null;
    }

    public T get()
    {
        return loc.get();
    }
}
