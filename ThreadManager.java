package com.epoint.epointqlk.gxqlk.utils;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
/**
 * 
 * @作者 lizp
 * @version [版本号, 2019年7月24日]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class ThreadManager
{
   
        
        /**
         * 返回一个初始化完成的线程池对象
         * @return
         */
        public static ThreadPoolTaskExecutor getThreadPool() {
            ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
            //最小线程数
            threadPoolTaskExecutor.setCorePoolSize(20);
            //最大线程数
            threadPoolTaskExecutor.setMaxPoolSize(2000);
            //空闲线程存活时间
            threadPoolTaskExecutor.setKeepAliveSeconds(1);
            //队列中最大线程
            threadPoolTaskExecutor.setQueueCapacity(2000);
            //初始化
            threadPoolTaskExecutor.initialize();
            return threadPoolTaskExecutor;
        }
     
     

}
