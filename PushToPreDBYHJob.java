package test.demo;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;



/**
 *  
 *  [功能详细描述]
 * @作者 lizp
 * @version [版本号, 2019年07月24日]
 * @see [相关类/方法]
 * @since [产品/模块版本] 
 */
@DisallowConcurrentExecution
public class ThreadDemo implements Job
{

    private ICommonDao baseDao = null;
   
    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        try {
            if (baseDao == null) {
                baseDao = CommonDao.getInstance();
            }
           
                ThreadPoolTaskExecutor threadPoolTaskExecutor = ThreadManager.getThreadPool();
                for (Record preData : preDataList) {
                    threadPoolTaskExecutor.execute(new B(preData));

                }
                while (true) {
                    try {
                        Thread.sleep(200);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //如果活跃的线程为0，则说明任务处理完毕
                    if (threadPoolTaskExecutor.getActiveCount() < 1) {
                        //销毁线程池
                        threadPoolTaskExecutor.destroy();
                        break;
                    }
                }

                
           
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            
        }
        finally {
            
            if (baseDao != null) {
                baseDao.close();
            }
            
        }

    }
    //推送业务数据
    public void runTreadForCataLog(Record preData, String batch) {
        
    }

    class B implements Runnable
    {
        private Record preData;
        private String batch;
        public B(Record preData,String batch) {
            this.preData = preData;
            this.batch = batch;
        }

        @Override
        public void run() {
            try {
                if (preData == null) {
                    
                    return;
                }
                runTreadForCataLog(preData, batch);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        

    }

}
