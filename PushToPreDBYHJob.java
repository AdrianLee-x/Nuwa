package com.epoint.epointqlk.gxqlk.pushpredb.job;

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

import com.epoint.core.EpointFrameDsManager;
import com.epoint.core.dao.CommonDao;
import com.epoint.core.dao.ICommonDao;
import com.epoint.core.grammar.Record;
import com.epoint.core.utils.container.ContainerFactory;
import com.epoint.core.utils.log.LogUtil;
import com.epoint.core.utils.string.StringUtil;
import com.epoint.epointqlk.gxqlk.constant.SqlTableName;
import com.epoint.epointqlk.gxqlk.constants.GxConstants;
import com.epoint.epointqlk.gxqlk.pushpredb.api.IAuditPushPreDBService;
import com.epoint.epointqlk.gxqlk.utils.ThreadManager;
import com.epoint.epointqlk.util.QlkCodeItemsConstants.TaskType;

/**
 *  [事项数据上报定时器]
 *  [功能详细描述]
 * @作者 lizp
 * @version [版本号, 2019年07月24日]
 * @see [相关类/方法]
 * @since [产品/模块版本] 
 */
@DisallowConcurrentExecution
public class PushToPreDBYHJob implements Job
{

    /**
     * 日志
     */
    private Logger log = LogUtil.getLog(this.getClass());

    private ICommonDao baseDao = null;
    /**
     * 查询每次扫描的数据量
     */
    private static final String flowControlSql = "SELECT PUSH_PREDATA FROM AUDIT_DATAFLOW_CONTROL LIMIT 1";

    /**
     * 查询本次扫描的数据主键字段等
     */
    private static final String selectSql = "SELECT * FROM PUSH_PREDATA WHERE PRE_FLAG = '0' AND IFNULL(ERR_FLAG,'')=''  ORDER BY INSERTDATE LIMIT ?1";

    /**
     * 查询基本目录信息
     */
    private static final String selectCatalogSql = "SELECT * FROM AUDIT_CATALOG WHERE ROWGUID = ?1";
    /**
     * 查詢实施清单信息
     */
    private static final String selectItemSql = "SELECT * FROM AUDIT_ITEM WHERE ROWGUID = ?1";

    /**
     * 查询前置库的操作日志表名
     */
    private static final String selectPreTableSql = "SELECT * FROM AUDIT_PREDB_OPERATETABLE";

    /**
     * 更新待推送数据表的记录
     */
    private static final String updatePushPreData = "UPDATE PUSH_PREDATA SET PRE_FLAG = ?1 WHERE ROWGUID = ?2";

    private static final String selectbatchSql = "select * from pre_batchnumber where year = ?1 and month = ?2 and day = ?3 order by batchnumber desc limit 1";

    private static final String insertbatchSql = "insert into pre_batchnumber(rowguid,year,month,day,batchnumber) values(?1,?2,?3,?4,?5)";

    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        try {
            EpointFrameDsManager.begin(null);
            if (baseDao == null) {
                baseDao = CommonDao.getInstance();
            }
            log.info("@@@@@@@@@@@@@@@@@@@@@@@start推送前置库服务PushToPreDBJob开始执行@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

            Integer flowControlNum = baseDao.find(flowControlSql, Integer.class);
            if (flowControlNum == null) {
                flowControlNum = 50;
            }
            List<Record> preDataList = baseDao.findList(selectSql, Record.class, flowControlNum);
            if (preDataList == null || preDataList.isEmpty()) {
                log.info("@@@@@@@@@@@@@@@@@@@@@@@empty推送前置库服务PushToPreDBJob本次查询无数据@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            }
            else {

                Calendar now = Calendar.getInstance();
                String year = now.get(Calendar.YEAR) + "";
                String month = String.format("%02d", now.get(Calendar.MONTH) + 1);
                String day = String.format("%02d", now.get(Calendar.DAY_OF_MONTH));
                Record upn = baseDao.find(selectbatchSql, Record.class, new Object[] {year, month, day });
                String batch = "";
                String batchNumber = "00001";
                if (upn == null) {
                    // 批次号

                    batchNumber = "00001";
                    batch = year + month + day + "00001";

                }
                else {
                    Integer batchnum = Integer.parseInt(upn.getStr("batchnumber")) + 1;
                    batchNumber = batchnum.toString();
                    StringBuffer banum = null;
                    while (batchNumber.length() < 5) {
                        banum = new StringBuffer();
                        banum.append("0").append(batchNumber);
                        batchNumber = banum.toString();
                    }
                    batch = upn.getStr("year") + upn.getStr("month") + upn.getStr("day") + batchNumber;
                }
                baseDao.execute(insertbatchSql,
                        new Object[] {UUID.randomUUID().toString(), year, month, day, batchNumber });
                
                
                ThreadPoolTaskExecutor threadPoolTaskExecutor = ThreadManager.getThreadPool();
                for (Record preData : preDataList) {
                    threadPoolTaskExecutor.execute(new B(preData,batch));

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
                        //统计本批次推送量
                        IAuditPushPreDBService preService = ContainerFactory.getContainInfo()
                                .getComponent(IAuditPushPreDBService.class);
                        Map<String, Integer> nummap = preService.getAllnum(batch);
                        if (nummap != null && !nummap.isEmpty()) {
                            for (Map.Entry<String, Integer> entry : nummap.entrySet()) {
                                preService.insertUpDataReconciliation(entry, batch);
                            }
                        }
                        break;
                    }
                }

                
           
            }
            EpointFrameDsManager.commit();
        }
        catch (Exception e) {
            EpointFrameDsManager.rollback();
            e.printStackTrace();
            log.info("@@@@@@@@@@@@@@@@@@@@@@@error推送前置库服务PushToPreDBJob执行出错!!@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        }
        finally {
            EpointFrameDsManager.close();
            if (baseDao != null) {
                baseDao.close();
            }
            log.info("@@@@@@@@@@@@@@@@@@@@@@@finish推送前置库服务PushToPreDBJob执行结束@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        }

    }
    //推送业务数据
    public void runTreadForCataLog(Record preData, String batch) {
        ICommonDao baseDao = CommonDao.getInstance(); 
        try{
            String dataKind = preData.getStr("data_kind");
            String rowguid = preData.getStr("data_guid");
            String updateType = preData.getStr("update_type");
            if (GxConstants.DataKind.CATALOG.equals(dataKind)) {
                Record pushCatalog = baseDao.find(selectCatalogSql, Record.class, rowguid);
                new GxDirectoryPushPreDB().formatPreData(pushCatalog, updateType, batch);
            }
            else {
                Record pushItem = baseDao.find(selectItemSql, Record.class, rowguid);
                if (pushItem == null) {
                    log.error("@@@@@@@@@@@@@@@@@@@@@@@@@rowguid：" + rowguid
                            + "查出来的实施清单为空@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                    baseDao.execute(GxItemBasicPushPreDB.updatePushPreSql,
                            new Object[] {"查出来的实施清单为空", rowguid });
                    return;
                }
                String taskType = pushItem.getStr("task_type");
                String sqlTableName = "";
                //根据不同类型，对数据做不同的操作,插入不同前置库表
                if (StringUtil.isBlank(taskType)) {
                    return;
                }
                else if (TaskType.XZXK.equals(taskType) || TaskType.XZJF.equals(taskType)
                        || TaskType.XZQR.equals(taskType) || TaskType.XZJL.equals(taskType)
                        || TaskType.XZCJ.equals(taskType) || TaskType.XZQT.equals(taskType)) {
                    sqlTableName = SqlTableName.GENERAL_BASIC;
                }
                else if (TaskType.XZCF.equals(taskType)) {
                    sqlTableName = SqlTableName.PUNISH_BASIC;
                }
                else if (TaskType.XZZS.equals(taskType)) {
                    sqlTableName = SqlTableName.HANDLE_BASIC;
                }
                else if (TaskType.XZQZ.equals(taskType) || TaskType.XZJC.equals(taskType)) {
                    sqlTableName = SqlTableName.CHECK_BASIC;
                }
                else if (TaskType.GGFW.equals(taskType)) {
                    sqlTableName = SqlTableName.PUBLIC_BASIC;
                }
                new GxItemBasicPushPreDB().pushItemData(sqlTableName, pushItem, updateType, batch, preData);
            }
            insertOperateLogTable(preData);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            baseDao.close();
        }
        
    }

    /**
     *  插入操作日志记录表,并删除待推送数据
     *  @param preData  
     */
    public void insertOperateLogTable(Record preData) {
        ICommonDao dao = CommonDao.getInstance(); 
        try{
            List<Record> operateTableList = baseDao.findList(selectPreTableSql, Record.class);
            if (operateTableList != null) {
                for (Record operateTable : operateTableList) {
                    String operateTableName = operateTable.getStr("operate_tablename");
                    new GxPushPreDB().insertOperateLog(preData, operateTableName);
                }
            dao.execute(updatePushPreData, new Object[] {"1", preData.getStr("rowguid") });
        }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            dao.close();
        }
        
    }
    
    class B implements Runnable
    {
        private Record preData;
        private String batch;
        public B(Record vtaskcode,String batch) {
            this.preData = vtaskcode;
            this.batch = batch;
        }

        @Override
        public void run() {
            try {
                if (preData == null) {
                    System.out.println("没有taskcode数据");
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
