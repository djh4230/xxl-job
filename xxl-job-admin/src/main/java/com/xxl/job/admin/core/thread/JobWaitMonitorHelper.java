package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.schedule.XxlJobDynamicScheduler;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.admin.core.util.MailUtil;
import com.xxl.job.admin.service.impl.XxlJobServiceImpl;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * job monitor instance
 * @author xuxueli 2015-9-1 18:05:56
 */
public class JobWaitMonitorHelper {
	private static Logger logger = LoggerFactory.getLogger(JobWaitMonitorHelper.class);
	
	private static JobWaitMonitorHelper instance = new JobWaitMonitorHelper();
	public static JobWaitMonitorHelper getInstance(){
		return instance;
	}

	// ---------------------- monitor ----------------------

	private LinkedBlockingQueue<XxlJobInfo> queue = new LinkedBlockingQueue<XxlJobInfo>(0xfff8);

	private Thread monitorThread;
	private volatile boolean toStop = false;
	public void start(){
		monitorThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// monitor
				while (!toStop) {
					try {
						List<XxlJobInfo> waitJobIdList = new ArrayList<XxlJobInfo>();
						int drainToNum = JobWaitMonitorHelper.instance.queue.drainTo(waitJobIdList);

						if (CollectionUtils.isNotEmpty(waitJobIdList)) {
							for (XxlJobInfo jobInfo : waitJobIdList) {
								if (jobInfo==null || jobInfo.getId()==0) {
									continue;
								}
								boolean flag=true;
								String[] parentIds=jobInfo.getChildJobId().split(",");
								for(String id:parentIds){
									XxlJobLog log = XxlJobDynamicScheduler.xxlJobLogDao.loadByJobId(Integer.valueOf(id));
									if(IJobHandler.SUCCESS.getCode()==log.getHandleCode()){
										continue;
									}else {
										flag=false;
										break;
									}
								}
								if(!flag){
									put(jobInfo);
								}else{
									remove(jobInfo);
									/**
									 * trigger job
									 */
									XxlJobTrigger.trigger(jobInfo.getId());
								}
							}
						}

						TimeUnit.SECONDS.sleep(10);
					} catch (Exception e) {
						logger.error("job monitor error:{}", e);
					}
				}

				// monitor all clear
			}
		});
		monitorThread.setDaemon(true);
		monitorThread.start();
	}

	public void toStop(){
		toStop = true;
		// interrupt and wait
		monitorThread.interrupt();
		try {
			monitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	// producer
	public static void put(XxlJobInfo jobInfo){
		getInstance().queue.offer(jobInfo);
	}
	public static void remove(XxlJobInfo jobInfo){
		getInstance().queue.remove(jobInfo);
	}

}
