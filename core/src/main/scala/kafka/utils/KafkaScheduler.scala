/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.util.concurrent._
import atomic._
import org.apache.kafka.common.utils.KafkaThread

/**
 * A scheduler for running jobs
 * 
 * This interface controls a job scheduler that allows scheduling either repeating background jobs 
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
trait Scheduler {
  
  /**
   * Initialize this scheduler so it is ready to accept scheduling of tasks
   */
  def startup()
  
  /**
   * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur. 
   * This includes tasks scheduled with a delayed execution.
   */
  def shutdown()
  
  /**
   * Check if the scheduler has been started
   */
  def isStarted: Boolean
  
  /**
   * Schedule a task
   * @param name The name of this task
   * @param delay The amount of time to wait before the first execution
   * @param period The period with which to execute the task. If < 0 the task will execute only once.
   * @param unit The unit for the preceding times.
   */
  def schedule(name: String, fun: ()=>Unit, delay: Long = 0, period: Long = -1, unit: TimeUnit = TimeUnit.MILLISECONDS)
}

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * 
 * It has a pool of kafka-scheduler- threads that do the actual work.
 * 
 * @param threads The number of threads in the thread pool
 * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 */
// TODO: by zmyer
@threadsafe
class KafkaScheduler(val threads: Int, 
                     val threadNamePrefix: String = "kafka-scheduler-", 
                     daemon: Boolean = true) extends Scheduler with Logging {
  //调度线程池对象
  private var executor: ScheduledThreadPoolExecutor = null
  //调度线程id
  private val schedulerThreadId = new AtomicInteger(0)

  // TODO: by zmyer
  override def startup() {
    debug("Initializing task scheduler.")
    this synchronized {
      if(isStarted)
        throw new IllegalStateException("This scheduler has already been started!")
      //创建调度器
      executor = new ScheduledThreadPoolExecutor(threads)
      //设置关闭之后的定时任务执行策略
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
      //设置关闭之后延时任务执行策略
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      //设置调度器中的线程池对象
      executor.setThreadFactory(new ThreadFactory() {
                                  def newThread(runnable: Runnable): Thread = 
                                    new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon)
                                })
    }
  }

  // TODO: by zmyer
  override def shutdown() {
    debug("Shutting down task scheduler.")
    // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.
    val cachedExecutor = this.executor
    if (cachedExecutor != null) {
      this synchronized {
        //关闭调度器
        cachedExecutor.shutdown()
        this.executor = null
      }
      //等待关闭结束
      cachedExecutor.awaitTermination(1, TimeUnit.DAYS)
    }
  }

  // TODO: by zmyer
  def schedule(name: String, fun: ()=>Unit, delay: Long, period: Long, unit: TimeUnit) {
    debug("Scheduling task %s with initial delay %d ms and period %d ms."
        .format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
    this synchronized {
      ensureRunning
      //创建运行对象
      val runnable = CoreUtils.runnable {
        try {
          trace("Beginning execution of scheduled task '%s'.".format(name))
          fun()
        } catch {
          case t: Throwable => error("Uncaught exception in scheduled task '" + name +"'", t)
        } finally {
          trace("Completed execution of scheduled task '%s'.".format(name))
        }
      }
      if(period >= 0)
        //定时循环调度
        executor.scheduleAtFixedRate(runnable, delay, period, unit)
      else
        //定时调度
        executor.schedule(runnable, delay, unit)
    }
  }

  // TODO: by zmyer
  def resizeThreadPool(newSize: Int): Unit = {
    executor.setCorePoolSize(newSize)
  }

  // TODO: by zmyer
  def isStarted: Boolean = {
    this synchronized {
      executor != null
    }
  }

  // TODO: by zmyer
  private def ensureRunning(): Unit = {
    if (!isStarted)
      throw new IllegalStateException("Kafka scheduler is not running.")
  }
}
