package com.monitorjbl

import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class BlockingExecutorService extends AbstractExecutorService {
  private final ExecutorService executor;
  private final Semaphore blockExecution;

  public BlockingExecutorService(int nTreads, int queueSize) {
    this.executor = Executors.newFixedThreadPool(nTreads);
    blockExecution = new Semaphore(queueSize);
  }

  @Override
  public void shutdown() {
    executor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return executor.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return executor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return executor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return executor.awaitTermination(timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    blockExecution.acquireUninterruptibly();
    executor.execute(new Runnable() {
      public void run() {
        try {
          command.run();
        } finally {
          blockExecution.release();
        }
      }
    })
  }
}
