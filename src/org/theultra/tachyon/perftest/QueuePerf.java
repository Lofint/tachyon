package org.theultra.tachyon.perftest;

import java.util.concurrent.CountDownLatch;

import org.theultra.tachyon.queue.IBlockingQueue;
import org.theultra.tachyon.queue.SimpleBlockingQueue;
import org.theultra.tachyon.queue.concurrent.ConcurrentQueue;


public class QueuePerf {
	static int ITERATIONS = 1024 * 1024 * 3 * 5 * 7 * 4 ;
	static int NUM_PRODUCER = 1;
	static int NUM_CONSUMER = 3;
	
	static int QUEUE_SIZE= 1024 * 1024;
	
	static IBlockingQueue<Object> queue;
	
	public static void main(String[] args) throws InterruptedException {
		
		queue = new ConcurrentQueue<Object>(QUEUE_SIZE);
		//queue = new SimpleBlockingQueue<Object>(QUEUE_SIZE);
		
		benchmark();
	}

	private static void benchmark() throws InterruptedException {
		final Object object = new Object();
		int iter = ((int)(ITERATIONS / NUM_PRODUCER / NUM_CONSUMER)) * NUM_PRODUCER * NUM_CONSUMER;

		final CountDownLatch latch = new CountDownLatch(NUM_CONSUMER);
		
		
		long tm0 = System.currentTimeMillis();
		
		for(int i = 0; i < NUM_PRODUCER; i ++) {
			final int s = i;
			new Thread(){
				public void run() {
					this.setName("Producer " + s);
					int times = iter / NUM_PRODUCER ;
					long t0 = System.nanoTime();
					for(int t = 0; t < times; t ++) {
						queue.put(object);
					}
					long tt = System.nanoTime() - t0;;
					System.out.println("Producer " + s + " has completed. Cost Per Put " + (tt/times) + "ns. ");
				}
			}.start();
		}
		
		for(int i = 0; i < NUM_CONSUMER; i ++) {
			final int s = i;
			new Thread(){
				public void run() {
					this.setName("Consumer " + s);
					int times = iter / NUM_CONSUMER ;
					long t0 = System.nanoTime();
					for(int t = 0; t < times; t ++) {
						queue.take();
					}
					long tt = System.nanoTime() - t0;
					latch.countDown();
					System.out.println("Consumer " + s + " has completed. Cost Per Take " + (tt/times) + "ns. ");
				}
			}.start();
		}
		
		

		latch.await();
		long tm1 = System.currentTimeMillis();
		System.out.format("Total %dM I/O, cost %dms, %,d/s, %.2fM/s", 
				iter/1000/1000, 
				(tm1 - tm0), 
				((long)iter * 1000 /(tm1-tm0)),  
				((float)iter /1000/(tm1-tm0)));
		
	}
	
}
