package org.theultra.tachyon.queue.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import org.theultra.tachyon.queue.IBlockingQueue;

/**
 * It is NOT a thread-safe None Block Queue. It is safe when there is only one sender thread and one receiver thread.
 * When the queue is full the put() method will return false instantly, and when it is empty, the get() method will return null instantly.
 * @author lofint
 * @param <T> 
 */
public class ConcurrentQueue<T> implements IBlockingQueue<T>{
	private static final int MIN_WAITTIME_NS = 16384; 
	private static final int MAX_WAITTIME_NS = MIN_WAITTIME_NS << 4;
	
	private static final int INTERNAL_SPIN_COUNT = 1; //must greater than 0;
	private static final int MIN_CAPACITY = 1024 * 8;
	private static final int DEFAULT_CAPACITY = 1024 * 128;
	private static final int MAX_CAPACITY = 1024 * 1024 * 4;
	private String name = "Unnamed Queue";
	private final byte[] falseOffer;
	private final byte[] falsePoll;
	private final AtomicReferenceArray<T> array;
	final int capacity;
	final int m;
	final AtomicLong tail;  
	final AtomicLong head;  
	
	final AtomicLong[] als = new AtomicLong[11];
	
	/**
	 * Create a NoneBlockArrayQueue with default capacity 1024 * 128
	 */
	public ConcurrentQueue() {
		this.capacity = DEFAULT_CAPACITY;
		array = new AtomicReferenceArray<T>(this.capacity);
		falseOffer = new byte[this.capacity];
		falsePoll = new byte[this.capacity];
		this.m = this.capacity - 1;
		
		for (int i = 0; i < als.length; i++) {
			als[i] = new AtomicLong(0);
		}
		head = als[3];
		tail = als[7];
	}
	
	/**
	 * Create a NoneBlockArrayQueue with the capacity is a power and just greater than given prefer one, MIN_CAPACITY = 1024 * 8, MAX_CAPACITY = 1024 * 1024 * 4 
	 */	
	public ConcurrentQueue(int preferCapacity) {
		this.capacity = IBlockingQueue.getPow2Value(preferCapacity, MIN_CAPACITY, MAX_CAPACITY);
		array = new AtomicReferenceArray<T>(this.capacity);
		falseOffer = new byte[this.capacity];
		falsePoll = new byte[this.capacity];
		this.m = this.capacity - 1;	
		
		for (int i = 0; i < als.length; i++) {
			als[i] = new AtomicLong(0);
		}
		head = als[3];
		tail = als[7];
	}
	
	public boolean offer(T obj) {
		if(obj == null) throw new NullPointerException("Can't put null object into this queue");
		while(true) {
			long head = this.head.get();
			int p =(int) (head & this.m);
			if(falsePoll[p] > 0) {
				synchronized(falsePoll) {
					if(falsePoll[p] > 0) {
						if(this.head.compareAndSet(head, head + 1)){
							falsePoll[p] --;
						}
					}
				}
				break;
			}
			if(array.get(p) != null) return false;
			if(this.head.compareAndSet(head, head + 1)) {
				for(int i = 0; i < INTERNAL_SPIN_COUNT; i ++) {
					if(!array.compareAndSet(p, null, obj)) {
						wait(2 << i);
					} else return true; 
				}
				synchronized(falseOffer) {
					falseOffer[p] ++;
					if(falseOffer[p] > 1) System.out.println("add failed " + "P " + p + " c " + falseOffer[p]);
				}
			}
			return false;
		}
		return false;
	}
	
	
	public boolean offer(T obj, long nanoTimeout){
		long w = 0;
		int waitTime = MIN_WAITTIME_NS;
		while(!offer(obj)) {
			w += waitTime;
			if(nanoTimeout > 0 && w > nanoTimeout) return false;
			wait(waitTime);
			if(waitTime < MAX_WAITTIME_NS) waitTime <<= 1;
		}
		return true;
	}
	
	public void put(T obj){
		int waitTime = MIN_WAITTIME_NS;
		while(!offer(obj)) {
			wait(waitTime);
			if(waitTime < MAX_WAITTIME_NS) waitTime <<= 1;
		}
	} 
	
	public T poll(){
		while(true) {
			T r;
			long tail = this.tail.get();
			int p = (int) (tail & this.m);
			if(falseOffer[p] > 0) {
				synchronized(falseOffer) {
					if(this.tail.compareAndSet(tail, tail + 1)) {
						falseOffer[p]--;
					} 
					break;
				}
			}
			r = array.get(p);
			if(r == null) return null;
			if(this.tail.compareAndSet(tail, tail + 1)) {
				for(int i = 0; i < INTERNAL_SPIN_COUNT; i ++) {
					if((r = array.getAndSet(p, null)) == null){
						wait(2 << i);
					} else return r;
				}
				synchronized(falsePoll) {
					falsePoll[p] ++;
					if(falsePoll[p] > 1) System.out.println("poll failed " + "P " + p + " c " + falsePoll[p]);
				}
			} 
			return null;
		}
		return null;
	}
	
	public T poll(long nanoTimeout){
		T r;
		long w = 0;
		int waitTime = MIN_WAITTIME_NS;
		while((r=poll()) == null) {
			w += waitTime;
			if(nanoTimeout > 0 && w > nanoTimeout) return null;
			wait(waitTime);
			if(waitTime < MAX_WAITTIME_NS) waitTime <<= 1;
		}
		return r;
	}
	
	
	public T take(){
		T r;
		int waitTime = MIN_WAITTIME_NS;
		while((r=poll()) == null) {
			wait(waitTime);
			if(waitTime < MAX_WAITTIME_NS) waitTime <<= 1;
		}
		return r;
	}
	
	public int size(){
		long tail = this.tail.get();
		long head = this.head.get();
		if (head > tail) {
			return (int) (head - tail);
		} else {
			return 0;
		}
	}
	
	public int capacity() {
		return this.capacity;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	private Object locker = new Object();
	private void wait(int nanos) {
		if(nanos > (MIN_WAITTIME_NS << 2)) {
			LockSupport.parkNanos(locker, nanos);
		} else {
			long t0 = System.nanoTime();
			while(System.nanoTime() - t0 < nanos);
		}
	}
}
