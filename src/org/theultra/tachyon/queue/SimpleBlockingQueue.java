package org.theultra.tachyon.queue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A non-thread-safe Queue.
 * @author lofint
 * @param <T> 
 */
@SuppressWarnings("restriction")
public class SimpleBlockingQueue<T> implements IBlockingQueue<T> {
	private static final int MIN_PACKTIME_NS = 8 << 4; 
	private static final int MAX_PACKTIME_NS = 8 << 16;
	//private static final long MAX_COUNTER = 0x7000000000000000l;
	private static final int MIN_CAPACITY = 1024 * 8;
	private static final int DEFAULT_CAPALITY = 1024 * 128;
	private static final int MAX_CAPACITY = 1024 * 1024 * 4;
	private String name = "Unnamed Queue";
	@sun.misc.Contended("g0")
	final Object[] array;
	@sun.misc.Contended("g0")
	final int capacity;
	@sun.misc.Contended("g0")
	final int m;
	@sun.misc.Contended("g1")
	long tail = 0; //not fetch(empty)
	@sun.misc.Contended("g2")
	long head = 0; //empty
	
	/**
	 * Create a NoneBlockArrayQueue with default capacity 1024 * 128
	 */
	public SimpleBlockingQueue() {
		this.capacity = DEFAULT_CAPALITY;
		array = new Object[this.capacity];
		this.m = this.capacity - 1;
	}
	
	/**
	 * Create a Queue with the capacity is a power of 2 and just greater than given prefer one, MIN_CAPACITY = 1024 * 8, MAX_CAPACITY = 1024 * 1024 * 4 
	 * @param preferCapacity 
	 */
	public SimpleBlockingQueue(int preferCapacity) {
		this.capacity = IBlockingQueue.getPow2Value(preferCapacity, MIN_CAPACITY, MAX_CAPACITY);
		array = new Object[this.capacity];
		this.m = this.capacity - 1;				
	}
	
	public void put(T obj) {
		int packTime = MIN_PACKTIME_NS;
		while(!offer(obj)) {
			LockSupport.parkNanos(packTime);
			if(packTime < MAX_PACKTIME_NS) packTime <<= 1;
		}
	}
	
	public boolean offer(T obj, final long nanoTimeout) {
		long w = 0;
		int packTime = MIN_PACKTIME_NS;
		while(!offer(obj)) {
			LockSupport.parkNanos(packTime);
			if(nanoTimeout > 0) {
				w += packTime;
				if(w > nanoTimeout) return false;
			}
			if(packTime < MAX_PACKTIME_NS) packTime <<= 1;
		}
		return true;
	}
	
	public boolean offer(T obj) {
		if(obj == null) throw new NullPointerException("Queue object can't be null");
		int p = (int) (head & this.m);
		if(array[p] != null) return false;
		head ++;
		array[p] = obj;
		return true;
	}
	
	@SuppressWarnings("unchecked")
	public T take(){
		Object r;
		int packTime = MIN_PACKTIME_NS;
		while((r=poll()) == null) {
			LockSupport.parkNanos(packTime);
			if(packTime < MAX_PACKTIME_NS) packTime <<= 1;
		}
		return (T)r;
	}
	
	@SuppressWarnings("unchecked")
	public T poll(final long nanoTimeout){
		Object r;
		long w = 0;
		int packTime = MIN_PACKTIME_NS;
		while((r=poll()) == null) {
			LockSupport.parkNanos(packTime);
			if(nanoTimeout > 0) {
				w += packTime;
				if(w > nanoTimeout) return null;
			}
			if(packTime < MAX_PACKTIME_NS) packTime <<= 1;
		}
		return (T)r;
	}
	
	@SuppressWarnings("unchecked")
	public T poll(){
		Object r;
		int p = (int) (tail & this.m);
		if((r=array[p]) == null)  return null;
		array[p] = null;
		tail++;
		return (T)r;
	}
	
	/**
	 * @return The size of objects in this queue
	 */
	public int size(){
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
}
