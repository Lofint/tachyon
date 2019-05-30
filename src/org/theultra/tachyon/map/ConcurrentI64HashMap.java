package org.theultra.tachyon.map;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-Safe, for objects which have a long id. a low memory(even with a load factor less than 0.2), less GC, high performance HashMap. 
 * <br>
 * Caution! When the load(size) is 1/2 as much or more than the capacity. the performance of map will significantly slow down.
 * <br>
 * Notice: The capacity of this map will never automatic increase. 
 * 
 * The recommended load factor is less than 0.25<br>
 * This map is designed for over 2M objects, 
 * if the number of objects is less than 512k and didn't need the special feature of this map, 
 * use the HashMap in JDK instead. 
 * @author Lofint
 *
 */
public class ConcurrentI64HashMap<T extends I64Obj>{
	//必须是2的幂
	public final static int MIN_CAPACITY = 1024;
	public final static int DEFAULT_CAPACITY = 1024 * 8;
	public final static int MAX_CAPACITY = 1024 * 1024 * 1024;
	
	public final static int MIN_LOCKER = 16;
	public final static int MAX_LOCKER = 32;
	
	
	private final I64Obj[] baseArray;
	private final int capacity;
	private final int m;
	private final MapLock<T>[] rLockers;
	private final int lockM;
	
	@SuppressWarnings("unchecked")
	public ConcurrentI64HashMap() {
		this.capacity = DEFAULT_CAPACITY;
		
		baseArray = new I64Obj[this.capacity];
		this.m = this.capacity - 1;
		
		int lockCount = this.capacity / 4096;
		lockCount = Math.min(MAX_LOCKER, Math.max(MIN_LOCKER, lockCount));
		
		lockM = lockCount -1;
		
		rLockers = new MapLock[lockCount];
		//sizes = new int[lockCount];
		
		for(int i = 0; i < lockCount; i ++) {
			rLockers[i] = new MapLock<T>();
		}
	}
	
	/**
	 * 
	 * @param capacity the prefer capacity by user, the real capacity will be an n power of 2 which just greater than the given capacity.
	 */
	@SuppressWarnings("unchecked")
	public ConcurrentI64HashMap(int capacity) {
		if(capacity < MIN_CAPACITY) capacity = MIN_CAPACITY;
		else {
			int n = capacity - 1;
			n |= n >>> 1;
			n |= n >>> 2;
			n |= n >>> 4;
			n |= n >>> 8;
			n |= n >>> 16;
			capacity = (n < 0) ? 1 : (n >= MAX_CAPACITY) ? MAX_CAPACITY : n + 1;
			
		}
		this.capacity = capacity;
		
		baseArray = new I64Obj[this.capacity];
		this.m = this.capacity - 1;
		
		int lockCount = this.capacity / 4096;
		lockCount = Math.min(MAX_LOCKER, Math.max(MIN_LOCKER, lockCount));
		
		lockM = lockCount -1;
		
		rLockers = new MapLock[lockCount];
		//sizes = new int[lockCount];
		
		for(int i = 0; i < lockCount; i ++) {
			rLockers[i] = new MapLock<T>();
		}
	}

	
	/**
	 * @param obj
	 * @return if replaced current obj, return true;
	 */	
	@SuppressWarnings("unchecked")
	public final boolean put(T obj) {
		long key = obj.getId();
		int p = position(key);
		int lockIdx = p & lockM;

		try{
			rLockers[lockIdx].lock();
			I64Obj cur = baseArray[p];
			
			I64MapNode<T> last = null;
			if(cur == null) {   // baseArray == null
				baseArray[p] = obj;
				this.rLockers[lockIdx].size++;
				//size.getAndIncrement();
				return false;
			}
			
			do {
				I64MapNode<T>  cNode = null;
				if(cur.getClass() == I64MapNode.class) cNode = (I64MapNode<T>)cur;
				
				if(cur.getId() == key) {  
					if(cNode != null) { //cNode id match， 替换obj即可
						cNode.value = obj;
						return true;
					} else {			//cur match
						if(last == null) { // baseArray[p] match;
							baseArray[p] = obj;
							return true;
						} else {			// last(node) - cur(obj & matched)
							last.next = obj;
							return true;
						}
					}
				} else {
					if(cNode != null) {
						cur = cNode.next;
						last = cNode;
					} else cur = null;
				}
			} while(cur != null);
			
			
			//baseArray[p] is not Null and can't found the obj who has the same key , insert a node
			I64MapNode<T> node = null;
			if(rLockers[lockIdx].unusedNodeChain != null) {
				node = rLockers[lockIdx].unusedNodeChain;
				rLockers[lockIdx].unusedNodeChain = (I64MapNode<T>) node.next;
				node.value = obj;
				//node.position = p;
				node.next = baseArray[p];
			} else {
//				node = new I64MapNode<T>(p, obj, baseArray[p]);
				node = new I64MapNode<T>(obj, baseArray[p]);
			}
			baseArray[p] = node;
			this.rLockers[lockIdx].size++;
			//size.getAndIncrement();
			return false;
		} finally {
			rLockers[lockIdx].unlock();
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public final T get(long key) {
		int p = position(key);
		I64Obj cur;
		if((cur = baseArray[p]) == null) return null;

		if(cur.getClass() == I64MapNode.class) {
			I64MapNode<T> cNode = ((I64MapNode<T>) cur);
			for(int t2 =0; t2 < 2; t2++) {
				while(true){
					I64Obj next = cNode.next;
					I64Obj obj = cNode.value;
					//if(cNode.position != p || obj == null) {
					if(obj == null) {
						break;
					} else {
						if(obj.getId() == key) return (T) obj;
						if(position(obj.getId()) != p) break;
						if(next.getClass() == I64MapNode.class) {
							cNode = ((I64MapNode<T>) next);
						} else {
							if(key == next.getId()) return (T) next;
							else return null;
						}
					}
				}
				
				cur = baseArray[p];
				if(cur.getClass() == I64MapNode.class) {
					cNode = ((I64MapNode<T>) cur);
				} else {
					if(key == cur.getId()) return (T)cur;
					return null;
				}
			}
			
			//如果连续失败2次，尝试锁定获取
			//System.out.println("-------------------------------failed 2 times try lock...------------------------------");
			
			int lockIdx = p & lockM;
			try{
				rLockers[lockIdx].lock();
				cur = baseArray[p];
				while(cur != null) {
					if(cur.getClass() == I64MapNode.class) {
						if(cur.getId() == key) {
							return ((I64MapNode<T>) cur).value;
						} else {
							cur = ((I64MapNode<T>) cur).next;
						}
					} else if(cur.getId() == key) {
						return (T) cur;
					} else {
						return null;
					}
				}
				return null;
			} finally {
				rLockers[lockIdx].unlock();
			}
		} else {
			if(key == cur.getId()) return (T)cur;
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public final T remove(long key) {
		int p = position(key);
		int lockIdx = p & lockM;
		
		T r = null;
		try{
			rLockers[lockIdx].lock();
			I64Obj cur = baseArray[p];
			I64MapNode<T> last = null, last2 = null;
			while(cur != null) {
				I64MapNode<T> cNode = null;
				if(cur.getClass() == I64MapNode.class) cNode = (I64MapNode<T>)cur;
				
				if(cur.getId() == key) {
					if(last == null) { //第一个
						if(cNode == null) {
							baseArray[p] = null;
							this.rLockers[lockIdx].size --;
							//size.getAndDecrement();
							return (T) cur;
						} else {
							baseArray[p] = cNode.next;
							r = cNode.value;
							
							//cNode.position = -1;
							cNode.value = null;
							cNode.next = rLockers[lockIdx].unusedNodeChain;
							rLockers[lockIdx].unusedNodeChain = cNode;
							
							this.rLockers[lockIdx].size --;
							//size.getAndDecrement();
							return r;
						}
					} else if(cNode == null) { //最后一个Obj, 需要把last脱壳，移除
						if(last2 == null) { //last(node, baseArray[p]) - cur(obj & match)
							baseArray[p] = last.value;

							//last.position = -1;
							last.value = null;
							last.next = rLockers[lockIdx].unusedNodeChain;
							rLockers[lockIdx].unusedNodeChain = last;
							
							this.rLockers[lockIdx].size --;
							//size.getAndDecrement();
							return (T) cur;
						} else {  						// last2(node)-last(node)-cur(obj & match)
							last2.next = last.value; 
							
							//last.position = -1;
							last.value = null;
							last.next = rLockers[lockIdx].unusedNodeChain;
							rLockers[lockIdx].unusedNodeChain = last;
							
							this.rLockers[lockIdx].size --;
							//size.getAndDecrement();
							return (T) cur;
						}
					} else { // last(node) - cur/cNode(node & match) - next(node or obj)
						r = cNode.value;
						last.next = cNode.next;
						//cNode.position = -1;
						cNode.value = null;
						cNode.next = rLockers[lockIdx].unusedNodeChain;
						rLockers[lockIdx].unusedNodeChain = cNode;
						
						this.rLockers[lockIdx].size --;
						//size.getAndDecrement();
						return r;
					}
				} else {
					if(cNode != null) { 
						cur = cNode.next;
						last2 = last;
						last = cNode;
					} else return null; //not match，如果最后比的是obj，那么结束了
				}
			}
			return null;
		} finally {
			rLockers[lockIdx].unlock();
		}
	}
	
	public int size() {
		int r = 0;
		for(int i = 0; i < rLockers.length; i ++) {
			r += this.rLockers[i].size;
		}
		return r;
	}
	
//	public void statistic() {
//		int inBase = 1 , twin=1, triple=1;
//		for(int i = 0; i < capacity; i ++) {
//			if(baseArray[i] != null) {
//				inBase++;
//				if (baseArray[i] instanceof Node) {
//					twin ++;
//					if(((Node)baseArray[i]).next instanceof Node) {
//						triple ++;
//					}
//				}
//			}
//		} 
//		System.out.println("Base Rate:" + (inBase * 100 / size) + "%, Twin Rate = " + (twin * 100 / size) + "%, Triple Rate:" + (triple * 100 / size) + "%");
//	}
	
	
	protected final int position(long key) {
		 key = (key >> 32) ^ key;  
		 //key = (((key >> 32) ^ key) ^ 0xdeadbeef);
		 return  (int)((key  ^ (key >>> 16))  & this.m);
		// hash = (int)((key >> 32) ^ key) ^ 0xdeadbeef
	}

	static final class I64MapNode<T extends I64Obj> implements I64Obj{
		T value;
		volatile I64Obj next;

		I64MapNode(int position, T obj, I64Obj next){
			this.value = obj;
			this.next = next;
		}
		
		I64MapNode(T obj, I64Obj next){
			this.value = obj;
			this.next = next;
		}
		
		@Override
		public long getId() {
			return value.getId();
		}
	}
	
	static final class MapLock<T extends I64Obj> extends ReentrantLock{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		long size = 0;
		I64MapNode<T> unusedNodeChain = null;
	}
	
}
