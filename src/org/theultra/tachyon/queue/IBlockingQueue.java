package org.theultra.tachyon.queue;

public interface IBlockingQueue<T> {
	void put(T t);
	T take();
	
	public static int getPow2Value(int prefer, final int min, final int max) {
		if(prefer < min) prefer = min;
		else {
			int n = prefer - 1;
			n |= n >>> 1;
			n |= n >>> 2;
			n |= n >>> 4;
			n |= n >>> 8;
			n |= n >>> 16;
			prefer = (n < 0) ? 1 : (n >= max) ? max : n + 1;
		}
		return prefer;
	}
}
