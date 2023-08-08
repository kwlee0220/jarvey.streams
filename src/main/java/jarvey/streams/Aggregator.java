package jarvey.streams;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Aggregator<T,R> {
	public void aggregate(T data);
	public R close();
}
