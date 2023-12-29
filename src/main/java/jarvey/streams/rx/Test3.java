package jarvey.streams.rx;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import utils.Generator;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class Test3 {
	public static final void main(String... args) throws Exception {
		ExecutorService exector = Executors.newFixedThreadPool(10);
		Generator<Integer> gen = new Generator<Integer>(exector, 10) {
			@Override
			public void run() throws Throwable {
				for ( int i =0; i < 10; ++i ) {
					yield(-i);
				}
			}
		};
		gen.forEach(System.out::println);
		System.out.println("------------------");
		
		exector.shutdown();
	}
}