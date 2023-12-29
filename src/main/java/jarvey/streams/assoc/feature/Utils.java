package jarvey.streams.assoc.feature;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class Utils {
	public static double norm(float[] vec) {
		double sum = 0;
		for ( float v: vec ) {
			sum += v*v;
		}
		
		return Math.sqrt(sum);
	}
	
	public static float[] normalize(float[] vec) {
		double norm = norm(vec);
		
		float[] normed = new float[vec.length];
		for ( int i =0; i < vec.length; ++i ) {
			normed[i] = (float)(vec[i] / norm);
		}
		
		return normed;
	}
	
	public static float dotProduct(float[] vec1, float[] vec2) {
		float sum = 0;
		for ( int i =0; i < vec1.length && i < vec2.length; ++i ) {
			sum += (vec1[i] * vec2[i]);
		}
		
		return sum;
	}
	
	public static double cosineSimilarityNormalized(float[] vec1, float[] vec2) {
		return dotProduct(vec1, vec2);
	}
	
	public static double cosineSimilarity(float[] vec1, float[] vec2) {
//		return dotProduct(vec1, vec2) / (norm(vec1) * norm(vec2));
		vec1 = normalize(vec1);
		vec2 = normalize(vec2);
		return cosineSimilarityNormalized(vec1, vec2);
	}
	
	public static double cosineSimilarity(float[] vec1, float[][] mat2, int kth) {
		MinMaxPriorityQueue<Double> heap = MinMaxPriorityQueue.maximumSize(kth)
																.create();
		for ( int ri = 0; ri < mat2.length; ++ri ) {
			heap.add(cosineSimilarity(vec1, mat2[ri]));
		}
		
		double kthValue = 0;
		for ( int i =0; i < kth; ++i ) {
			kthValue = heap.pollFirst();
		}
		return kthValue;
	}
}
