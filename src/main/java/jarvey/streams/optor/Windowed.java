package jarvey.streams.optor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Windowed<T> {
	private final Window m_window;
	private final T m_data;
	
	public static <T> Windowed<T> of(Window window, T data) {
		return new Windowed<>(window, data);
	}
	
	private Windowed(Window window, T data) {
		m_window = window;
		m_data = data;
	}
	
	public Window window() {
		return m_window;
	}
	
	public T value() {
		return m_data;
	}
	
	@Override
	public String toString() {
		return String.format("Windowed[%s]", m_data);
	}
}
