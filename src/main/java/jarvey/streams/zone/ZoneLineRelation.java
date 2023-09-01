package jarvey.streams.zone;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum ZoneLineRelation {
	Unassigned("U"),
	Entered("E"),
	Left("L"),
	Inside("I"),
	Through("T"),
	Deleted("D");
	
	private final String m_symbol;
	
	private ZoneLineRelation(String symbol) {
		m_symbol = symbol;
	}
	
	public String getSymbol() {
		return m_symbol;
	}
}