package completo;

import java.io.Serializable;

public class Vertice implements Serializable {
	private Long id;
	private Double affinita;
	private Double centralita;
	private Double utilita;
	private long[] vicini;

	public Vertice(Long id) {
		super();
		this.id = id;
	}

	public Vertice(Long id, Double affinita, Double centralita, Double utilita, long[] vicini) {
		super();
		this.id = id;
		this.affinita = affinita;
		this.centralita = centralita;
		this.utilita = utilita;
		this.vicini = vicini;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Double getAffinita() {
		return affinita;
	}

	public void setAffinita(Double affinita) {
		this.affinita = affinita;
	}

	public Double getCentralita() {
		return centralita;
	}

	public void setCentralita(Double centralita) {
		this.centralita = centralita;
	}

	public Double getUtilita() {
		return utilita;
	}

	public void setUtilita(Double utilita) {
		this.utilita = utilita;
	}

	public long[] getVicini() {
		return vicini;
	}

	public void setVicini(long[] vicini) {
		this.vicini = vicini;
	}

}
