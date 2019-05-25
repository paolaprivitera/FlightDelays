package it.polito.tdp.extflightdelays.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.extflightdelays.model.Airline;
import it.polito.tdp.extflightdelays.model.Airport;
import it.polito.tdp.extflightdelays.model.Flight;
import it.polito.tdp.extflightdelays.model.Rotta;

public class ExtFlightDelaysDAO {

	public List<Airline> loadAllAirlines() {
		String sql = "SELECT * from airlines";
		List<Airline> result = new ArrayList<Airline>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				result.add(new Airline(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRLINE")));
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	public List<Airport> loadAllAirports(Map<Integer,Airport> aIdMap) {
		// Ho modificato il metodo che mi era stato fornito,
		// passandogli anche l'aIdMap
		String sql = "SELECT * FROM airports";
		List<Airport> result = new ArrayList<Airport>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				
				// Prima di creare l'oggetto verifico di non averlo gia' nella mappa
				
				if(aIdMap.get(rs.getInt("ID")) == null) {
					Airport airport = new Airport(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRPORT"),
							rs.getString("CITY"), rs.getString("STATE"), rs.getString("COUNTRY"), rs.getDouble("LATITUDE"),
							rs.getDouble("LONGITUDE"), rs.getDouble("TIMEZONE_OFFSET"));
					
					aIdMap.put(airport.getId(), airport);
					result.add(airport); // lista che non usero'
										 // perche' ho gia' la mappa
										 // (L'ha fatto per non stravolgere il metodo gia' fornito)
				}else {
					result.add(aIdMap.get(rs.getInt("ID")));
				}
				
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	
	// Metodo che mi da' tutte le rotte
	
	public List<Rotta> getRotte(Map<Integer,Airport> aIdMap, int distanzaMedia){
		String sql = "SELECT ORIGIN_AIRPORT_ID as id1, DESTINATION_AIRPORT_ID as id2, AVG(DISTANCE) as avgg " + 
				"FROM flights " + 
				"GROUP BY ORIGIN_AIRPORT_ID, DESTINATION_AIRPORT_ID " + 
				"HAVING avgg > ? ";
		
		List<Rotta> result = new ArrayList<Rotta>();
		
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, distanzaMedia);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				// Serve l'idMap perche' dalla query ho soltanto gli id degli aeroporti
				// ma nella rotta a me serve l'aeroporto corrispondente
				// quindi come parametro del metodo getRotte(...) devo ricevere anche l'idMap
				// dove avro' creato una sola volta tutti gli aeroporti
				Airport partenza = aIdMap.get(rs.getInt("id1"));
				Airport destinazione = aIdMap.get(rs.getInt("id2"));
				
				if(partenza == null || destinazione == null) { // per sicurezza (non e' obbligatorio)
					// se ho riempito bene la mappa con gli aeroporti non avro' problemi
					throw new RuntimeException("Problema in getRotte");
				}

				Rotta rotta = new Rotta(partenza, destinazione, rs.getDouble("avgg"));
				result.add(rotta);
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
		
	}

}
