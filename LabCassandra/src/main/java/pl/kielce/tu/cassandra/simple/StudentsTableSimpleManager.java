package pl.kielce.tu.cassandra.simple;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

public class StudentsTableSimpleManager extends SimpleManager{

	public StudentsTableSimpleManager(CqlSession session) {
		super(session);
	}

	public void createTable() {
		executeSimpleStatement( 
				"CREATE TABLE kurier (\n" +
				"    id int PRIMARY KEY,\n" +  
				"    imie text,\n" +
				"    nazwisko text,\n" +
				"    pesel int\n" +
				");");
	}

	public void createCarTable() {
		executeSimpleStatement(
				"CREATE TABLE samochod (\n" +
						"    id int PRIMARY KEY,\n" +
						"    marka text,\n" +
						"    model text,\n" +
						"    nr_rej text\n" +
						");");
	}
	public void insertIntoCarTable(int id, String marka, String model, String nr_rej) {
		System.out.println(" VALUES (" + id + ", "+ "'"+marka + "'"+ ", " + "'"+ model + "'"+ ", " +"'"+ nr_rej+"'" + ");");
		executeSimpleStatement("INSERT INTO samochod (id, marka, model, nr_rej) " +
				" VALUES (" + id + ", "+ "'"+marka + "'"+ ", " + "'"+ model + "'"+ ", " +"'"+ nr_rej+"'" + ");");
	}
	public void updateIntoCarTable(int id, String marka, String model, String nr_rej) {
		executeSimpleStatement("UPDATE samochod SET nazwisko = " + "'" + marka + "'" + " WHERE id = " + id + ";");
		executeSimpleStatement("UPDATE samochod SET imie = " + "'" + model + "'" + " WHERE id = " + id + ";");
		executeSimpleStatement("UPDATE samochod SET pesel = " + "'"+nr_rej+ "'" + " WHERE id = " + id + ";");
	}
	public void deleteFromCarTable(int id) {
		executeSimpleStatement("DELETE FROM samochod WHERE id = " + id + ";");
	}
	public void selectFromCarTable() {
		String statement = "SELECT * FROM Samochod;";
		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("samochod: ");
			System.out.print(row.getInt("id") + ", ");
			System.out.print(row.getString("marka") + ", ");
			System.out.print(row.getString("model") + ", ");
			System.out.print(row.getString("nr_rej") + ", ");

		}
		System.out.println();
		System.out.println("Statement \"" + statement + "\" executed successfully");
	}
	
	public void insertIntoTable(int id, String imie, String nazwisko, int pesel) {
		executeSimpleStatement("INSERT INTO kurier (id, imie, nazwisko, pesel) " +
				" VALUES (" + id + ", "+ "'"+imie + "'"+ ", " + "'"+ nazwisko + "'"+ ", " + pesel + ");");
	}

	public void updateIntoTable(int id, String imie, String nazwisko, int pesel) {
		executeSimpleStatement("UPDATE kurier SET nazwisko = " + "'" + nazwisko + "'" + " WHERE id = " + id + ";");
		executeSimpleStatement("UPDATE kurier SET imie = " + "'" + imie + "'" + " WHERE id = " + id + ";");
		executeSimpleStatement("UPDATE kurier SET pesel = " + pesel + " WHERE id = " + id + ";");
	}
	
	public void deleteFromTable(int id) {
		executeSimpleStatement("DELETE FROM kurier WHERE id = " + id + ";");
	}
	
	public void selectFromTable() {
		String statement = "SELECT * FROM kurier;";
		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("kurier: ");
			System.out.print(row.getInt("id") + ", ");
			System.out.print(row.getString("imie") + ", ");
			System.out.print(row.getString("nazwisko") + ", ");
			System.out.print(row.getInt("pesel") + ", ");

		}
		System.out.println();
		System.out.println("Statement \"" + statement + "\" executed successfully");
	}
	
	public void dropTable() {
		executeSimpleStatement("DROP TABLE kurier;");
	}
	public void dropCarTable() {
		executeSimpleStatement("DROP TABLE samochod;");
	}
}
