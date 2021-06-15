package pl.kielce.tu.cassandra.simple;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Scanner;

public class TestCassandraSimple {
	public static void main(String[] args) {
		try (CqlSession session = CqlSession.builder().build()) {
			KeyspaceSimpleManager keyspaceManager = new KeyspaceSimpleManager(session, "firma_przewozowa");
			keyspaceManager.dropKeyspace();
			keyspaceManager.selectKeyspaces();
			keyspaceManager.createKeyspace();
			keyspaceManager.useKeyspace();


			Scanner scan = new Scanner(System.in);
			int wybor = -1;
			int kurier_id = 1;
			int samochod_id = 1;


			StudentsTableSimpleManager tableManager = new StudentsTableSimpleManager(session);
			tableManager.createTable();
			tableManager.createCarTable();
			while (wybor!=0){
				System.out.println("Wybierz:\n1 aby dodac\n2 aby usunac\n3 aby zaktualizowac\n4 aby pokazac\n0 aby wyjsc");
				wybor = Integer.parseInt(scan.nextLine());
				if(wybor == 1){
					System.out.println("Wybierz 1 dla kuriera\n2 dla samochodu");
					wybor=Integer.parseInt(scan.nextLine());
					if (wybor==1){
						System.out.println("Wpisz imie");
						String imie = scan.nextLine();
						System.out.println("Wpisz nazwisko");
						String nazwisko = scan.nextLine();
						System.out.println("Wpisz pesel");
						int pesel = Integer.parseInt(scan.nextLine());
						tableManager.insertIntoTable(kurier_id, imie, nazwisko, pesel);
						kurier_id += 1;

						wybor = -1;
					}
					else if(wybor == 2){
						System.out.println("Wpisz marke");
						String marka = scan.nextLine();
						System.out.println("Wpisz model");
						String model = scan.nextLine();
						System.out.println("Wpisz nr_rejestracyjny");
						String nr_rej = scan.nextLine();
						tableManager.insertIntoCarTable(samochod_id, marka, model, nr_rej);
						samochod_id += 1;

						wybor = -1;
					}

				}
				else if(wybor == 2){
					System.out.println("Wybierz 1 dla kuriera\n2 dla samochodu");
					wybor=Integer.parseInt(scan.nextLine());
					if(wybor == 1){
						System.out.println("Wpisz id");
						int id = Integer.parseInt(scan.nextLine());
						tableManager.deleteFromTable(id);
						wybor = -1;
					}
					else if(wybor == 2){
						System.out.println("Wpisz id");
						int id = Integer.parseInt(scan.nextLine());
						tableManager.deleteFromCarTable(id);
						wybor = -1;
					}

				}
				else if(wybor == 3){
					System.out.println("Wybierz 1 dla kuriera\n2 dla samochodu");
					wybor=Integer.parseInt(scan.nextLine());
					if(wybor == 1){
						System.out.println("Wpisz id");
						int id = Integer.parseInt(scan.nextLine());
						System.out.println("Wpisz imie");
						String imie = scan.nextLine();
						System.out.println("Wpisz nazwisko");
						String nazwisko = scan.nextLine();
						System.out.println("Wpisz pesel");
						int pesel = Integer.parseInt(scan.nextLine());
						tableManager.updateIntoTable(id, imie, nazwisko, pesel);
						wybor = -1;
					}
					else if(wybor==2){
						System.out.println("Wpisz id");
						int id = Integer.parseInt(scan.nextLine());
						System.out.println("Wpisz marke");
						String marka = scan.nextLine();
						System.out.println("Wpisz model");
						String model = scan.nextLine();
						System.out.println("Wpisz nr rejestracyjny");
						String nr_rej = scan.nextLine();
						tableManager.updateIntoCarTable(id, marka, model, nr_rej);
						wybor = -1;
					}

				}
				else if(wybor == 4){
					System.out.println("Wybierz 1 dla kuriera\n2 dla samochodu");
					wybor=Integer.parseInt(scan.nextLine());
					if(wybor == 1){
						tableManager.selectFromTable();
						wybor = -1;
					}
					else if(wybor == 2){
						tableManager.selectFromCarTable();
						wybor = -1;
					}

				}
				else if(wybor == 0){
					break;
				}
			}






			tableManager.dropTable();
		}
	}
}