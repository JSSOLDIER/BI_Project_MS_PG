using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace BI_Project_MS_PG
{
    class Program
    {
        //Client select statement
        public static void ClientSelect(MySQLConnect src, PGConnect dst)
        {
            string query = "SELECT * FROM client";

            if (src.OpenConnection() == true && dst.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, src.getConnection());
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    query = "insert into client values(" + dataReader[0] + "," + dataReader[1] + ",'" + dataReader[2] + "','" + dataReader[3] + "','" + dataReader[4] + "'," + dataReader[5] + ")";
                    dst.Insert(query);
                }

                //close Data Reader
                dataReader.Close();

                //close Connection
                src.CloseConnection();
                dst.CloseConnection();
            }
        }

        //Commande select statement
        public static void CommandeSelect(MySQLConnect src, PGConnect dst)
        {
            string query = "SELECT * FROM commande";

            if (src.OpenConnection() == true && dst.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, src.getConnection());
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    query = "insert into commande values(" + dataReader[0] + "," + dataReader[1] + ",'" + dataReader[2] + "'," + dataReader[3] + "," + dataReader[4] + "," + dataReader[5] + "," + dataReader[6] + ")";
                    dst.Insert(query);
                }

                //close Data Reader
                dataReader.Close();

                //close Connection
                src.CloseConnection();
                dst.CloseConnection();
            }
        }

        //Fournisseur select statement
        public static void FournisseurSelect(MySQLConnect src, PGConnect dst)
        {
            string query = "SELECT * FROM fournisseur";

            if (src.OpenConnection() == true && dst.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, src.getConnection());
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    query = "insert into fournisseur values(" + dataReader[0] + ",'" + dataReader[1] + "','" + dataReader[2] + "')";
                    dst.Insert(query);
                }

                //close Data Reader
                dataReader.Close();

                //close Connection
                src.CloseConnection();
                dst.CloseConnection();
            }
        }

        //Produit select statement
        public static void ProduitSelect(MySQLConnect src, PGConnect dst)
        {
            string query = "SELECT * FROM produit";

            if (src.OpenConnection() == true && dst.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, src.getConnection());
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    query = "insert into produit values(" + dataReader[0] + ",'" + dataReader[1] + "','" + dataReader[2] + "','" + dataReader[3] + "')";
                    dst.Insert(query);
                }

                //close Data Reader
                dataReader.Close();

                //close Connection
                src.CloseConnection();
                dst.CloseConnection();
            }
        }

        //Stock select statement
        public static void StockSelect(MySQLConnect src, PGConnect dst)
        {
            string query = "SELECT * FROM stock";

            if (src.OpenConnection() == true && dst.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, src.getConnection());
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    query = "insert into stock values(" + dataReader[0] + ",'" + dataReader[1] + "'," + dataReader[2] + ")";
                    dst.Insert(query);
                }

                //close Data Reader
                dataReader.Close();

                //close Connection
                src.CloseConnection();
                dst.CloseConnection();
            }
        }

        static void Main(string[] args)
        {
            MySQLConnect conBI = new MySQLConnect("bi");
            PGConnect conDWH = new PGConnect("dwh");

            Console.WriteLine("Tapez 0 pour transmettre la table Client");
            Console.WriteLine("Tapez 1 pour transmettre la table Commande");
            Console.WriteLine("Tapez 2 pour transmettre la table Fournisseur");
            Console.WriteLine("Tapez 3 pour transmettre la table Produit");
            Console.WriteLine("Tapez 4 pour transmettre la table Stock");
            Console.WriteLine("Tapez 5 pour transmettre la base de données au DWH");
            Console.WriteLine("Tapez 6 pour quitter");

            int choix;
            DateTime tempsdeb;
            TimeSpan diffTemps;

            string number = Console.ReadLine();
            int.TryParse(number, out choix);

            while (choix != 6)
            {
                switch (choix)
                {
                    case 0: tempsdeb = DateTime.Now;
                        ClientSelect(conBI, conDWH);
                        diffTemps = DateTime.Now - tempsdeb;
                        Console.WriteLine("Table Client: " + conBI.Count("client") + "enregistrements traitées \n**L'operation a duré " + diffTemps);
                        break;

                    case 1: tempsdeb = DateTime.Now;
                        CommandeSelect(conBI, conDWH);
                        diffTemps = DateTime.Now - tempsdeb;
                        Console.WriteLine("Table Commande: " + conBI.Count("commande") + "enregistrements traitées \n**L'operation a duré " + diffTemps);
                        break;

                    case 2: tempsdeb = DateTime.Now;
                        FournisseurSelect(conBI, conDWH);
                        diffTemps = DateTime.Now - tempsdeb;
                        Console.WriteLine("Table Fournisseur: " + conBI.Count("fournisseur") + "enregistrements traitées \n**L'operation a duré " + diffTemps);
                        break;

                    case 3: tempsdeb = DateTime.Now;
                        ProduitSelect(conBI, conDWH);
                        diffTemps = DateTime.Now - tempsdeb;
                        Console.WriteLine("Table Produit: " + conBI.Count("produit") + "enregistrements traitées \n**L'operation a duré " + diffTemps);
                        break;

                    case 4: tempsdeb = DateTime.Now;
                        StockSelect(conBI, conDWH);
                        diffTemps = DateTime.Now - tempsdeb;
                        Console.WriteLine("Table Stock: " + conBI.Count("stock") + "enregistrements traitées \n**L'operation a duré " + diffTemps);
                        break;

                    case 5: tempsdeb = DateTime.Now;
                        ClientSelect(conBI, conDWH); 
                        CommandeSelect(conBI, conDWH);
                        FournisseurSelect(conBI, conDWH);
                        ProduitSelect(conBI, conDWH);
                        StockSelect(conBI, conDWH);
                        diffTemps = DateTime.Now - tempsdeb;
                        Console.WriteLine("Table Client: " + conBI.Count("client") + "enregistrements traitées.");
                        Console.WriteLine("Table Commande: " + conBI.Count("commande") + "enregistrements traitées.");
                        Console.WriteLine("Table Fournisseur: " + conBI.Count("fournisseur") + "enregistrements traitées.");
                        Console.WriteLine("Table Produit: " + conBI.Count("produit") + "enregistrements traitées.");
                        Console.WriteLine("Table Stock: " + conBI.Count("stock") + "enregistrements traitées.");
                        Console.WriteLine("**L'operation a duré " + diffTemps);
                        break;
                }
                Console.WriteLine("Tapez 0 pour transmettre la table Client");
                Console.WriteLine("Tapez 1 pour transmettre la table Commande");
                Console.WriteLine("Tapez 2 pour transmettre la table Fournisseur");
                Console.WriteLine("Tapez 3 pour transmettre la table Produit");
                Console.WriteLine("Tapez 4 pour transmettre la table Stock");
                Console.WriteLine("Tapez 5 pour transmettre la base de données au DWH");
                Console.WriteLine("Tapez 6 pour quitter");
                number = Console.ReadLine();
                int.TryParse(number, out choix);
            }
            Environment.Exit(0);
        }
    }
}
