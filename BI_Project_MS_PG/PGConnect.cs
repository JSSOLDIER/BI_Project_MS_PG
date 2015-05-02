using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace BI_Project_MS_PG
{
    class PGConnect
    {
        private NpgsqlConnection connection;
        private string database;
        
        //Constructor
        public PGConnect(string db)
        {
            Initialize(db);
        }

        //Initialize values
        private void Initialize(string db)
        {
            database = db;
            string connectionString;
            connectionString = "SERVER=localhost;Port=5432;UID=postgres;PASSWORD=metallica;DATABASE=" + database + ";";

            connection = new NpgsqlConnection(connectionString);
        }

        //Get the connection reference
        public NpgsqlConnection getConnection()
        {
            return connection;
        }

        //open connection to database
        public bool OpenConnection()
        {
            try
            {
                connection.Open();
                return true;
            }
            catch (NpgsqlException)
            {
                //The two most common error numbers when connecting are as follows:
                Console.WriteLine("Cannot connect to server or Invalid username/password. Contact administrator");         
                return false;
            }
        }

        //Close connection
        public bool CloseConnection()
        {
            try
            {
                connection.Close();
                return true;
            }
            catch (NpgsqlException ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        //Insert statement
        public void Insert(string query)
        {

            //create command and assign the query and connection from the constructor
            NpgsqlCommand cmd = new NpgsqlCommand(query, connection);

            //Execute command
            cmd.ExecuteNonQuery();
        }
    }
}
