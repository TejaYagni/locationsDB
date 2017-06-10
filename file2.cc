/*
CS688 Assignment 3
Name: Saiteja Yagni
ZID: z1789380
Instructor: Dr Duffin
Due Date: 16 November 2016
*/

/*Program to display the details of the city like which state and country it is in */

//Below are the header files
#include<iostream>
#include<string.h>
#include<sstream>
#include<postgresql/libpq-fe.h>
#include<vector> // As vectors are used

using std :: cin;
using std :: cout;
using std :: string;
using std :: ostringstream;
using std :: endl;
using std :: vector;
using std :: cerr;

//Below are the functions that will be used to prompt user to enter the city name and then query database and retrieve the information
string queryUser();
PGconn* Connection();
void queryDB(PGconn* mydb,const string &city);
string escape_string(PGconn* mydb,const string &city);

/*
Name: main
Return Type: int 
Arguments: None
Notes: This is where the program execution starts. It will query user and gets teh city name and then creates a connection and calls to query the db and get the required information
*/
int main()
{
  string city = queryUser(); //Function call to get the city name
  PGconn* mydb = Connection(); //Creates a database connection 
  queryDB(mydb,city); //Function call to get the required information from the database
  PQfinish(mydb); //Closes the database connection
  return 0;
}

/*
Name: queryDB
Return Type: void
Arguments: PGconn* mydb, const string &city
Notes: This function takes a database connection and a city name as input then queries the database and prints the end result on the output stream i.e city related information
*/
void queryDB(PGconn* mydb,const string &city)
{
  PGresult* result;
  ostringstream query; //To hold the query 
  vector<string> city_ids; //Vector to store the city ids
  
  query << "SELECT location_id from z1789380.Location where name = '"<<escape_string(mydb,city)<<"';";
  result = PQexec(mydb,query.str().c_str()); //Query converted into c-style string and is send to PQexec to execute it
  query.str(" ");

  //stores number of rows and columns 
  int nrows = PQntuples(result);
  int ncols = PQnfields(result);

  for(int row = 0; row < nrows; ++row)
    {
      for(int col = 0; col < ncols; ++col)
	{
	  city_ids.push_back(PQgetvalue(result,row,col)); //ids are pushed into the vector
	}
    }
  
  PQclear(result); //Clears the result variable
  
  int vector_size = city_ids.size(); //Stores the size of the vector

  
  for(int i = 0; i < vector_size; ++i)
    {
      vector<string> fk_loc_id; //Vector to store the foreign keys 
      string temp; //Temp variable to store the value retrieved from the query

      //Query to get the location ids is created and executed
      query << "SELECT fk_location_id from z1789380.City where location_id = "<<city_ids[i]<<";";
      result = PQexec(mydb,query.str().c_str());
      query.str(" ");

      //TO store the number of rows and columns retrieved
      nrows = PQntuples(result);
      ncols = PQnfields(result);

      for(int row = 0; row < nrows; ++row)
	{
	  for(int col = 0; col < ncols; ++col)
	    {
	      temp = PQgetvalue(result,0,0); 
	    }
	}
      
      if(temp != "") //If the value retrieved is not null then it is stored into vector else ignored
	{
	  fk_loc_id.push_back(PQgetvalue(result,0,0));

	}

      PQclear(result);
      
      int size_loc = fk_loc_id.size();
      
      for (int i = 0; i < size_loc; ++i)
	{	  
	  string temp_type;
	  string line = "City of "+city; //Line to which the region names are concatenated
	  do
	    {
	      //Query created and executed for retrieving names from location table
	      query << "select name from z1789380.Location where location_id = "<<fk_loc_id[i]<<";"; 
	      result = PQexec(mydb,query.str().c_str());
	      query.str(" ");

	      string name = PQgetvalue(result,0,0); //name is added to the variable 
	      
	      PQclear(result);

	      //Query that retrieves type and key for the region it is associated with it
	      query << "select fk_location_id,type from z1789380.Region where location_id = "<<fk_loc_id[i]<<";";
	      result = PQexec(mydb,query.str().c_str());
	      query.str(" ");

	      //Retrieved results are stored into vector and a variable
	      fk_loc_id[i] = PQgetvalue(result,0,0);
	      temp_type = PQgetvalue(result,0,1);
	      
	      PQclear(result);

	      line = line+" is in "+name+" "+temp_type; //Line is concatenated with the name and type of the region
	      	      
	    }while(fk_loc_id[i]!=""); //Untill there is no more foreign key associated with it this loop will be executed
	    cout << line<<endl;
	}
	  
    }          
}

/*
Name: queryUser
Return Type: string
Arguments: None
Notes: Asks the user to enter the name of the city and returns it
*/
string queryUser()
{
  string city;
  cout << "Enter the name of the city!"<<endl;
  getline(cin,city);

  return city;
}

/*
Name: Connection
Return Type: PGconn*
Arguments: None
Notes: Creates a database connection and returns it
*/
PGconn* Connection()
{
  PGconn* mydb;
  mydb = PQconnectdb("host = courses dbname = z1789380 password = 1994Feb18"); //Connection established
  
  if(PQstatus(mydb)!=CONNECTION_OK) //Checks if connection is okay, if not dispalys error message
    {
      cerr << "Can't connect to db" << endl;
      cerr << PQerrorMessage(mydb);
      return NULL;
    }
  return mydb;
}

/*
Name: escape_string
Arguments: PGconn * conn, const string &s
Return Type: string
Notes: Takes a database connection and a constant string as arguments processes it and then returns the string back
*/
string  escape_string(PGconn * conn , const string & s)
{
  char * buffer;
  buffer = new char[s.length()*2 + 1];

  PQescapeStringConn(conn, buffer, s.c_str(), s.length(), NULL); //Pre-defined function used to filter the string

  string result = buffer;

  delete [] buffer;
  return result;
}
