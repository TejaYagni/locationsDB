/*
CS688 Assignment 3 Fall 2016
Name: Saiteja Yagni
ZID: z1789380
Instructor: Dr Duffin
Due Date: 16 November 2016
*/

/*C++ program to process different kinds of files and load tehm into a database*/

//Below are the header files that will be used 
#include<iostream> //For input and output purposes
#include<fstream> //For file streaming
#include<string.h> //As string data type will be used
#include<postgresql/libpq-fe.h> //For connecting and working with postgres database
#include<sstream> //For createing a stream to write a query
#include<vector> //As vectors will be used to store
#include<stdlib.h> 


using std:: string;
using std:: ifstream;
using std:: cout;
using std:: endl;
using std:: cerr;
using std:: ostringstream;
using std:: istringstream;
using std:: vector;


class Abc
{
public:
  void Type(string &arg,string &input, PGconn* mydb);
  void Type1process(string &line,PGconn* mydb);
  void Type2process(string &line,PGconn* mydb);
  void Type3process(string &line,PGconn* mydb);
  PGconn* Connection();
  string escape_string(PGconn* conn,const string &s);
};

/*
Function : main
Return Type: int 
Arguments: int argc, char *argv[]
Notes: This is where the program execution starts. This function takes two command line arguments. The first one takes a filename and the second takes the file type
*/

int main(int argc, char *argv[])
{
  Abc a;
  PGconn* mydb = a.Connection();
  if(PQstatus(mydb) != CONNECTION_OK) //This control statement is executed whenwe can't connect to database
    {
      cerr <<"Couldn't connect to database"<<endl;
    }

  string arg = argv[2]; //Stores the file type into variable
  string dir = argv[1]; //stores the file name into a variable
  a.Type(arg,dir,mydb); //Function call to Type which then forwards to the function depending upon the process
  
  PQfinish(mydb); //connection closed
  return 0;
}

/*
Name: Type
Class: Abc
Arguments: string &arg,string &input, PGconn* mydb
Return Type: void
Notes: This function takes three arguments of which two are strings which are references to file name and type and the other is a database connection
*/
void Abc :: Type(string &arg, string &input, PGconn* mydb)
{
  string row;
  string dir = "/home/turing/duffin/courses/cs688/data/"+input; 
  ifstream myfile(dir.c_str()); //directory is convered into c style string and is opened using ifstream

  if(myfile.is_open()) //If the file opened sucessfully then this control statement is executed
    {
      while(getline(myfile,row))
	{
	  if(arg == "Type1") //If the file is of Type1 then this controls tatement is exeuted which calls Type1process method
	    {
	      Type1process(row,mydb);
	    }
	  else if(arg == "Type2") //If the file is of Type1 then this controls tatement is exeuted which calls Type1process method
	    {
	      Type2process(row,mydb);
	    }
	  else  //If the file is of Type1 then this controls tatement is exeuted which calls Type1process method
	    {
	      Type3process(row,mydb);
	    }
	}
      myfile.close(); 
    }
  else cout << "Unable to open file"; //Displays if file can't be opened
  
}

/*
Function: Type1process
Arguments: string &line,PGconn* mydb
Return Type: void
Class: Abc
Notes: Type1 files are processed here, this function takes a line from file and database connection as arguments. It then process and insert the data into database
*/
void  Abc :: Type1process(string &line,PGconn* mydb)
{
  PGresult* result; //Variable to hold the result retrieved

  ostringstream query; //Output string stream to hold the query
  istringstream iss(line); //Input string stream to hold the line and used to tokenize
  vector<string> tokens; //Vector to holds the tokens
  string token; //String to tempararily hold the token
  
  while(getline(iss,token,'\t')) //Breaks the string at tabs and pushes it into to the vector
    {
      tokens.push_back(token);
    }  

  //Query to insert the values into the table and retrieve the location id generated upon inserting the query
  query << "INSERT INTO z1789380.location (name,data_id) VALUES ('"<<escape_string(mydb,tokens[1])<<"','"<<escape_string(mydb,tokens[0])<<"') returning location_id;";
  result = PQexec(mydb,query.str().c_str());
  query.str(" ");//clear the stream

  //Stores number of rows and columns retrieved
  int nrows = PQntuples(result);
  int ncols = PQnfields(result);
  string val;
    
  for(int row =0;row < nrows; ++row)
    {
      for(int col = 0; col < ncols; ++col)
	{
	  val = PQgetvalue(result,row,col); //Stores the value into variable val
	}
    }

  PQclear(result);

 
  if(tokens[0].length()>2) //If the length of the first token i.e the data_id is greater than 2 then this control statement is executed to get the location_id of the the region in which it is
    {
      //Query to get the location_id 
      query << "SELECT location_id from z1789380.location where data_id = '"<<escape_string(mydb,tokens[0].substr(0,2))<<"';";
      result = PQexec(mydb,query.str().c_str());
      query.str(" ");

      nrows = PQntuples(result);
      ncols = PQnfields(result);
      string fk_val;
      for(int row = 0;row < nrows; row++)
	{
	  for(int col = 0; col < ncols; col++)
	    {
	      fk_val = PQgetvalue(result,row,col); //retrieved location_id is stored into fk_val variable as it will be used as foreign key for the region
	    }
	}
      
      PQclear(result); //Clears the result variable

      //Query to insert location id, type of region and foreign key into the table
      query << "INSERT INTO z1789380.Region (location_id,type,fk_location_id) VALUES ("<<atoi(val.c_str())<<",'"<<escape_string(mydb,tokens[2])<<"',"<<atoi(fk_val.c_str())<<");";
      PQexec(mydb,query.str().c_str()); //Query is executed
      query.str(" ");//clear the stream
      
    }
  else //If the length of data_id is not greater than 2 then this part of control statement is executed
    { 
      //Query to insert into region table 
      query << "INSERT INTO z1789380.Region (location_id,type) VALUES ("<<atoi(val.c_str())<<",'"<<escape_string(mydb,tokens[2])<<"');";
      PQexec(mydb,query.str().c_str());
      query.str(" ");//clear the stream
    }
  

}      

/*
Name: Type2process
Arguments: string &line, PGconn* mydb
Return Type: void
Class: Abc
Notes: This function is called to process type two files which are outside US files. This function takes as arguments a reference to a string and a database conncection and return nothing
*/
void Abc :: Type2process(string &line,PGconn* mydb)
{
  PGresult* result;
  ostringstream query;
  istringstream iss(line);
  vector<string> tokens;
  string token;

  int nrows,ncols;
  string loc_id,cmp_data_id,fk_loc_id;
  
  while(getline(iss,token,'\t')) //Breaks the line at tabs and stores them in a vector
    {
      tokens.push_back(token);
    }
  
  if(tokens[11] != "") //If the 11th token which is Populated place is not null ten this control statement is executed
    {
      if((atoi( tokens[11].c_str() ) < 3 )&&(tokens[9] == "A" ||tokens[9] == "P"))
	/*
	  If the Populated place value is less than 3 and
	  token 9 which is feature classification is A or P then this control statement  is executed
	*/
	{
	  
	  cmp_data_id = tokens[12]+tokens[13]; //Tokens 12 and 13 which uniquely identifies a region are concatenated

	  // Query to select the location_id of the region given data_id
	  query << "SELECT location_id from z1789380.Location where data_id = '"<<escape_string(mydb,cmp_data_id)<<"';";
	  result = PQexec(mydb,query.str().c_str());
	  query.str(" ");
	  
	  
	  nrows = PQntuples(result);
	  ncols = PQnfields(result);
	  
	  for(int row = 0; row < nrows; ++row)
	    {
	      for(int col = 0; col < ncols; ++col)
		{
		  fk_loc_id = PQgetvalue(result,row,col); //Stores the location_id into variable
		}
	    }
	  PQclear(result);
	  
	  if(fk_loc_id == "") //If there is no location_id i.e there is no information about the region then location_id for country is queried 
	    {
	      query << "SELECT location_id from z1789380.Location where data_id = '"<<escape_string(mydb,tokens[12])<<"';";
	      result = PQexec(mydb,query.str().c_str());
	      query.str(" ");
	      fk_loc_id = PQgetvalue(result,0,0);
	      PQclear(result);
	    }

	  //Query to insert into location table the required details like lat,lon,data_id is built and executed and the location_id 
	  query << "INSERT INTO z1789380.Location(name,latitude,longitude,data_id) values('"<<escape_string(mydb,tokens[24])<<"',"<<atof(tokens[3].c_str())<<","<<atof(tokens[4].c_str())<<",'"<<escape_string(mydb,tokens[2])<<"') returning location_id;";
	  result = PQexec(mydb,query.str().c_str());
	  query.str(" ");
	  
	  nrows = PQntuples(result);
	  ncols = PQnfields(result);
      
	  for(int row = 0; row < nrows; ++row)
	    {
	      for(int col = 0; col < ncols; ++col)
		{
		  loc_id = PQgetvalue(result,row,col); //result retrieved i.e location_id retrieved is stored into variable
		}
	    }
      
	  PQclear(result);

	  //If the region type is administrative then it is stored into region table
	  if(tokens[9] == "A")
	    {
	      query << "INSERT INTO z1789380.Region(location_id,type,fk_location_id) VALUES ("<<loc_id<<",'"<<escape_string(mydb,tokens[10])<<"',"<<fk_loc_id<<");";
	    }
	  else if(tokens[9] == "P")
	    //If the region type is populated then it is stored into city table
	    {
	      query << "INSERT INTO z1789380.City(location_id,fk_location_id) VALUES ("<<loc_id<<","<<fk_loc_id<<");";
	    }
	  
	  PQexec(mydb,query.str().c_str());
	  query.str(" ");
	  
	}
  
    }
}

/*
Name: Type3process
Arguments: string &line, PGconn* mydb
Return Type: void
Class: Abc
Notes: This function is called to process files of type 3 which are inside US. It takes two arguments a reference to a string and a database connection and then processes it and stores into tha database
*/
void Abc :: Type3process(string &line,PGconn* mydb)
{
  istringstream iss(line);
  vector<string> tokens;
  string token;
  ostringstream query;
  PGresult* result;
  string dup;
  
  while(getline(iss,token,'|')) //Breaks the line when encounters |
    {
      tokens.push_back(token); //Stores the tokens into vector
    }
  
  if(tokens[2] == "Populated Place") //We only store cities which are populated places
    {
      //Query to get the location_id given data_id is built and executed
      query << "SELECT location_id from z1789380.Location where data_id = '"<<escape_string(mydb,tokens[0])<<"';";
      result = PQexec(mydb,query.str().c_str());
      query.str(" ");
      
      int nrows = PQntuples(result);
      int ncols = PQnfields(result);

      for(int row = 0; row < nrows; ++row)
	{
	  for(int col = 0; col < ncols; ++col)
	    {
	      dup = PQgetvalue(result,row,col); //If a locatin_id is found then it is stored into variable
	    }
	}
      
      PQclear(result); //clear the result variable

      if(dup == "") //If there are no duplicates then this control statement is executed
	{
	  // Query to get the location_id of the county it is in built and executed
	  query << "SELECT location_id from z1789380.Location where data_id = '"<<"US"+escape_string(mydb,tokens[4]+tokens[6])<<"';";
	  result = PQexec(mydb,query.str().c_str());
	  query.str(" ");

	  nrows = PQntuples(result);
	  ncols = PQnfields(result);
	  string county_id;
	  
	  for(int row = 0; row < nrows; ++row)
	    {
	      for(int col = 0; col < ncols; ++col)
		{
		  county_id = PQgetvalue(result,row,col); //Stores the retrieved id into variable
		}
	    }

	  PQclear(result);
	  
	  if(county_id == "") //If there is no county in the locations table then this control statement is executed
	    {
	      //Query to insert county into location is created and executed which retrieves the location_id generated
	      query << "INSERT INTO z1789380.Location(name,data_id) values ('"<<tokens[5]<<"','"<<"US"+escape_string(mydb,tokens[4]+tokens[6])<<"') returning location_id;";
	      result = PQexec(mydb,query.str().c_str());
	      query.str(" ");

	      nrows = PQntuples(result);
	      ncols = PQnfields(result);

	      for(int row = 0; row < nrows; ++row)
		{
		  for(int col = 0; col < ncols; ++col)
		    {
		      county_id = PQgetvalue(result,row,col); //Location id is stored into the variable
		    }
		}

	      PQclear(result);

	      //Query to get the location_id of the state is executed and is stored in fk_location_id variable
	      query << "SELECT location_id from z1789380.Location where data_id = 'US"<<escape_string(mydb,tokens[4])<<"';";
	      result = PQexec(mydb,query.str().c_str());
	      query.str(" ");

	      nrows = PQntuples(result);
	      ncols = PQnfields(result);
	      string fk_location_id;

	      for(int row = 0; row < nrows; ++row)
		{
		  for(int col = 0; col < ncols; ++col)
		    {
		      fk_location_id = PQgetvalue(result,row,col);
		    }
		}

	      PQclear(result);

	      //Query to insert the county into the region is executed 
	      query << "INSERT INTO z1789380.Region(location_id,type,fk_location_id) values ("<<county_id<<",'county',"<<fk_location_id<<");";
	      PQexec(mydb,query.str().c_str());
	      query.str(" ");
	    }

	  //Now the city details are stored into the locatin table and generated location_id is retrieved and stored into variable location_id
	  query << "INSERT INTO z1789380.Location(name,latitude,longitude,data_id) values ('"<<escape_string(mydb,tokens[1])<<"',"<<tokens[9]<<","<<tokens[10]<<",'"<<tokens[0]<<"') returning location_id;";
	  result = PQexec(mydb,query.str().c_str());
	  query.str(" ");

	  nrows = PQntuples(result);
	  ncols = PQnfields(result);

	  string location_id;

	  for(int row = 0; row < nrows; ++row)
	    {
	      for(int col = 0; col < ncols; ++col)
		{
		  location_id = PQgetvalue(result,row,col);
		}
	    }

	  PQclear(result);


	  //query to store location_id and the foreign key is the city's county idinto city table is executed
	  query << "INSERT INTO z1789380.City(location_id,fk_location_id) values ("<<location_id<<","<<county_id<<");";
	  PQexec(mydb,query.str().c_str());
	  query.str(" ");
	}
      
    }
  
}
/*
Name: Connection
Arguments: None
Return Type: PGconn*
Notes: This function creates a database connection and returns it
*/
PGconn* Abc :: Connection()
{
  PGconn* mydb;
  mydb = PQconnectdb("host = courses dbname = z1789380 password = 1994Feb18"); //Connection established

  if(PQstatus(mydb)!=CONNECTION_OK)
    {
      cerr << "Can't connect to db" << endl;
      cerr << PQerrorMessage(mydb);
      return NULL;
    }
  return mydb;
}

/*
Name: escape_string
Arguments: PGconn* conn, const string & s
Return Type: string
Notes: This function takes a database connection and a constant reference to string as arguments
*/
string  Abc :: escape_string(PGconn * conn , const string & s)
{
  char * buffer;
  buffer = new char[s.length()*2 + 1];

  PQescapeStringConn(conn, buffer, s.c_str(), s.length(), NULL); //Filters the string

  string result = buffer;

  delete [] buffer;
  return result;
}
