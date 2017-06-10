/*
CS688 Assignment 3
Name: Saiteja Yagni
zid: z1789380
Instructor: Dr Duffin
Due Date: 16 November 2016
*/
/*Program to calculate the distance between two cities.*/

/* This Program takes input as two city names and calculates distance between them if there is more than one city by the same name then it will calculate the distance between all of them */

//Below are the header files 
#include<iostream> 
#include<fstream>
#include<postgresql/libpq-fe.h>
#include<string>
#include<math.h>
#include<vector>
#include<sstream>
#include<stdlib.h>

using std :: cin;
using std :: cout;
using std :: endl;
using std :: string;
using std :: ostringstream;
using std :: vector;
using std :: cerr;

/*Below are the functions that will be used to find the distance between cities*/
string askUser(string &number);
vector<string> getCityIds(PGconn* mydb,const string &city);
double getLongitude(const string &city);
double distance(double,double,double,double);
string escape_string(PGconn* conn,const string &s);
PGconn* connection();
void displayDist(PGconn* mydb, const string &city_id1,const string &city_id2);
string cityDetails(const string &city_id,PGconn* mydb);

/*
Function Name: main
Return Type: int
Arguments: None
Description: Program execution starts from here. User prompts  for entering the names of cities  
*/
int main()
{
  string firstCity = "first";
  string secondCity = "second";

  //Function calls to get the names of cities 
  firstCity = askUser(firstCity); 
  secondCity = askUser(secondCity);

  //Function call to create a connection
  PGconn* mydb = connection();

  //Method calls to get the city ids
  vector<string> firstCityIds = getCityIds(mydb,firstCity);
  vector<string> secondCityIds = getCityIds(mydb,secondCity);
  
  /*
    Below are the statements that are used to calculate the distance between the cities. 
    This will help in calculating the distance from different places with the same name
   */


  for(unsigned int i = 0; i < firstCityIds.size(); ++i)
    {
      for(unsigned int j = 0; j < secondCityIds.size(); ++j)
	{
	  displayDist(mydb,firstCityIds[i],secondCityIds[j]); //Method call to get the distance between two places
	}
    }
  
  return 0;
}

/*
Name: askUser
Return Type: string
Arguments: string &number
Notes: This function takes string as an argument and the prompts user to enter the name of the city and returns it
*/
string askUser(string &number)
{
  string cityName;
  cout << "Enter the name of "<<number<<" city!!"<<endl; //Prompts user to enter the name
  getline(cin,cityName); //Get the input from the user
  return cityName; 
}

/*
Name: getCityIds
Return Type: vector<string>
Arguments: PGconn* mydb,string &city
Notes: This function takes a database connection and and a string as arguments and then gets the ids of the cities wit name and stores them in to a vector of string and returns it
*/
vector<string> getCityIds(PGconn* mydb,const string &city)
{
  PGresult* result;
  ostringstream query; //Varible to hold the postgres query
  vector<string> loc_ids; //Holds the location ids
  
  query << "SELECT location_id from z1789380.Location where name ='"<<escape_string(mydb,city)<<"';"; //Query that has to be executed on the postgres database
  result = PQexec(mydb,query.str().c_str()); //Query is executed and retrieved values are stored in the result variable
  query.str(" ");

  unsigned int nrows = PQntuples(result); //number of rows retrieved
  unsigned int ncols = PQnfields(result); //number of columns retrieved
  
  for(unsigned int row = 0; row < nrows; ++row)
    {
      for(unsigned int col = 0; col <ncols; ++col)
	{
	  loc_ids.push_back(PQgetvalue(result,row,col));//Retrieved values are stored in the vector
	}
    }
  
  PQclear(result);
  
  return loc_ids; 
}


/*
Name: displayDist
Return Type: void
Arguments: PGconn* mydb,string &city_id1,string &city_id2
*/
void displayDist(PGconn* mydb,const string &city_id1,const string &city_id2)
{
  if(city_id1 != "" && city_id2 != "")
    {
  PGresult* result;
  ostringstream query;
  query << "SELECT latitude,longitude from z1789380.Location where location_id = "<<city_id1<<";";
  result = PQexec(mydb,query.str().c_str());
  query.str(" ");
  string lat1,lon1,lat2,lon2; //Variables to hold the latitude and longitude values 

  //stores the latitude and longitude values of the first city
  lat1 = PQgetvalue(result,0,0);
  lon1 = PQgetvalue(result,0,1);

  PQclear(result);

  query << "SELECT latitude,longitude from z1789380.Location where location_id ="<<city_id2<<";";
  result = PQexec(mydb,query.str().c_str());
  query.str(" ");

  //Stores the latitude amd longitude values of the second city
  lat2 = PQgetvalue(result,0,0);
  lon2 = PQgetvalue(result,0,1);

  PQclear(result);

  if(lat1 !="" && lat2 != "")
    {
  
    string city1_details = cityDetails(city_id1,mydb);
    string city2_details = cityDetails(city_id2,mydb);

//Displays it on to the console
  cout << "Distance between the cities "<<city1_details<<" and "<<city2_details<<" is "<<distance(atof(lat1.c_str()),atof(lon1.c_str()),atof(lat2.c_str()),atof(lon2.c_str()))<<" miles"<<endl;
    }
    }

}

/*
Name: cityDetails
Arguments: const string &city_id, PGconn* mydb
Return Type: string
Notes: This function takes a constant reference to a string and a database connection as arguments and returns a string with details of the city whose location_id has been sent as argument
*/
string cityDetails(const string &city_id,PGconn* mydb)
{
  
  PGresult* result;
  ostringstream query;
  string line = "City of ";
  string temp_type,temp_city_id = city_id;

  // Query to retrieve the locationid of the region in which the city is built and executed below
  query << "select fk_location_id from z1789380.City where location_id = "<<temp_city_id<<";";
  result = PQexec(mydb,query.str().c_str());
  query.str(" ");

  temp_city_id = PQgetvalue(result,0,0); 

  PQclear(result);
  
  // Query to retrieve the name of city is built and executed here
  query << "select name from z1789380.Location where location_id = "<<city_id<<";";
  result = PQexec(mydb,query.str().c_str());
  query.str();

  line = line + string(PQgetvalue(result,0,0)); //Name of the city is concatenated to the line variable

  PQclear(result);
  
  do
    {
      //Query created and executed for retrieving names from location table
      query << "select name from z1789380.Location where location_id = "<<temp_city_id<<";";
      result = PQexec(mydb,query.str().c_str());
      query.str(" ");
      
      string name;

      int nrows = PQntuples(result);
      int ncols = PQnfields(result);

      for(int row = 0; row < nrows; ++row)
	{
	  for(int col = 0; col <ncols; ++col)
	    {
	      
	      name = PQgetvalue(result,row,col); //name is added to the variable
	    }
	}
      PQclear(result);
      
      //Query that retrieves type and key for the region it is associated with it
      query << "select fk_location_id,type from z1789380.Region where location_id = "<<temp_city_id<<";";
      result = PQexec(mydb,query.str().c_str());
      query.str(" ");
      
      //Retrieved results are stored into vector and a variable
      temp_city_id = PQgetvalue(result,0,0);
      temp_type = PQgetvalue(result,0,1);
      
      PQclear(result);
      
      line = line+" in "+name+" "+temp_type; //Line is concatenated with the name and type of the region
      
    }while(temp_city_id!=""); //Untill there is no more foreign key associated with it this loop will be executed
  
  return line;
}


/*
Name: distance
Return Type: double
Arguments: double lat1, double lon1, double lat2, double lon2
Notes: Takes latitude and longitude values of two cities and calculates the distance between them and returns it
*/
double distance (double lat1, double lon1, double lat2, double lon2)
{
  const double RADIUS = 3958.748;
  double b = (90 - lat1) * M_PI / 180.0;
  double c = (90 - lat2) * M_PI / 180.0;
  
  double a = fabs(lon1 - lon2);
  if(a > 180.0)
    a = 360 - a;
  a *= M_PI / 180.0;
  double d = acos(cos(b)*cos(c) + sin(b)*sin(c)*cos(a)) * RADIUS;
  return d;
}

/*
Name: escape_string
Return Type: string
Arguments: PGconn* conn, const string &s
Notes: This function takes a database connection and a string variable as inputs and returns the string
*/
string escape_string(PGconn * conn , const string & s)
{
  char * buffer;
  buffer = new char[s.length()*2 + 1];

  PQescapeStringConn(conn, buffer, s.c_str(), s.length(), NULL); //Escape string connection filters teh string and returns the value back

  string result = buffer; //filtered value is stored in the result whic is returned

  delete [] buffer;
  return result;
}

/*
Name: connection
Return Type: PGconn*
Arguments: None
Notes: Creates a databse connection and returns it
*/
PGconn* connection()
{
  PGconn* mydb;
  mydb = PQconnectdb("host = courses dbname = z1789380 password = 1994Feb18"); //Connection created

  if(PQstatus(mydb)!=CONNECTION_OK) //checks if the connection is established or not
    {
      cerr << "Can't connect to db" << endl;
      cerr << PQerrorMessage(mydb);
      return NULL;
    }
  return mydb;
}
