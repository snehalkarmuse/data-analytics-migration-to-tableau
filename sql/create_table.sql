create table flights
(
	Id  serial primary key,
	Year int,
	Month  int,
	Day_Of_Month int,
	Day_of_Week int,
	Dep_Time float,
	CRS_Dep_Time int,
	Arr_Time  float,
	CRS_Arr_Time  int,
	Unique_Carrier  varchar(30),
	Flight_Num int,
	Tail_Num  float ,
	Actual_Elapsed_Time  float ,
	CRS_Elapsed_Time  int,
	Arr_Delay  float,
	Dep_Delay  float ,
	Origin  varchar(30),
	Dest  varchar(30),
	Distance  float,
	Cancelled  int,
	Diverted  int,
	Air_Time float,
	Taxi_In  float ,
	Taxi_Out  float 
);