CREATE TABLE table_m3(
	"Date" DATE, 
	"Day" INT, 
	"Month" VARCHAR(50), 
	"Year" INT, 
	"Customer_Age" INT, 
	"Age_Group" VARCHAR(50),
    "Customer_Gender" VARCHAR(50), 
	"Country" VARCHAR(50), 
	"State" VARCHAR(50), 
	"Product_Category" VARCHAR(50),
    "Sub_Category" VARCHAR(50), 
	"Product" VARCHAR(50), 
	"Order_Quantity" INT, 
	"Unit_Cost" INT, 
	"Unit_Price" INT,
    "Profit" INT,
	"Cost" INT, 
	"Revenue" INT
)

COPY table_m3
FROM 'C:\Users\LENOVO\p2-ftds008-hck-m3-taaratheresia\P2M3_taara_data_raw.csv'
DELIMITER ','
CSV HEADER;

SELECT * FROM table_m3