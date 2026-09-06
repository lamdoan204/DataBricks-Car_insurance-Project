-- 1. Tạo Database mới
\echo '---------------------------------------'
\ehco 'Bắt đầu tạo Database car_insurance_db '
CREATE DATABASE if not exist car_insurance_db;
\echo 'Tạo Database: car_insurance_db thành công'


\c car_insurance_db
\echo '---------------------------------------'
\ehco 'Bắt đầu tạo Table customers '
CREATE TABLE if not exist customers (
    customer_id NUMERIC(10, 1) PRIMARY KEY, 
    name VARCHAR(150),                       
    date_of_birth DATE,                      
    borough VARCHAR(50),                     
    neighborhood VARCHAR(100),               
    zip_code VARCHAR(10)                     
);
\echo 'Tạo Table: customers thành công'
\echo '---------------------------------------'
\echo 'Bắt đầu tạo Table policies '
CREATE TABLE if not exist policies (
    policy_no VARCHAR(50) PRIMARY KEY,       
    cust_id NUMERIC(10, 1) NOT NULL,          
    policy_type VARCHAR(20),                  
    pol_issue_date DATE,                      
    pol_eff_date DATE,                        
    pol_expiry_date DATE,                     
    make VARCHAR(50),                         
    model VARCHAR(50),                        
    model_year NUMERIC(4, 0),                 
    chassis_no VARCHAR(50),                   
    use_of_vehicle VARCHAR(30),               
    product VARCHAR(50),                      
    sum_insured NUMERIC(12, 2) DEFAULT 0.00,  
    premium NUMERIC(12, 2) DEFAULT 0.00,      
    deductable NUMERIC(12, 2) DEFAULT 0.00,   

    CONSTRAINT fk_policies_customers 
        FOREIGN KEY (cust_id) 
        REFERENCES customers(customer_id)
        ON UPDATE CASCADE
);
\echo 'Tạo Table: policies thành công'

\echo '---------------------------------------'
\ehco 'Bắt đầu tạo Table insurance_claims '
CREATE TABLE if not exist insurance_claims (

    claim_no VARCHAR(50) PRIMARY KEY,
    policy_no VARCHAR(50) NOT NULL,
    claim_date DATE NOT NULL,
    months_as_customer INT,
    
    injury NUMERIC(12, 2) DEFAULT 0.00,
    property NUMERIC(12, 2) DEFAULT 0.00,
    vehicle NUMERIC(12, 2) DEFAULT 0.00,
    total NUMERIC(12, 2) DEFAULT 0.00, 
    
    collision_type VARCHAR(50),
    number_of_vehicles_involved INT,
    
    age NUMERIC(4, 1), 
    insured_relationship VARCHAR(50),
    license_issue_date DATE,
    
    accident_date DATE, 
    accident_hour INT CHECK (accident_hour BETWEEN 0 AND 23), 
    accident_type VARCHAR(50), 
    severity VARCHAR(50),
    
    number_of_witnesses INT DEFAULT 0,
    suspicious_activity BOOLEAN,

    CONSTRAINT fk_claims_policies 
        FOREIGN KEY (policy_no) 
        REFERENCES policies(policy_no)
        ON UPDATE CASCADE
);
\echo 'Tạo Table: insurance_claims thành công'

