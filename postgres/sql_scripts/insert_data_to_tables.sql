\c car_insurance_db;
\echo "Bắt đầu insert dữ liệu vào các bảng"
\echo "-----------------------------------"
SET datestyle = 'ISO, DMY';

\echo 'Bắt đầu insert dữ liệu vào Table: customers'
\copy customers(customer_id, date_of_birth, borough, neighborhood, zip_code, name) FROM '../data/customers.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL 'null')
\echo '-----------------------------------'
\echo 'Insert dữ liệu vào Table: customers thành công'
\echo '-----------------------------------'
\echo 'Bắt đầu insert dữ liệu vào Table: policies'
\copy policies(policy_no, cust_id, policy_type, pol_issue_date, pol_eff_date, pol_expiry_date, make, model, model_year, chassis_no, use_of_vehicle, product, sum_insured, premium, deductable) FROM '../data/policies.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',')
\echo '-----------------------------------'
\echo 'Insert dữ liệu vào Table: policies thành công'
\echo '-----------------------------------'
\echo 'Bắt đầu insert dữ liệu vào Table: insurance_claims'
\copy insurance_claims(claim_no, policy_no, claim_date, months_as_customer, injury, property, vehicle, total, collision_type, number_of_vehicles_involved, age, insured_relationship, license_issue_date, accident_date, accident_hour, accident_type, severity, number_of_witnesses, suspicious_activity) FROM '../data/claims.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',')
\echo '-----------------------------------'
\echo 'Insert dữ liệu vào Table: insurance_claims thành công'
