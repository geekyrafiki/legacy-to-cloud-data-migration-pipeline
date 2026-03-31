CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_patient (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(20),
    email VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_appointment (
    appointment_id INT PRIMARY KEY,
    patient_id INT NOT NULL REFERENCES warehouse.dim_patient(patient_id),
    appointment_date TIMESTAMP,
    appointment_status VARCHAR(50),
    provider_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_billing (
    billing_id INT PRIMARY KEY,
    patient_id INT NOT NULL REFERENCES warehouse.dim_patient(patient_id),
    billing_date DATE,
    amount NUMERIC(12,2),
    procedure_code VARCHAR(50)
);
