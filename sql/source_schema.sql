CREATE SCHEMA IF NOT EXISTS legacy;

CREATE TABLE IF NOT EXISTS legacy.legacy_patients (
    legacy_patient_id INT,
    fname VARCHAR(100),
    lname VARCHAR(100),
    dob VARCHAR(50),
    sex VARCHAR(10),
    email VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS legacy.legacy_appointments (
    appointment_id INT,
    patient_id INT,
    appointment_date VARCHAR(50),
    appointment_status VARCHAR(50),
    provider_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS legacy.legacy_billing_transactions (
    billing_id INT,
    patient_id INT,
    billing_date VARCHAR(50),
    amount VARCHAR(50),
    procedure_code VARCHAR(50)
);
