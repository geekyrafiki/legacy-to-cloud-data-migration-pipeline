SELECT COUNT(*) AS null_patient_ids
FROM warehouse.dim_patient
WHERE patient_id IS NULL;

SELECT patient_id, COUNT(*) AS duplicate_count
FROM warehouse.dim_patient
GROUP BY patient_id
HAVING COUNT(*) > 1;

SELECT a.appointment_id
FROM warehouse.fact_appointment a
LEFT JOIN warehouse.dim_patient p
    ON a.patient_id = p.patient_id
WHERE p.patient_id IS NULL;

SELECT b.billing_id
FROM warehouse.fact_billing b
LEFT JOIN warehouse.dim_patient p
    ON b.patient_id = p.patient_id
WHERE p.patient_id IS NULL;
