SELECT 'legacy.legacy_patients' AS table_name, COUNT(*) AS row_count FROM legacy.legacy_patients
UNION ALL
SELECT 'warehouse.dim_patient', COUNT(*) FROM warehouse.dim_patient
UNION ALL
SELECT 'legacy.legacy_appointments', COUNT(*) FROM legacy.legacy_appointments
UNION ALL
SELECT 'warehouse.fact_appointment', COUNT(*) FROM warehouse.fact_appointment
UNION ALL
SELECT 'legacy.legacy_billing_transactions', COUNT(*) FROM legacy.legacy_billing_transactions
UNION ALL
SELECT 'warehouse.fact_billing', COUNT(*) FROM warehouse.fact_billing;
