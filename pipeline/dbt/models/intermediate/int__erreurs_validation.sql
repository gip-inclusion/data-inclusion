SELECT * FROM ({{ select_structure_errors(ref('int__union_structures__enhanced')) }})
UNION ALL
SELECT * FROM ({{ select_service_errors(ref('int__union_services__enhanced')) }})
