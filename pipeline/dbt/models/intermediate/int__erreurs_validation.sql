SELECT * FROM ({{ select_structure_errors(ref('int__structures'), schema_version='v0') }})
UNION ALL
SELECT * FROM ({{ select_service_errors(ref('int__services'), schema_version='v0') }})
UNION ALL
SELECT * FROM ({{ select_structure_errors(ref('int__structures'), schema_version='v1') }})
UNION ALL
SELECT * FROM ({{ select_service_errors(ref('int__services'), schema_version='v1') }})
