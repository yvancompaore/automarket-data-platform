-- Test : reliability_score toujours entre 0 et 1
select count(*) as failures
from {{ ref('int_parts_enriched') }}
where reliability_score < 0 or reliability_score > 1
