-- Test : aucun prix négatif dans le catalogue enrichi
select count(*) as failures
from {{ ref('int_parts_enriched') }}
where price_eur < 0
