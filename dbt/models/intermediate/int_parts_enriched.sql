-- int_parts_enriched
-- Enrichissement du catalogue :
-- - Score de fiabilité multi-sources (logique Parts.IO)
-- - Détection des prix aberrants (IQR)
-- - Couverture catalogue par marque/catégorie

{{
  config(
    materialized='table',
    tags=['intermediate', 'catalog']
  )
}}

with catalog as (
    select * from {{ ref('stg_parts_catalog') }}
),

-- Prix médian et IQR par catégorie pour détection outliers
price_stats as (
    select
        category,
        brand_type,
        percentile_cont(0.25) within group (order by price_eur) as q1,
        percentile_cont(0.50) within group (order by price_eur) as median_price,
        percentile_cont(0.75) within group (order by price_eur) as q3,
        avg(price_eur)                                           as avg_price,
        stddev(price_eur)                                        as stddev_price,
        count(*)                                                 as parts_count
    from catalog
    where price_eur is not null
    group by 1, 2
),

-- Compte les sources pour chaque fingerprint (proxy fiabilité)
source_coverage as (
    select
        _fingerprint,
        count(distinct source)  as nb_sources,
        count(distinct supplier) as nb_suppliers,
        min(price_eur)          as min_price,
        max(price_eur)          as max_price,
        avg(price_eur)          as avg_price_cross_source,
        -- Variation de prix cross-sources (plus c'est haut, moins c'est fiable)
        case
            when min(price_eur) > 0
            then round((max(price_eur) - min(price_eur)) / min(price_eur), 4)
            else null
        end                     as price_spread_ratio
    from catalog
    where _fingerprint is not null
    group by 1
),

enriched as (
    select
        c.part_id,
        c.part_number,
        c.brand,
        c.brand_type,
        c.category,
        c.sub_category,
        c.description,
        c.price_eur,
        c.stock_quantity,
        c.availability,
        c.is_available,
        c.price_tier,
        c.supplier,
        c.source,
        c.quality_score,
        c._fingerprint,
        c.ingested_year,
        c.ingested_month,

        -- Statistiques prix de référence
        ps.median_price                 as category_median_price,
        ps.avg_price                    as category_avg_price,
        ps.q1                           as category_q1,
        ps.q3                           as category_q3,
        ps.parts_count                  as category_parts_count,

        -- Détection prix aberrant (IQR * threshold)
        case
            when c.price_eur < (ps.q1 - {{ var('price_outlier_threshold') }} * (ps.q3 - ps.q1))
              or c.price_eur > (ps.q3 + {{ var('price_outlier_threshold') }} * (ps.q3 - ps.q1))
            then true
            else false
        end                             as is_price_outlier,

        -- Ratio par rapport au prix médian catégorie
        case
            when ps.median_price > 0
            then round(c.price_eur / ps.median_price, 4)
            else null
        end                             as price_vs_median_ratio,

        -- Enrichissement multi-sources
        coalesce(sc.nb_sources, 1)      as nb_sources,
        coalesce(sc.nb_suppliers, 1)    as nb_suppliers,
        sc.price_spread_ratio,

        -- Score de fiabilité composite (logique Parts.IO)
        -- Plus nb_sources élevé → plus fiable
        -- Plus price_spread_ratio faible → plus cohérent
        -- Plus quality_score élevé → meilleure donnée source
        round(
            (
                least(coalesce(sc.nb_sources, 1) / 3.0, 1.0) * 0.40   -- poids sources
                + c.quality_score                             * 0.35   -- poids qualité brute
                + case
                    when sc.price_spread_ratio is null         then 0.25
                    when sc.price_spread_ratio < 0.05          then 0.25
                    when sc.price_spread_ratio < 0.15          then 0.15
                    else                                            0.05
                  end                                                   -- poids cohérence prix
            ),
            4
        )                               as reliability_score

    from catalog c
    left join price_stats ps
        on c.category = ps.category and c.brand_type = ps.brand_type
    left join source_coverage sc
        on c._fingerprint = sc._fingerprint
)

select * from enriched
