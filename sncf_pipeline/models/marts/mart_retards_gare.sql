{{ config(materialized='table') }}

select 
  date_circulation,
  gare_arrivee,
  AVG(retard) as retard_moyen

from {{ ref('stg_retards')}}

GROUP BY gare_arrivee, date_circulation