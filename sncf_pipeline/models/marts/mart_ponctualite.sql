{{ config(materialized='table') }}

with tauxponctualite as (
  select 
    count(circulation_id) as total,
    countif(retard>5) as nb_retards,
    date_circulation,
    ligne_id

  from {{ ref('stg_retards')}}

  GROUP BY ligne_id, date_circulation
)
select 
  ligne_id,
  date_circulation,
  total,
  nb_retards,
  ROUND(((total - nb_retards) * 100.0 / total), 2) as taux_ponctualite
from tauxponctualite