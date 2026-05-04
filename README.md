# SNCF Pipeline — Lambda Architecture

Pipeline de données ferroviaires combinant streaming temps réel et batch historique,
conçu pour analyser la ponctualité des trains SNCF sur la région Hauts-de-France.

---

## Architecture

```
API SNCF Open Data
      │
      ├── STREAMING → Pub/Sub → Dataflow (Beam) → BigQuery (raw_streaming)
      └── BATCH     → Cloud Storage → Dataflow (Beam) → BigQuery (raw_batch)
                              │
                      Great Expectations (data quality)
                              │
                            dbt (staging → mart)
                              │
                       BigQuery (datamart)
                              │
                       Looker Studio (dashboard)

Orchestration : Airflow | CI/CD : GitHub Actions
```

---

## Stack technique

| Composant | Technologie |

| Ingestion streaming | Python + Pub/Sub |
| Ingestion batch | Python + Cloud Storage |
| Transformation | Apache Beam / Dataflow |
| Stockage | BigQuery (partitionné + clusterisé) |
| Transformation analytique | dbt |
| Data quality | Great Expectations |
| Orchestration | Airflow |
| Visualisation | Looker Studio |
| CI/CD | GitHub Actions |

---

## Structure du projet

```
projet_sncf/
├── .github/workflows/          # ci.yml 
├── ingestion/
│   ├── mock_generator.py       # Générateur de données ferroviaires mock
│   ├── publisher_streaming.py  # Publication Pub/Sub (1 message par train)
│   └── batch_fetcher.py        # Récupération API (à l'avenir) + dépôt Cloud Storage
├── beam/
│   ├── pipeline_streaming.py   # Pipeline Beam streaming (Pub/Sub → BigQuery)
│   └── pipeline_batch.py       # Pipeline Beam batch (GCS → BigQuery)
├── sncf_pipeline/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_retards.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── mart_ponctualite.sql
│   │       ├── mart_retards_gare.sql
│   └── dbt_project.yml
├── great_expectations/
│   ├── expectations/
│   │   └── raw_batch_suite.json
│   └── checkpoints/
│       └── sncf_checkpoint.yml
├── dags/
│   └── sncf_pipeline.py        # DAG Airflow quotidien
├── tests/
│   └── test_pipeline.py        # Tests unitaires Beam
├── requirements.txt
└── README.md
```

---


## Modèle de données

| Table | Couche | Partition | Cluster | Description |
|---|---|---|---|---|
| raw_batch | RAW | date_circulation | ligne_id | Données historiques SNCF |
| raw_streaming | RAW | event_time | — | Événements temps réel Pub/Sub |
| ref_lignes | REF | — | — | Référentiel des lignes ferroviaires |
| ref_gares | REF | — | — | Référentiel des gares |
| stg_retards | STAGING | — | — | Nettoyage + catégorisation (VIEW dbt) |
| mart_ponctualite | MART | — | — | Taux ponctualité par ligne/jour (TABLE) |
| mart_retards_gare | MART | — | — | Retards moyens par gare/jour (TABLE) |

---

## DAG Airflow

Scheduling : quotidien à 2h00

```
fetch_sncf_api
      │
run_dataflow_batch
      │
check_data_quality (Great Expectations)
      │              │
   succès          échec → send_alert_mail
      │
run_dbt_staging
      │
run_dbt_marts
      │
run_dbt_tests
```

---
## Données

En attente de livraison de la clé API SNCF Open Data, l'ensemble des données
utilisées sont fictives et générées via `ingestion/mock_generator.py`.
Ce script simule des flux de trains réalistes (lignes, gares, retards, statuts)
et sera remplacé par les appels API réels dès réception des accès.


---

## Statut d'avancement

- [x] Structure du projet
- [x] Mock data generator
- [x] Publisher Pub/Sub
- [x] Pipeline Beam streaming
- [x] Pipeline Beam batch
- [x] Modèles dbt
- [ ] Great Expectations
- [ ] DAG Airflow
- [ ] Dashboard Looker Studio
- [x] CI/CD GitHub Actions

---

## Contexte

Ce projet reproduit en stack cloud moderne (GCP, dbt, Airflow) l'architecture
on-premise que j'ai conçue durant mon alternance au Conseil Régional Hauts-de-France,
où je fiabilisais les données de comptage voyageurs SNCF pour évaluer les performances
contractuelles de l'opérateur ferroviaire.

---

## Auteur

**Theo Perus** — Data Engineer

- LinkedIn : [linkedin.com/in/theoperus](https://www.linkedin.com/in/theo-perus/)
- Portfolio GCP e-commerce : [github.com/TheoPrs/gcp_pipeline_ecommerce](https://github.com/TheoPrs/gcp_pipeline_ecommerce)