import sys
from ingestion.mock_generator import mock_generator
from dataclasses import asdict
sys.path.insert(0, 'ingestion')


def test_data_mock_generator():
    gen = mock_generator()
    circulation = next(gen)
    assert circulation.gare_depart != circulation.gare_arrivee
    assert circulation.etat == "a_l_heure" or circulation.etat == "en_retard"
    assert circulation.heure_arrivee > circulation.heure_depart
    for _ in asdict(circulation).values():
        assert _ is not None
