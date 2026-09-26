import math

import pytest

from tests import support as m


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        (math.nan, None),
        (math.inf, None),
        (-1.0, None),
        (0.0, None),
        (0.01, 0.0),
        (0.05, 0.1),
        (73.44, 73.4),
        (73.45, 73.5),
        (100.9, 100.0),
    ],
)
def test_clamp_0_100_characterization(value, expected):
    assert m.clamp_0_100(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("abc", None),
        (0, None),
        (8.95, 90),
        (99.95, 1000),
        (100, 1000),
    ],
)
def test_score_to_tenths_characterization(value, expected):
    assert m.score_to_tenths(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, (None, None)),
        (7, (7.0, None)),
        ("7/10", (7.0, 10.0)),
        ("85%", (85.0, None)),
        ("n/a", (None, None)),
        ("nan", (None, None)),
        ("inf", (None, None)),
        ("8/0", (8.0, 0.0)),
    ],
)
def test_parse_value_and_scale_characterization(value, expected):
    assert m.parse_value_and_scale(value) == expected


@pytest.mark.parametrize(
    "numeric, denominator, hint, expected",
    [
        (None, None, None, None),
        (7.3, 10.0, None, 73.0),
        (4.2, None, 5, 84.0),
        (8.0, None, 5, 80.0),
        (8.0, None, 10, 80.0),
        (4.0, None, 4, 100.0),
        (3.5, None, 4, 87.5),
        (4.0, None, None, 80.0),
        (8.0, None, None, 80.0),
        (67.0, None, None, 67.0),
    ],
)
def test_scale_to_100_characterization(numeric, denominator, hint, expected):
    assert m.scale_to_100(numeric, denominator, hint) == expected


def test_parse_tmdb_vote_average_characterization():
    assert m.parse_tmdb_vote_average(None) is None
    assert m.parse_tmdb_vote_average({"vote_average": "7.3"}) == 73.0
    assert m.parse_tmdb_vote_average({"vote_average": 0}) is None


def test_parse_mdblist_ratings_primary_paths_and_source_overrides():
    payload = {
        "Metascore": "84",
        "imdbRating": "7.3",
        "ratings": [
            {"source": "Rotten Tomatoes", "score": "95"},
            {"source": "Rotten Tomatoes Audience", "value": "88%"},
            {"source": "Metacritic User", "value": "8.1/10"},
            {"source": "Letterboxd", "value": "3.6/5"},
            {"source": "Trakt", "value": "79"},
            {"source": "IMDb", "value": "6.9"},
            {"source": "TMDB", "value": "8.2/10"},
            {"source": "MyAnimeList", "value": "8.0/10"},
            {"source": "Roger-Ebert", "value": "4/5"},
        ],
        "score": "77",
    }

    assert m.parse_mdblist_ratings(payload) == {
        "MC": 84.0,
        "IM": 73.0,
        "RT": 95.0,
        "PC": 88.0,
        "LB": 72.0,
        "TR": 79.0,
        "TM": 82.0,
        "ML": 80.0,
        "RE": 80.0,
    }


def test_parse_mdblist_ratings_treats_source_scores_as_normalized():
    payload = {
        "ratings": [
            {"source": "IMDb", "value": 0.8, "score": 8},
            {"source": "Rotten Tomatoes", "value": 10, "score": 10},
            {"source": "Popcorn", "value": 9, "score": 9},
            {"source": "Metacritic", "value": 9, "score": 9},
            {"source": "Trakt", "value": 6, "score": 6},
            {"source": "TMDB", "value": 7, "score": 7},
            {"source": "Letterboxd", "value": 0.4, "score": 8},
            {"source": "MyAnimeList", "value": 0.9, "score": 9},
        ],
    }

    assert m.parse_mdblist_ratings(payload) == {
        "IM": 8.0,
        "RT": 10.0,
        "PC": 9.0,
        "MC": 9.0,
        "TR": 6.0,
        "TM": 7.0,
        "LB": 8.0,
        "ML": 9.0,
    }


def test_parse_mdblist_ratings_uses_source_specific_value_scales_without_score():
    payload = {
        "ratings": [
            {"source": "IMDb", "value": "6.5"},
            {"source": "Tomatoes", "value": "10"},
            {"source": "Popcorn", "value": "9"},
            {"source": "Trakt", "value": "6"},
            {"source": "TMDB", "value": "7"},
            {"source": "Letterboxd", "value": "4.4"},
            {"source": "MyAnimeList", "value": "7.2"},
            {"source": "RogerEbert", "value": "4"},
        ],
        "score": "7",
    }

    assert m.parse_mdblist_ratings(payload) == {
        "IM": 65.0,
        "RT": 10.0,
        "PC": 9.0,
        "TR": 6.0,
        "TM": 7.0,
        "LB": 44.0,
        "ML": 72.0,
        "RE": 100.0,
    }


def test_parse_mdblist_ratings_global_score_is_not_a_trakt_rating():
    assert m.parse_mdblist_ratings({"score": "7"}) == {}


def test_parse_mdblist_ratings_imdb_without_trakt_does_not_invent_trakt():
    payload = {
        "ratings": [
            {"source": "Internet Movie Database", "value": "7.8/10"},
        ],
        "score": "74",
    }

    assert m.parse_mdblist_ratings(payload) == {
        "IM": 78.0,
    }


def test_parse_mdblist_ratings_metacritic_user_like_scores_are_guarded():
    payload = {
        "ratings": [
            {"source": "Metacritic", "value": "9.0"},
            {"source": "Metacritic", "value": "88"},
        ]
    }

    assert m.parse_mdblist_ratings(payload) == {
        "MC": 88.0,
    }


@pytest.mark.parametrize("payload", [None, {}])
def test_parse_mdblist_ratings_empty_payload_returns_empty_dict(payload):
    assert m.parse_mdblist_ratings(payload) == {}
