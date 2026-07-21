import jellyfish


def test_jaro_winkler_scores_near_variants_high():
    # spelling slip
    assert jellyfish.jaro_winkler_similarity("nikolai volkov", "nykolai volkov") > 0.9
    # dropped last char
    assert jellyfish.jaro_winkler_similarity("tariq khan", "tariq kha") > 0.9


def test_jaro_winkler_separates_unrelated_names():
    assert jellyfish.jaro_winkler_similarity("james johnson", "boris volkov") < 0.7


def test_lowercasing_absorbs_case_and_is_stable():
    a = jellyfish.jaro_winkler_similarity("Chen Sokolov".lower(), "chen sokolov")
    assert a == 1.0
