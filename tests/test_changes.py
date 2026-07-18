from datetime import date

from src.database import classify_changes


OLD = date(2026, 7, 18)
NEW = date(2026, 7, 19)


def test_new_course_is_added_immediately():
    changes = classify_changes(set(), {("course", NEW)}, {"course": "Course"})

    assert changes.additions == {("course", NEW)}
    assert changes.destructive == ()


def test_single_date_change_is_an_update_candidate():
    changes = classify_changes({("course", OLD)}, {("course", NEW)}, {"course": "Course"})

    assert not changes.additions
    assert len(changes.destructive) == 1
    candidate = changes.destructive[0]
    assert candidate.change_type == "updated"
    assert candidate.old_date == OLD
    assert candidate.new_date == NEW


def test_missing_result_is_a_removal_candidate():
    changes = classify_changes({("course", OLD)}, set(), {"course": "Course"})

    assert changes.destructive[0].change_type == "removed"
    assert changes.destructive[0].old_date == OLD


def test_multiple_dates_are_compared_as_exact_pairs():
    changes = classify_changes(
        {("course", OLD), ("course", NEW)},
        {("course", NEW)},
        {"course": "Course"},
    )

    assert not changes.additions
    assert len(changes.destructive) == 1
    assert changes.destructive[0].change_type == "removed"
    assert changes.destructive[0].old_date == OLD


def test_unchanged_results_produce_no_events():
    pairs = {("course", OLD)}
    changes = classify_changes(pairs, pairs, {"course": "Course"})

    assert not changes.additions
    assert not changes.destructive
