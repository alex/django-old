from __future__ import with_statement

from operator import attrgetter

from django.test import TestCase, skipUnlessDBFeature

from models import Country, Restaurant, Pizzeria, State


class BulkCreateTests(TestCase):
    def setUp(self):
        self.data = [
            Country(name="United States of America", iso_two_letter="US"),
            Country(name="The Netherlands", iso_two_letter="NL"),
            Country(name="Germany", iso_two_letter="DE"),
            Country(name="Czech Republic", iso_two_letter="CZ")
        ]

    def test_simple(self):
        Country.objects.bulk_create(self.data)
        self.assertQuerysetEqual(Country.objects.order_by("-name"), [
            "United States of America", "The Netherlands", "Germany", "Czech Republic"
        ], attrgetter("name"))

    @skipUnlessDBFeature("has_bulk_insert")
    def test_efficiency(self):
        with self.assertNumQueries(1):
            Country.objects.bulk_create(self.data)

    def test_inheritance(self):
        Restaurant.objects.bulk_create([
            Restaurant(name="Nicholas's")
        ])
        self.assertQuerysetEqual(Restaurant.objects.all(), [
            "Nicholas's",
        ], attrgetter("name"))
        with self.assertRaises(ValueError):
            Pizzeria.objects.bulk_create([
                Pizzeria(name="The Art of Pizza")
            ])
        self.assertQuerysetEqual(Pizzeria.objects.all(), [])
        self.assertQuerysetEqual(Restaurant.objects.all(), [
            "Nicholas's",
        ], attrgetter("name"))

    def test_non_auto_increment_pk(self):
        State.objects.bulk_create([
            State(two_letter_code=s)
            for s in ["IL", "NY", "CA", "ME"]
        ])
        self.assertQuerysetEqual(State.objects.order_by("two_letter_code"), [
            "CA", "IL", "ME", "NY",
        ], attrgetter("two_letter_code"))