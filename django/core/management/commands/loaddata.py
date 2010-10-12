import os
import gzip
import zipfile
from optparse import make_option

from django.conf import settings
from django.core import serializers
from django.core.management.base import BaseCommand, CommandError
from django.core.management.color import no_style
from django.db import connections, router, transaction, DEFAULT_DB_ALIAS
from django.db.models import get_apps
from django.utils.itercompat import product

try:
    import bz2
    has_bz2 = True
except ImportError:
    has_bz2 = False


class SingleZipReader(zipfile.ZipFile):
    def __init__(self, *args, **kwargs):
        zipfile.ZipFile.__init__(self, *args, **kwargs)
        if settings.DEBUG:
            assert len(self.namelist()) == 1, "Zip-compressed fixtures must contain only one file."

    def read(self):
        return zipfile.ZipFile.read(self, self.namelist()[0])

def humanize(dirname):
    if dirname:
        return "'%s'" % dirname
    return 'absolute path'

def find_fixture_data(fixture_labels, verbosity, using, stdout):
    app_module_paths = []
    for app in get_apps():
        if hasattr(app, '__path__'):
            # It's a 'models/' subpackage
            app_module_paths.extend(app.__path__)
        else:
            # It's a models.py module
            app_module_paths.append(app.__file__)
    app_fixtures = [
        os.path.join(os.path.dirname(path), 'fixtures')
        for path in app_module_paths
    ]

    compression_types = {
        None:   file,
        'gz':   gzip.GzipFile,
        'zip':  SingleZipReader
    }
    if has_bz2:
        compression_types['bz2'] = bz2.BZ2File

    objs = []
    models = set()
    fixture_count = 0
    found_object_count = 0
    fixture_object_count = 0

    for fixture_label in fixture_labels:
        parts = fixture_label.split('.')

        if len(parts) > 1 and parts[-1] in compression_types:
            compression_formats = [parts[-1]]
            parts = parts[:-1]
        else:
            compression_formats = compression_types.keys()

        if len(parts) == 1:
            fixture_name = parts[0]
            formats = serializers.get_public_serializer_formats()
        else:
            fixture_name, format = '.'.join(parts[:-1]), parts[-1]
            if format in serializers.get_public_serializer_formats():
                formats = [format]
            else:
                formats = []

        if formats:
            if verbosity >= 2:
                stdout.write("Loading '%s' fixtures...\n" % fixture_name)
        else:
            raise CommandError("Problem installing fixture '%s': %s is not a "
                "known serialization format." % (fixture_name, format))

        if os.path.isabs(fixture_name):
            fixture_dirs = [fixture_name]
        else:
            fixture_dirs = app_fixtures + list(settings.FIXTURE_DIRS) + ['']

        for fixture_dir in fixture_dirs:
            if verbosity >= 2:
                stdout.write("Checking %s for fixtures...\n" % humanize(fixture_dir))

            label_found = False
            for combo in product([using, None], formats, compression_formats):
                database, format, compression_format = combo
                file_name = '.'.join(
                    p for p in [
                        fixture_name, database, format, compression_format
                    ]
                    if p
                )

                if verbosity >= 3:
                    stdout.write("Trying %s for %s fixture '%s'...\n" % \
                        (humanize(fixture_dir), file_name, fixture_name))
                full_path = os.path.join(fixture_dir, file_name)
                open_method = compression_types[compression_format]
                try:
                    fixture = open_method(full_path, 'r')
                except IOError:
                    if verbosity >= 2:
                        stdout.write("No %s fixture '%s' in %s.\n" % \
                            (format, fixture_name, humanize(fixture_dir)))
                    continue

                if label_found:
                    fixture.close()
                    raise CommandError("Multiple fixtures named '%s' in %s."
                        " Aborting" % (fixture_name, humanize(fixture_dir)))
                fixture_count += 1
                objects_in_fixture = 0
                found_objects_in_fixture = 0
                if verbosity >= 2:
                    stdout.write("Installing %s fixture '%s' from %s.\n" % \
                        (format, fixture_name, humanize(fixture_dir)))
                objects = serializers.deserialize(format, fixture, using=using)
                for obj in objects:
                    objects_in_fixture += 1
                    if router.allow_syncdb(using, obj.object.__class__):
                        found_objects_in_fixture += 1
                        models.add(obj.object.__class__)
                        objs.append(obj)
                found_object_count += found_objects_in_fixture
                fixture_object_count += objects_in_fixture
                label_found = True
                fixture.close()

                # If the fixture we loaded contains 0 objects, assume that an
                # error was encountered during fixture loading.
                if objects_in_fixture == 0:
                    raise CommandError("No fixture data found for '%s'. "
                        "(File format may be invalid.)" % fixture_name)
    return objs, models, fixture_object_count

class Command(BaseCommand):
    help = 'Installs the named fixture(s) in the database.'
    args = "fixture [fixture ...]"

    option_list = BaseCommand.option_list + (
        make_option('--database', action='store', dest='database',
            default=DEFAULT_DB_ALIAS, help='Nominates a specific database to load '
                'fixtures into. Defaults to the "default" database.'),
    )

    def handle(self, *fixture_labels, **options):
        using = options.get('database', DEFAULT_DB_ALIAS)

        connection = connections[using]
        self.style = no_style()

        verbosity = int(options.get('verbosity', 1))
        show_traceback = options.get('traceback', False)

        # commit is a stealth option - it isn't really useful as
        # a command line option, but it can be useful when invoking
        # loaddata from within another script.
        # If commit=True, loaddata will use its own transaction;
        # if commit=False, the data load SQL will become part of
        # the transaction in place when loaddata was invoked.
        commit = options.get('commit', True)

        # Get a cursor (even though we don't need one yet). This has
        # the side effect of initializing the test database (if
        # it isn't already initialized).
        cursor = connection.cursor()

        # Start transaction management. All fixtures are installed in a
        # single transaction to ensure that all references are resolved.
        if commit:
            transaction.commit_unless_managed(using=using)
            transaction.enter_transaction_management(using=using)
            transaction.managed(True, using=using)

        try:
            objs, models, fixture_object_count = find_fixture_data(
                fixture_labels,
                verbosity=verbosity,
                using=using,
                stdout=self.stdout
            )
        except CommandError:
            if commit:
                transaction.rollback(using=using)
                transaction.leave_transaction_management(using=using)
            raise
        for obj in objs:
            obj.save(using=using)

        # If we found even one object in a fixture, we need to reset the
        # database sequences.
        if objs:
            sequence_sql = connection.ops.sequence_reset_sql(self.style, models)
            if sequence_sql:
                if verbosity >= 2:
                    self.stdout.write("Resetting sequences\n")
                for line in sequence_sql:
                    cursor.execute(line)

        if commit:
            transaction.commit(using=using)
            transaction.leave_transaction_management(using=using)

        if fixture_object_count == 0:
            if verbosity >= 1:
                self.stdout.write("No fixtures found.\n")
        else:
            if verbosity >= 1:
                if fixture_object_count == loaded_object_count:
                    self.stdout.write("Installed %d object(s) from %d fixture(s)\n" % (
                        loaded_object_count, fixture_count))
                else:
                    self.stdout.write("Installed %d object(s) (of %d) from %d fixture(s)\n" % (
                        loaded_object_count, fixture_object_count, fixture_count))

        # Close the DB connection. This is required as a workaround for an
        # edge case in MySQL: if the same connection is used to
        # create tables, load data, and query, the query can return
        # incorrect results. See Django #7572, MySQL #37735.
        if commit:
            connection.close()
