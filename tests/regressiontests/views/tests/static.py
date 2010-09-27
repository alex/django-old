import mimetypes
from os import path

from django.test import TestCase
from django.http import HttpResponseNotModified
from regressiontests.views.urls import media_dir

class StaticTests(TestCase):
    """Tests django views in django/views/static.py"""

    def test_serve(self):
        "The static view can serve static media"
        media_files = ['file.txt', 'file.txt.gz']
        for filename in media_files:
            response = self.client.get('/views/site_media/%s' % filename)
            file_path = path.join(media_dir, filename)
            self.assertEquals(open(file_path).read(), response.content)
            self.assertEquals(len(response.content), int(response['Content-Length']))
            self.assertEquals(mimetypes.guess_type(file_path)[1], response.get('Content-Encoding', None))

    def test_unknown_mime_type(self):
        response = self.client.get('/views/site_media/file.unknown')
        self.assertEquals('application/octet-stream', response['Content-Type'])

    def test_copes_with_empty_path_component(self):
        file_name = 'file.txt'
        response = self.client.get('/views/site_media//%s' % file_name)
        file = open(path.join(media_dir, file_name))
        self.assertEquals(file.read(), response.content)

    def test_is_modified_since(self):
        file_name = 'file.txt'
        response = self.client.get(
            '/views/site_media/%s' % file_name,
            HTTP_IF_MODIFIED_SINCE='Thu, 1 Jan 1970 00:00:00 GMT')
        file = open(path.join(media_dir, file_name))
        self.assertEquals(file.read(), response.content)

    def test_not_modified_since(self):
        file_name = 'file.txt'
        response = self.client.get(
            '/views/site_media/%s' % file_name,
            HTTP_IF_MODIFIED_SINCE='Mon, 18 Jan 2038 05:14:07 UTC'
            # This is 24h before max Unix time. Remember to fix Django and
            # update this test well before 2038 :)
            )
        self.assertTrue(isinstance(response, HttpResponseNotModified))

    def test_invalid_if_modified_since(self):
        """Handle bogus If-Modified-Since values gracefully

        Assume that a file is modified since an invalid timestamp as per RFC
        2616, section 14.25.
        """
        file_name = 'file.txt'
        invalid_date = 'Mon, 28 May 999999999999 28:25:26 GMT'
        response = self.client.get('/views/site_media/%s' % file_name,
                                   HTTP_IF_MODIFIED_SINCE=invalid_date)
        file = open(path.join(media_dir, file_name))
        self.assertEquals(file.read(), response.content)
        self.assertEquals(len(response.content),
                          int(response['Content-Length']))

