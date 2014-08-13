# Copyright 2011 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Functional test case that utilizes httplib2 against the API server"""

import hashlib
import os
import tempfile

import httplib2
import mock
import requests
import six

from glance import convertor
from glance.openstack.common import jsonutils
from glance.openstack.common import processutils
from glance.openstack.common import units
from glance.tests import functional

FIVE_MB = 5 * units.Mi


class TestConvertor(functional.FunctionalTest):

    def setUp(self):
        super(TestConvertor, self).setUp()
        self.cleanup()
        self.api_server.convert_image_to_raw = 'True'
        self.api_server.deployment_flavor = 'noauth'
        self.start_servers(**self.__dict__.copy())

        # prepare raw and vmdk contents
        self.image_data_raw = b"*" * FIVE_MB

        fd_vmdk, path_vmdk = tempfile.mkstemp()
        fd_raw, path_raw = tempfile.mkstemp()
        os.write(fd_raw, self.image_data_raw)
        os.close(fd_raw)

        processutils.execute("/usr/bin/qemu-img", "convert", "-O", "vmdk",
                             path_raw, path_vmdk)

        self.image_data_vmdk = os.read(fd_vmdk, os.path.getsize(path_vmdk))
        os.close(fd_vmdk)
        os.remove(path_vmdk)
        os.remove(path_raw)

    def _url(self, path):
        return 'http://127.0.0.1:%d%s' % (self.api_port, path)

    def _headers(self, custom_headers=None):
        base_headers = {
            'X-Identity-Status': 'Confirmed',
            'X-Auth-Token': '932c5c84-02ac-4fe5-a9ba-620af0e2bb96',
            'X-User-Id': 'f9a41d13-0c13-47e9-bee2-ce4e8bfe958e',
            'X-Tenant-Id': '932c5c84-02ac-4fe5-a9ba-620af0e2bb96',
            'X-Roles': 'member',
        }
        base_headers.update(custom_headers or {})
        return base_headers

    def test_api_v2_bare(self):
        self._do_test_api_v2('bare', 'raw', self.image_data_raw)

    def test_api_v2_ova(self):
        self._do_test_api_v2('ova', 'vmdk', self.image_data_vmdk)

    def _do_test_api_v2(self, container_format, expected_format,
                        expected_content):
        expected_size = len(expected_content)

        # Image list should be empty
        path = self._url('/v2/images')
        response = requests.get(path, headers=self._headers())
        self.assertEqual(200, response.status_code)
        images = jsonutils.loads(response.text)['images']
        self.assertEqual(0, len(images))

        # Create an vmdk image
        path = self._url('/v2/images')
        headers = self._headers({'content-type': 'application/json'})
        data = jsonutils.dumps({'name': 'testing',
                                'disk_format': 'vmdk',
                                'container_format': container_format})
        response = requests.post(path, headers=headers, data=data)
        self.assertEqual(201, response.status_code)
        image = jsonutils.loads(response.text)
        image_id = image['id']

        # upload data
        path = self._url('/v2/images/%s/file' % image_id)
        headers = self._headers({'Content-Type': 'application/octet-stream'})
        response = requests.put(path, headers=headers,
                                data=self.image_data_vmdk)
        self.assertEqual(204, response.status_code)

        # get info
        path = self._url('/v2/images/%s' % image_id)
        response = requests.get(path, headers=self._headers())
        self.assertEqual(200, response.status_code)

        data = response.json()

        expected_image_headers = {
            'id': image_id,
            'name': 'testing',
            'status': 'active',
            'disk_format': expected_format,
            'container_format': container_format,
            'size': expected_size}

        for expected_key, expected_value in expected_image_headers.items():
            self.assertEqual(data[expected_key], expected_value,
                             "For key '%s' expected header value '%s'. "
                             "Got '%s'" % (expected_key,
                                           expected_value,
                                           data[expected_key]))

        # get data
        path = self._url('/v2/images/%s/file' % image_id)
        headers = self._headers({'Content-Type': 'application/octet-stream'})
        response = requests.get(path, headers=headers)
        self.assertEqual(200, response.status_code)

        expected_std_headers = {
            'content-type': 'application/octet-stream'}

        for expected_key, expected_value in expected_std_headers.items():
            self.assertEqual(response.headers[expected_key], expected_value,
                             "For key '%s' expected header value '%s'. "
                             "Got '%s'" % (expected_key,
                                           expected_value,
                                           response.headers[expected_key]))

        self.assertEqual(response.content, expected_content)
        self.assertEqual(hashlib.md5(response.content).hexdigest(),
                         hashlib.md5(expected_content).hexdigest())

        # Delete
        path = self._url('/v2/images/%s' % image_id)
        response = requests.delete(path, headers=self._headers())
        self.assertEqual(204, response.status_code)

    def test_api_v1_bare(self):
        self._do_test_api_v1('bare', 'raw', self.image_data_raw)

    def test_api_v1_ova(self):
        self._do_test_api_v1('ova', 'vmdk', self.image_data_vmdk)

    def _do_test_api_v1(self, container_format, expected_format,
                        expected_content):
        expected_size = len(expected_content)

        headers = self._headers({
            'Content-Type': 'application/octet-stream',
            'X-Image-Meta-Name': 'testing',
            'X-Image-Meta-disk_format': 'vmdk',
            'X-Image-Meta-container_format': container_format,
        })
        path = "http://%s:%d/v1/images" % ("127.0.0.1", self.api_port)
        http = httplib2.Http()
        response, content = http.request(path, 'POST', headers=headers,
                                         body=self.image_data_vmdk)
        self.assertEqual(response.status, 201)
        data = jsonutils.loads(content)
        image_id = data['image']['id']

        self.assertEqual(data['image']['checksum'],
                         hashlib.md5(expected_content).hexdigest())
        self.assertEqual(data['image']['size'], expected_size)
        self.assertEqual(data['image']['name'], "testing")

        # 3. HEAD image
        # Verify image found now
        path = "http://%s:%d/v1/images/%s" % ("127.0.0.1", self.api_port,
                                              image_id)
        http = httplib2.Http()
        response, content = http.request(path, 'HEAD', headers=self._headers())
        self.assertEqual(response.status, 200)
        self.assertEqual(response['x-image-meta-name'], "testing")

        # 4. GET image
        # Verify all information on image we just added is correct
        path = "http://%s:%d/v1/images/%s" % ("127.0.0.1", self.api_port,
                                              image_id)
        http = httplib2.Http()
        response, content = http.request(path, 'GET', headers=self._headers())
        self.assertEqual(response.status, 200)

        expected_image_headers = {
            'x-image-meta-id': image_id,
            'x-image-meta-name': 'testing',
            'x-image-meta-status': 'active',
            'x-image-meta-disk_format': expected_format,
            'x-image-meta-container_format': container_format,
            'x-image-meta-size': str(expected_size)}

        expected_std_headers = {
            'content-length': str(expected_size),
            'content-type': 'application/octet-stream'}

        for expected_key, expected_value in expected_image_headers.items():
            self.assertEqual(response[expected_key], expected_value,
                             "For key '%s' expected header value '%s'. "
                             "Got '%s'" % (expected_key,
                                           expected_value,
                                           response[expected_key]))

        for expected_key, expected_value in expected_std_headers.items():
            self.assertEqual(response[expected_key], expected_value,
                             "For key '%s' expected header value '%s'. "
                             "Got '%s'" % (expected_key,
                                           expected_value,
                                           response[expected_key]))

        self.assertEqual(content, expected_content)
        self.assertEqual(hashlib.md5(content).hexdigest(),
                         hashlib.md5(expected_content).hexdigest())

        path = "http://%s:%d/v1/images/%s" % ("127.0.0.1", self.api_port,
                                              image_id)
        http = httplib2.Http()
        response, content = http.request(path, 'DELETE',
                                         headers=self._headers())
        self.assertEqual(response.status, 200)

    def test_unit_convert_vmdk(self):
        mock_src = tempfile.mkstemp()
        mock_dest = tempfile.mkstemp()

        with mock.patch('tempfile.mkstemp', side_effect=[mock_src,
                                                         mock_dest]):
            image_data_src = six.StringIO(self.image_data_vmdk)
            image_data, size = convertor.convert(image_data_src)
            self.assertEqual(self.image_data_raw, image_data.read())
            self.assertEqual(FIVE_MB, size)
            # Ensure file have been delete
            image_data.close()
            self.assertFalse(os.path.exists(mock_src[1]))
            self.assertFalse(os.path.exists(mock_dest[1]))

    def test_unit_convert_raw(self):
        mock_src = tempfile.mkstemp()
        mock_dest = tempfile.mkstemp()

        with mock.patch('tempfile.mkstemp', side_effect=[mock_src,
                                                         mock_dest]):

            image_data_src = six.StringIO(self.image_data_raw)
            image_data, size = convertor.convert(image_data_src)
            self.assertEqual(self.image_data_raw, image_data.read())
            self.assertEqual(FIVE_MB, size)
            # Ensure file have been deleted
            image_data.close()
            self.assertFalse(os.path.exists(mock_src[1]))
            self.assertFalse(os.path.exists(mock_dest[1]))
