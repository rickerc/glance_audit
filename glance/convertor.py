# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010-2011 OpenStack, LLC
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

import os
import tempfile

from oslo.config import cfg

from glance.common import utils
import glance.domain.proxy
from glance.openstack.common import log as logging
from glance.openstack.common import processutils


policy_opts = [
    cfg.BoolOpt('convert_image_to_raw', default=False,
                help=_('Anything upload in glance in bare container '
                       'is automatically converted into a raw image.')),
    cfg.StrOpt('convert_image_to_raw_path',
               help=_('Temporary directory used to convert image to raw, '
                      'by default the system temporary direcotry is used.')),
]

CONF = cfg.CONF
CONF.register_opts(policy_opts)


LOG = logging.getLogger(__name__)


def safe_delete(path):
    try:
        os.remove(path)
    except Exception:
        LOG.warn("Fail to remove file: %s", path)


def safe_close(fd):
    try:
        os.close(fd)
    except Exception:
        pass


def convert(image_data):
    """The image to raw convertor, it does:

        - Read and put image_data into a temporary file
        - Create a copy of the file into raw format
        - Return a file object of this new raw file with the new file size
        - Also the glance v1/v2 image upload pipeline read this temporary
          instead of the streaming data
    """

    fd_src, src = tempfile.mkstemp(prefix="glance_convert_image_src",
                                   dir=CONF.convert_image_to_raw_path)
    fd_dest, dest = tempfile.mkstemp(prefix="glance_convert_image_dest",
                                     dir=CONF.convert_image_to_raw_path)
    try:
        for data in utils.chunkreadable(image_data):
            os.write(fd_src, data)
    finally:
        safe_close(fd_src)
        safe_close(fd_dest)

    cmd = ["/usr/bin/qemu-img", "convert", "-O", "raw", src, dest]
    processutils.execute(*cmd)
    safe_delete(src)

    # NOTE(sileht): we open the file fd and then delete the file
    # So the file exists until the fd is closed
    ret = file(dest), os.path.getsize(dest)
    safe_delete(dest)
    return ret


class ImageRepoProxy(glance.domain.proxy.Repo):

    def __init__(self, image_repo, context, db_api):
        self.image_repo = image_repo
        self.db_api = db_api
        proxy_kwargs = {'db_api': db_api, 'context': context}
        super(ImageRepoProxy, self).__init__(image_repo,
                                             item_proxy_class=ImageProxy,
                                             item_proxy_kwargs=proxy_kwargs)

    def save(self, image):
        # NOTE(sileht): the disk_format can be changed only in queued state
        if image.container_format == 'bare' and image.status == "queued":
            image.disk_format = 'raw'
        super(ImageRepoProxy, self).save(image)

    def add(self, image):
        if image.container_format == 'bare':
            image.disk_format = 'raw'
        super(ImageRepoProxy, self).add(image)


class ImageFactoryProxy(glance.domain.proxy.ImageFactory):
    def __init__(self, factory, context, db_api):
        proxy_kwargs = {'db_api': db_api, 'context': context}
        super(ImageFactoryProxy, self).__init__(factory,
                                                proxy_class=ImageProxy,
                                                proxy_kwargs=proxy_kwargs)


class ImageProxy(glance.domain.proxy.Image):

    def __init__(self, image, context, db_api):
        self.image = image
        self.context = context
        self.db_api = db_api
        super(ImageProxy, self).__init__(image)

    def set_data(self, data, size=None):
        try:
            if not self.image.container_format or \
                    self.image.container_format == 'bare':
                data, size = convert(utils.CooperativeReader(data))
                self.image.size = size
            self.image.set_data(data, size=size)
        except Exception:
            LOG.exception(_('Cleaning up %s after convertion fail.')
                          % self.image.image_id)
            location = self.image.locations[0]['url']
            glance.store.safe_delete_from_backend(
                location, self.context, self.image.image_id)
            raise
