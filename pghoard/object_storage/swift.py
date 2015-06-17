"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import dateutil.parser
import time
from swiftclient import client  # pylint: disable=import-error
from .base import BaseTransfer


def fix_path(path):
    if path[0] != "/":
        path = "/" + path  # Swift seems to require a slash in the beginning
    return path


class SwiftTransfer(BaseTransfer):
    def __init__(self, username, key, authurl, tenant_name, container_name):
        BaseTransfer.__init__(self)
        self.username = username
        self.key = key
        self.authurl = authurl
        self.tenant_name = tenant_name
        self.container_name = container_name
        self.conn = client.Connection(user=self.username, key=self.key, authurl=self.authurl,
                                      tenant_name=self.tenant_name, auth_version="2.0")
        self.container = self.get_or_create_container(self.container_name)
        self.log.debug("SwiftTransfer initialized")

    def get_metadata_for_key(self, key):
        headers = self.conn.head_object(self.container_name, fix_path(key))
        return dict((header_key[len("x-object-meta-"):].replace("-", "_"), value) for header_key, value in headers.items()
                    if header_key.startswith("x-object-meta-"))

    def list_path(self, path):
        return_list = []
        _, results = self.conn.get_container(self.container_name, prefix=fix_path(path), delimiter="/")
        for r in results:
            return_list.append({
                "name": r["name"],
                "size": r["bytes"],
                "last_modified": dateutil.parser.parse(r["last_modified"]),
                "metadata": self.get_metadata_for_key(r["name"]),
                })
        return return_list

    def delete_key(self, key_name):
        self.log.debug("Deleting key: %r", key_name)
        return self.conn.delete_object(self.container_name, fix_path(key_name))

    def get_contents_to_file(self, obj_key, filepath_to_store_to):
        result_tuple = self.conn.get_object(self.container_name, fix_path(obj_key))
        with open(filepath_to_store_to, 'wb') as fp:
            # TODO: Figure out something that doesn't read the whole file into memory (swiftclient.service?)
            fp.write(result_tuple[1])

    def get_contents_to_string(self, obj_key):
        headers, data = self.conn.get_object(self.container_name, fix_path(obj_key))
        return data, dict((header_key[len("x-object-meta-"):].replace("-", "_"), value) for header_key, value in headers.items()
                          if header_key.startswith("x-object-meta-"))

    def store_file_from_memory(self, key, memstring, metadata=None):
        metadata_to_send = {}
        if metadata:
            metadata_to_send = dict(("x-object-meta-" + str(k), str(v)) for k, v in metadata.items())
        self.conn.put_object(self.container_name, key, contents=memstring, headers=metadata_to_send)

    def store_file_from_disk(self, key, filepath, metadata=None):
        metadata_to_send = {}
        if metadata:
            metadata_to_send = dict(("x-object-meta-" + str(k), str(v)) for k, v in metadata.items())
        with open(filepath, "rb") as fp:
            self.conn.put_object(self.container_name, key, contents=fp, headers=metadata_to_send)

    def get_or_create_container(self, container_name):
        start_time = time.time()
        self.conn.put_container(container_name, headers={})
        self.log.debug("Created container: %r successfully, took: %.3fs", container_name, time.time() - start_time)
        return container_name
