#
# Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

import json
import os
import re
import yaml
import hashlib
import shutil
from framework import Config
from framework import S3PyCliTest
from awss3api import AwsTest
from s3client_config import S3ClientConfig
from aclvalidation import AclTest
from auth import AuthTest
from ldap_setup import LdapInfo
# Helps debugging
# Config.log_enabled = True
# Config.dummy_run = True
# Config.client_execution_timeout = 300 * 1000
# Config.request_timeout = 300 * 1000
# Config.socket_timeout = 300 * 1000

# Transform AWS CLI text output into an object(dictionary)
# with content:
# {
#   "prefix":<list of common prefix>,
#   "keys": <list of regular keys>,
#   "next_token": <token>
# }
def get_aws_cli_object(raw_aws_cli_output):
    cli_obj = {}
    raw_lines = raw_aws_cli_output.split('\n')
    common_prefixes = []
    content_keys = []
    for _, item in enumerate(raw_lines):
        if (item.startswith("COMMONPREFIXES")):
            # E.g. COMMONPREFIXES  quax/
            line = item.split('\t')
            common_prefixes.append(line[1])
        elif (item.startswith("CONTENTS")):
            # E.g. CONTENTS\t"98b5e3f766f63787ea1ddc35319cedf7"\tasdf\t2020-09-25T11:42:54.000Z\t3072\tSTANDARD
            line = item.split('\t')
            content_keys.append(line[2])
        elif (item.startswith("NEXTTOKEN")):
            # E.g. NEXTTOKEN       eyJDb250aW51YXRpb25Ub2tlbiI6IG51bGwsICJib3RvX3RydW5jYXRlX2Ftb3VudCI6IDN9
            line = item.split('\t')
            cli_obj["next_token"] = line[1]
        else:
            continue

    if (common_prefixes is not None):
        cli_obj["prefix"] = common_prefixes
    if (content_keys is not None):
        cli_obj["keys"] = content_keys

    return cli_obj

# Extract the upload id from response which has the following format
# [bucketname    objecctname    uploadid]

def get_upload_id(response):
    key_pairs = response.split('\t')
    return key_pairs[2]

def get_etag(response):
    key_pairs = response.split('\t')
    return key_pairs[3]

def load_test_config():
    conf_file = os.path.join(os.path.dirname(__file__),'s3iamcli_test_config.yaml')
    with open(conf_file, 'r') as f:
            config = yaml.safe_load(f)
            S3ClientConfig.ldapuser = config['ldapuser']
            S3ClientConfig.ldappasswd = config['ldappasswd']

def get_response_elements(response):
    response_elements = {}
    key_pairs = response.split(',')

    for key_pair in key_pairs:
        tokens = key_pair.split('=')
        response_elements[tokens[0].strip()] = tokens[1].strip()

    return response_elements

# Run before all to setup the test environment.
print("Configuring LDAP")
S3PyCliTest('Before_all').before_all()

def create_object_list_file(file_name, obj_list=[], quiet_mode="false"):
    cwd = os.getcwd()
    file_to_create = os.path.join(cwd, file_name)
    objects = "{ \"Objects\": [ { \"Key\": \"" + obj_list[0] + "\" }"
    for obj in obj_list[1:]:
        objects += ", { \"Key\": \"" + obj + "\" }"
    objects += " ], \"Quiet\": " + quiet_mode + " }"
    with open(file_to_create, 'w') as file:
        file.write(objects)
    return file_to_create

def delete_object_list_file(file_name):
    os.remove(file_name)

def get_md5_sum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

#******** Create Bucket ********
AwsTest('Aws can create bucket').create_bucket("copyobjectbucket").execute_test().command_is_successful()

# creating dir for files.
upload_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "copy_object_test_upload")
os.makedirs(upload_dir, exist_ok=True)
filesize = [1024, 5120, 32768]
start = 0

# Create root level files under upload_dir, also calculating the checksum of files.
md5sum_dict_before_upload = {}
file_list_before_upload = ['1kfile', '5kfile', '32kfile']
for root_file in file_list_before_upload:
    file_to_create = os.path.join(upload_dir, root_file)
    key = root_file
    with open(file_to_create, 'wb+') as fout:
        fout.write(os.urandom(filesize[start]))
    md5sum_dict_before_upload[root_file] = get_md5_sum(file_to_create)
    start = start + 1

#Recursively upload all files from upload_dir to 'copyobjectbucket'.







#******** Bucket Versioning ********
bucket = "versionedbucket"
file = "1kfile"

#******** Create Bucket ********
AwsTest('Aws can create a bucket')\
    .create_bucket(bucket)\
    .execute_test()\
    .command_is_successful()

#******** Can't enable Versioning when an object exists ********
# XXX: Temporary until null versions are explicitly supported
AwsTest('Aws can put to a bucket')\
    .put_object(bucket, file, 1024)\
    .execute_test()\
    .command_is_successful()
AwsTest('Can not enable versioning on bucket with existing objects')\
    .put_bucket_versioning(bucket, "Enabled")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("OperationNotSupported")
AwsTest('Aws can delete the object')\
    .delete_object(bucket, file)\
    .execute_test()\
    .command_is_successful()

#******** Get Bucket Versioning default status ********
AwsTest('Aws can get bucket versioning default status')\
    .get_bucket_versioning(bucket)\
    .execute_test()\
    .command_response_should_be_empty()

#******** Enable Versioning on Bucket ********
AwsTest('Aws can enable versioning on bucket')\
    .put_bucket_versioning(bucket, "Enabled")\
    .execute_test()\
    .command_is_successful()

#******** Get Bucket Versioning Enabled status ********
AwsTest('Aws can get bucket versioning Enabled status')\
    .get_bucket_versioning(bucket)\
    .execute_test()\
    .command_response_should_have("Enabled")

#************ Negative case to get versioning status of non-existant bucket *******
AwsTest('Aws can not get versioning status of non-existant bucket')\
    .get_bucket_versioning("non-existant-bucket")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("NoSuchBucket")

#********** Negative case to check versioning status cannot be changed back to unversioned ***********
AwsTest('Aws can not change the versioning status back to unversioned')\
    .put_bucket_versioning(bucket, "Unversioned")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("MalformedXML")

#************ Negative case to enable versioning on non-existant bucket *******
AwsTest('Aws can not enable versioning on non-existant bucket')\
    .put_bucket_versioning("non-existant-bucket", "Enabled")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("NoSuchBucket")

#************ Put to a versioned bucket *******
result = AwsTest('Aws can put to a versioned bucket')\
    .put_object(bucket, file, 1024, output="json")\
    .execute_test()\
    .command_is_successful()
version_id = json.loads(result.status.stdout).get("VersionId")
assert version_id, "No version ID was returned in PutObject response"
assert re.match(r"[0-9A-Za-z]+", version_id)

#************ Get with version IDs *******
AwsTest('Aws can get object without specifing versionId')\
    .get_object(bucket, file).execute_test()\
    .command_is_successful()\
    .command_response_should_have(version_id)

AwsTest('Aws can get object with specific versionId')\
    .get_object(bucket, file, version_id=version_id)\
    .execute_test()\
    .command_is_successful()\
    .command_response_should_have(version_id)

AwsTest('Aws can not get object with wrong versionId')\
    .get_object(bucket, file, version_id="wrong-id")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("InvalidArgument")\
    .command_error_should_have("Invalid version id specified")

AwsTest('Aws can not get object with empty versionId')\
    .get_object(bucket, file, version_id="empty")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("InvalidArgument")\
    .command_error_should_have("Version id cannot be the empty string")

#************ Head object with version IDs *******
AwsTest('Aws can head object without specifing versionId')\
    .head_object(bucket, file).execute_test()\
    .command_is_successful()\
    .command_response_should_have(version_id)

result = AwsTest('Aws can put another version to a versioned bucket')\
    .put_object(bucket, file, 1024, output="json")\
    .execute_test()\
    .command_is_successful()
version_id2 = json.loads(result.status.stdout).get("VersionId")
assert version_id2, "No version ID was returned in PutObject response"
assert re.match(r"[0-9A-Za-z]+", version_id2)

result = AwsTest('Aws can delete latest version of the object')\
    .delete_object(bucket, file)\
    .execute_test()\
    .command_is_successful()
print("Result: [%s]." % (result))
version_id3 = json.loads(result.status.stdout).get("VersionId")
assert version_id3, "No version ID was returned in PutObject response"
#assert re.match(r"[0-9A-Za-z]+", version_id3)

AwsTest('Aws can head object with specific versionId')\
    .head_object(bucket, file, version_id=version_id)\
    .execute_test()\
    .command_is_successful()\
    .command_response_should_have(version_id)

AwsTest('Aws can not head object with wrong versionId')\
    .head_object(bucket, file, version_id="wrong-id")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("An error occurred (400) when calling the HeadObject operation")\
    .command_error_should_have("Bad Request")

AwsTest('Aws can not head object with empty versionId')\
    .head_object(bucket, file, version_id="empty")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("An error occurred (400) when calling the HeadObject operation")\
    .command_error_should_have("Bad Request")

AwsTest('Aws can not head object if latest is a delete marker')\
    .head_object(bucket, file)\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("Not Found")

AwsTest('Aws can head object if versionId is not a delete marker')\
    .head_object(bucket, file, version_id=version_id2)\
    .execute_test()\
    .command_is_successful()\
    .command_response_should_have(version_id2)

AwsTest('Aws can not head object if versionId is a delete marker')\
    .head_object(bucket, file, version_id=version_id3)\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("Method Not Allowed")

#******** Suspend Versioning on Bucket ********
# XXX: Temporarily disabled until suspension is supported
# AwsTest('Aws can suspend versioning on bucket')\
#     .put_bucket_versioning(bucket, "Suspended")\
#     .execute_test()\
#     .command_is_successful()

#******** Get Bucket Versioning Suspended status ********
# XXX: Temporarily disabled until suspension is supported
# AwsTest('Aws can get bucket versioning Suspended status')\
#     .get_bucket_versioning(bucket)\
#     .execute_test()\
#     .command_response_should_have("Suspended")

#******** Suspend Versioning on Bucket ********
# XXX: Temporary until suspension is supported
AwsTest('Can not suspend versioning on bucket')\
    .put_bucket_versioning(bucket, "Suspended")\
    .execute_test(negative_case=True)\
    .command_should_fail()\
    .command_error_should_have("OperationNotSupported")

AwsTest('Aws can delete the latest version of the object (DeleteMarker)')\
    .delete_object(bucket, file)\
    .execute_test()\
    .command_is_successful()

#******** Test Cleanup ********
# XXX: Temporarily disabled until deleting objects by version-id is supported
# AwsTest('Aws can delete all the object versions')\
#     .delete_object(bucket, file)\
#     .execute_test()\
#     .command_is_successful()

# XXX: Temporarily disabled until deleting objects by version-id is supported
# AwsTest('Aws can delete the bucket')\
#     .delete_bucket(bucket)\
#     .execute_test()\
#     .command_is_successful()


################################################################################

#************ Authorize copy-object ********************

