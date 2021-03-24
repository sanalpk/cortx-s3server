#!/usr/bin/env python3

import sys
import json
import uuid
import argparse

import plumbum
from plumbum import local
from typing import List, Optional


s3api = local["aws"]["s3api"]
OBJECT_SIZE = [0, 1, 2, 4095, 4096, 4097,
               2**20 - 1, 2**20, 2**20 + 100, 2 ** 24]
PART_SIZE = [5 * 2 ** 20, 6 * 2 ** 20]
PART_NR = [i+1 for i in range(2)]


def create_random_file(path: str, size: int, first_byte: Optional[str] = None):
    print(f'create_random_file path={path} size={size}')
    with open('/dev/urandom', 'rb') as r:
        with open(path, 'wb') as f:
            data = r.read(size)
            if first_byte:
                ba = bytearray(data)
                ba[0] = ord(first_byte[0])
                data = bytes(ba)
            f.write(data)

                       
def test_multipart_upload(bucket: str, key: str, output: str,
                          parts: List[str], get_must_fail: bool) -> None:
    create_multipart = json.loads(s3api["create-multipart-upload",
                                        "--bucket", bucket, "--key", key]())
    upload_id = create_multipart["UploadId"]
    print(create_multipart)
    i: int = 1
    for part in parts:
        print(s3api["upload-part", "--bucket", bucket, "--key", key,
                    "--part-number", str(i), "--upload-id", upload_id,
                    "--body", part]())
        i += 1
    parts_list = json.loads(s3api["list-parts",
                                  "--bucket", bucket, "--key", key,
                                  "--upload-id", upload_id]())
    print(parts_list)
    parts_file = {}
    parts_file["Parts"] = [{k: v for k, v in d.items()
                            if k in ["ETag", "PartNumber"]}
                           for d in parts_list["Parts"]]
    print(parts_file)
    with open('/tmp/parts.json', 'w') as f:
        f.write(json.dumps(parts_file))
    print(s3api["complete-multipart-upload", "--multipart-upload",
                "file:///tmp/parts.json", "--bucket", bucket, "--key", key,
                "--upload-id", upload_id]())
    s3api["get-object",  "--bucket", bucket, "--key", key, output]()
    (local['cat'][parts] |
     local['diff']['--report-identical-files', '-', output])()
    s3api["delete-object",  "--bucket", bucket, "--key", key]()
    

def test_put_get(bucket: str, key: str, body: str, output: str,
                 get_must_fail: bool) -> None:
    s3api["put-object",  "--bucket", bucket, "--key", key, '--body', body]()
    try:
        s3api["get-object",  "--bucket", bucket, "--key", key, output]()
        assert not get_must_fail
    except plumbum.commands.processes.ProcessExecutionError as e:
        print(123)
        print(e)
        assert get_must_fail
    (local['diff']['--report-identical-files', body, output])()
    s3api["delete-object",  "--bucket", bucket, "--key", key]()
    

def auto_test_put_get(args) -> None:
    first_byte = {'none': 'k', 'zero': 'z', 'first_byte': 'f'}[args.corruption]
    for size in OBJECT_SIZE:
        if args.create_objects:
            create_random_file(args.body, args.object_size, first_byte)
        for i in range(args.iterations):
            test_put_get(args.bucket, str(i), args.body, args.output,
                         args.corruption != 'none')


def auto_test_multipart(args) -> None:
    first_byte = {'none': 'k', 'zero': 'z', 'first_byte': 'f'}[args.corruption]
    for part_size in PART_SIZE:
        for last_part_size in OBJECT_SIZE:
            for part_nr in PART_NR:
                parts = [f'{args.body}.part{i+1}' for i in range(part_nr)]
                for i, part in enumerate(parts):
                    if args.create_objects:
                        create_random_file(part, part_size,
                                           first_byte if i == 0 else None)
                if last_part_size > 0:
                    parts += [f'{args.body}.last_part']
                    if args.create_objects:
                        create_random_file(parts[-1], last_part_size,
                                           first_byte)
                test_multipart_upload(args.bucket,
                                      f'part_size={part_size}_'
                                      f'last_part_size={last_part_size}_'
                                      f'part_nr={part_nr}_'
                                      f'uuid={uuid.uuid4()}',
                                      args.output, parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto-test-put-get', action='store_true')
    parser.add_argument('--auto-test-multipart', action='store_true')
    parser.add_argument('--bucket', type=str, default='test')
    parser.add_argument('--object-size', type=int, default=2 ** 20)
    parser.add_argument('--body', type=str, default='./s3-object.bin')
    parser.add_argument('--output', type=str, default='./s3-object-output.bin')
    parser.add_argument('--iterations', type=int, default=1)
    parser.add_argument('--create-objects', action='store_true')
    parser.add_argument('--corruption', choices=['none', 'zero', 'first_byte'],
                        default='none')
    args = parser.parse_args()
    print(args)
    if args.auto_test_put_get:
        auto_test_put_get(args)
    if args.auto_test_multipart:
        auto_test_multipart(args)


if __name__ == '__main__':
    main()