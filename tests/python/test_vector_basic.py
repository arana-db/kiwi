# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
"""向量索引基础功能验证脚本。

运行前请确保 Kiwi 服务已启动（默认端口 7379）。
"""

import redis
import struct

DIM = 4
INDEX = 'vtest_idx'
PREFIX = 'vdoc:'
PORT = 7379


def pack_vec(values):
    """将 float 列表编码为 FLOAT32 小端字节序。"""
    return struct.pack(f'<{len(values)}f', *values)


def clean_docs(r):
    for i in range(1, 4):
        r.delete(f'{PREFIX}{i}')


def main():
    r = redis.Redis(host='localhost', port=PORT, decode_responses=True, protocol=2)
    r.ping()

    clean_docs(r)

    # FT.CREATE: 创建向量索引（已存在则忽略）
    try:
        r.execute_command(
            'FT.CREATE', INDEX, 'ON', 'HASH', 'PREFIX', '1', PREFIX,
            'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6',
            'TYPE', 'FLOAT32', 'DIM', str(DIM), 'DISTANCE_METRIC', 'L2'
        )
        print('FT.CREATE OK')
    except redis.ResponseError as e:
        if 'already exists' not in str(e).lower():
            raise
        print('FT.CREATE: index already exists')

    # HSET: 写入带向量字段的 Hash
    r.hset(f'{PREFIX}1', mapping={
        'vec': pack_vec([1.0, 0.0, 0.0, 0.0]),
        'name': 'alpha'
    })
    r.hset(f'{PREFIX}2', mapping={
        'vec': pack_vec([0.0, 1.0, 0.0, 0.0]),
        'name': 'beta'
    })
    r.hset(f'{PREFIX}3', mapping={
        'vec': pack_vec([0.0, 0.0, 1.0, 0.0]),
        'name': 'gamma'
    })
    print('HSET 3 docs OK')

    query = pack_vec([1.0, 0.0, 0.0, 0.0])

    # 先用 KNN 3 检查 3 个文档是否都被索引
    res_all = r.execute_command(
        'FT.SEARCH', INDEX, '*=>[KNN 3 @vec $q]',
        'PARAMS', '2', 'q', query,
        'RETURN', '1', 'name',
        'DIALECT', '2'
    )
    print('FT.SEARCH (all docs) result:', res_all)
    keys_all = {res_all[i] for i in range(1, len(res_all), 2)}
    assert keys_all == {f'{PREFIX}1', f'{PREFIX}2', f'{PREFIX}3'}, (
        f'expected 3 indexed docs, got {res_all[0]} hits with keys {keys_all}'
    )

    # KNN 2 检查 top-2 排序
    res = r.execute_command(
        'FT.SEARCH', INDEX, '*=>[KNN 2 @vec $q]',
        'PARAMS', '2', 'q', query,
        'RETURN', '1', 'name',
        'DIALECT', '2'
    )
    print('FT.SEARCH (top 2) result:', res)
    assert res[0] == 2, f'expected 2 hits, got {res[0]}'
    assert res[1] == f'{PREFIX}1', f'expected {PREFIX}1 as first hit, got {res[1]}'
    assert res[2] == ['name', 'alpha'], f"expected ['name', 'alpha'], got {res[2]}"
    assert res[4] in (['name', 'beta'], ['name', 'gamma'])

    # HDEL: 删除 doc1 的向量字段
    deleted = r.hdel(f'{PREFIX}1', 'vec')
    print('HDEL vec =>', deleted)
    assert deleted == 1, f'expected HDEL to delete 1 field, got {deleted}'

    # 再次检索，验证 doc1 不再参与向量搜索
    res2 = r.execute_command(
        'FT.SEARCH', INDEX, '*=>[KNN 3 @vec $q]',
        'PARAMS', '2', 'q', query,
        'RETURN', '1', 'name',
        'DIALECT', '2'
    )
    print('FT.SEARCH after HDEL result:', res2)

    assert res2[0] == 2, f'expected 2 hits after HDEL, got {res2[0]}'
    keys_after = {res2[i] for i in range(1, len(res2), 2)}
    assert keys_after == {f'{PREFIX}2', f'{PREFIX}3'}, f'unexpected keys: {keys_after}'

    clean_docs(r)
    print('all assertions passed')


if __name__ == '__main__':
    main()
