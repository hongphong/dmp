#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'

# !/usr/bin/python
#
# Copyright 11/9/18 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# # set enviroment in dev mode
from airflow.providers.oracle.hooks.oracle import OracleHook
from dmp.wf.hooks.dbi_hook import CustomDbApiHook
from urllib.parse import quote_plus

OracleHook.__bases__ = (CustomDbApiHook,)


class CusOracleHook(OracleHook):
    pass
