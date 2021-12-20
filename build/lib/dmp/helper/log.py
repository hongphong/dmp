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

import logging.handlers
import time
import traceback

# import logging
# logger = logging.getLogger("airflow.task")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("[DMP]")
logger.setLevel(logging.INFO)
logging.Formatter.converter = time.gmtime
