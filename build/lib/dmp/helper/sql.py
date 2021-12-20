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
from dmp.helper.utils import convert_int
import re
import string


def replace_template(text, params):
    """
    replace template params with given param
    	text: ex: select * from {table}
    	params: ex :{table: 'table_tempƒ'}
    	return:

    """
    if type(params) is dict:
        for k, v in params.items():
            text = text.replace('{%s}' % k, '%s' % v)
    return text



def clean_col_name(s):
    if type(s) is list or type(s) is tuple:
        s = '_'.join([clean_col_name(k) for k in s if clean_col_name(k) != ''])
        return s
    s = s.lower().strip()
    s = re.sub(u'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(u'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(u'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(u'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(u'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(u'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(u'[ìíịỉĩ]', 'i', s)
    s = re.sub(u'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(u'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(u'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(u'[ỳýỵỷỹ]', 'y', s)
    s = re.sub(u'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(u'[Đ]', 'D', s)
    s = re.sub(u'[đ]', 'd', s)
    # punc
    s = re.sub(r'[%s-–]' % (string.punctuation.replace("_", "")), '', s)
    s = re.sub(r'\s+', '_', s).strip()
    if (convert_int(s) > 0):
        s = ("c_%s" % s)
    return s

