# !/usr/bin/python
#
# Copyright Phong Pham Hong <phongpham1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__author__ = 'phongphamhong'

import json


def get_conf(selection, key, default='', d_type=''):
    from airflow.configuration import conf as main_setting
    try:
        rs = main_setting.conf.get(selection, key)
        if d_type == 'json':
            rs = json.loads(rs)
        return rs
    except BaseException as e:
        return default


class Setting:
    UPLOAD_FILE_TEMP = '/tmp/airflow_upload'
    SPARK_CONFIG_DEFAULT = 'spark_default_config'
    LOCAL_TIME_ZONE = 'asia/bangkok'
