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
from typing import Dict, List, Optional, Tuple

from airflow import configuration

__all__ = ["get_dag_folder", 'path']

def normalize_path(path: str) -> str:
    comps = (path or '/').split('/')
    result: List[str] = []
    for comp in comps:
        if comp in ('', '.'):
            pass
        elif comp != '..' or (result and result[-1] == '..'):
            result.append(comp)
        elif result:
            result.pop()
    return '/'.join(result)

def get_dag_folder():
    return '/' + normalize_path(configuration.conf.get('core', 'dags_folder'))


def path(path):
    return get_dag_folder() + '/' + str(path).lstrip('/').rstrip('/')
