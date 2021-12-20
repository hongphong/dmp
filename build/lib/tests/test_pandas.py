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

import random

from dmp.helper.utils import convert_time
from dmp.helper.utils import render_date_range

if __name__ == "__main__":
    # from dmp.df.pandas import PandasPipeline
    # a = PandasPipeline.connect_db("hssk_mongo2")
    # pipe = a.read_mongodb("Account", "colombo10263", query={}, limit=100)
    from dmp.helper.connection import WfConnection

    WfConnection.remove_wf_connection("dw_oracle")
    WfConnection.save_wf_connection(
        connection_id="dw_oracle",
        host="10.255.41.164",
        login=("KGM_Report"),
        password=("Viettel!@#2021"),
        connection_type="oracle+cx_oracle",
        extra={"service_name": "tcdb"}
    )

    t = WfConnection("dw_oracle").get_wf_db().get_sqlalchemy_engine()
    print(t)
