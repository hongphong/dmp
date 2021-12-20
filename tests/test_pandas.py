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

from dmp.helper.utils import convert_time, convert_int, convert_float
from dmp.helper.utils import render_date_range

i = 20

# Create one byte from the integer 16
single_byte = i.to_bytes(1, byteorder='big', signed=True)
print(single_byte)

