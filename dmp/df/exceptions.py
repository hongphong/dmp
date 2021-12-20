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


class DfEmptyException(Exception):
    def __init__(self, message: str = "Your DataFrame is empty", *args):
        return super(DfEmptyException, self).__init__(message, *args)


class DfNotInitialized(Exception):
    def __init__(self, message: str = "DataFrame is not initialized", *args):
        return super(DfNotInitialized, self).__init__(message, *args)
