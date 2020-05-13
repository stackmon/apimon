# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM fedora:32

RUN dnf --disablerepo updates-modular --disablerepo fedora-modular \
    install -y git gcc python3-devel python3-setuptools python3-pip nmap-ncat procps-ng

WORKDIR /usr/app
ENV PATH=/root/.local/bin:$PATH
RUN mkdir -p /var/{lib/apimon,log/apimon}

COPY ./requirements.txt /usr/app/requirements.txt

RUN git clone https://github.com/ansible/ansible --branch stable-2.9 && \
    git clone https://review.opendev.org/openstack/openstacksdk

RUN cd ansible && python3 setup.py install --user

ADD . /usr/app/apimon

RUN pip3 install --user -r /usr/app/requirements.txt

RUN cd openstacksdk \
    && git fetch https://review.opendev.org/openstack/openstacksdk \
    refs/changes/97/727097/3 \
    && git checkout FETCH_HEAD \
    && python3 setup.py install --user

RUN cd apimon && python3 setup.py install --user

ENV PATH=/root/.local/bin:$PATH

#CMD ["/usr/app/entrypoint.sh"]
