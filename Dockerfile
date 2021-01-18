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
FROM fedora:33

LABEL description="APImon (OpenStack API monitoring) container"
LABEL maintainer="Open Telekom Cloud (ecosystem)"

RUN dnf --disablerepo updates-modular --disablerepo fedora-modular \
    install -y git gcc nmap-ncat procps-ng net-tools \
    python3-devel python3-setuptools python3-pip \
    python3-psycopg2 python3-sqlalchemy \
    python3-dns && dnf clean all

RUN git config --global user.email "apimon@test.com"
RUN git config --global user.name "apimon"

RUN mkdir -p /var/{lib/apimon,log/apimon,log/executor,log/scheduler}

RUN useradd apimon

WORKDIR /usr/app

COPY ./requirements.txt /usr/app/requirements.txt

RUN \ 
     git clone https://github.com/opentelekomcloud/python-otcextensions \
--branch metrics && \
#    git clone https://github.com/ansible/ansible --branch stable-2.10 && \
     git clone https://review.opendev.org/openstack/openstacksdk

RUN pip3 install -r /usr/app/requirements.txt

#RUN cd ansible && python3 setup.py install --user
RUN cd openstacksdk && python3 setup.py install --force
RUN cd python-otcextensions && python3 setup.py install --force

ADD . /usr/app/apimon

# RUN cd openstacksdk \
#     && git fetch https://review.opendev.org/openstack/openstacksdk \
#     refs/changes/97/727097/7 \
#     && git checkout FETCH_HEAD \
#     && python3 setup.py install --user

RUN cd apimon && python3 setup.py install

RUN rm -rf /usr/app/{ansible,apimon,python-otcextensions}

USER apimon

ENV HOME=/home/apimon
