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

# Base image is pulled from the DHI Artifactory mirror (hardened, non-root
# capable) rather than the public upstream. The mirror host is passed in as a
# build arg (ARTIFACTORY_URL) so the build works both locally and in CI where
# the secret is injected.
ARG ARTIFACTORY_URL=artifactory.devops.telekom.de
FROM ${ARTIFACTORY_URL}/dhi.io/python:3.11-debian13-dev

LABEL description="StackMon component: APImon (OpenStack API monitoring) container"
LABEL maintainer="StackMon members"

ENV DEBIAN_FRONTEND=noninteractive
# PEP 668: Debian 13 marks the system Python as externally managed, so pip
# refuses to install into it without this.
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# Runtime + build dependencies (Debian 13 / trixie package names).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        gcc \
        ncat \
        procps \
        iproute2 \
        xz-utils \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-sqlalchemy \
        python3-dnspython \
        python3-psycopg2 \
        passwd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN git config --global user.email "apimon@test.com"
RUN git config --global user.name "apimon"

# Create a dedicated, non-root user with a real home directory (the container
# runs as this user at the end).
RUN useradd -m -d /home/apimon apimon

RUN mkdir -p /var/lib/apimon /var/log/apimon /var/log/executor /var/log/scheduler
RUN chown -R apimon:apimon /var/lib/apimon /var/log/apimon /var/log/executor /var/log/scheduler

WORKDIR /usr/app

COPY ./requirements.txt /usr/app/requirements.txt

RUN pip install --no-cache-dir --break-system-packages -r /usr/app/requirements.txt

ADD . /usr/app/apimon

RUN cd /usr/app/apimon && python3 setup.py install

USER apimon
ENV HOME=/home/apimon
