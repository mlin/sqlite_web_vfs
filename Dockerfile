FROM ubuntu:20.04
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND noninteractive
ARG build_type=Release

RUN apt-get -qq update && apt-get -qq install -y --no-install-recommends --no-install-suggests \
     ca-certificates git-core build-essential cmake pkg-config \
     libcurl4-openssl-dev sqlite3 libsqlite3-dev libmicrohttpd-dev \
     python3-pytest pylint aria2

ADD . /work
WORKDIR /work

RUN rm -rf build && cmake -DCMAKE_BUILD_TYPE=$build_type . -B build && cmake --build build -j $(nproc)

WORKDIR /work/build
CMD ctest -V
