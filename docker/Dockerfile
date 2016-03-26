 FROM ubuntu:14.04
 RUN apt-get update; \
     apt-get -y upgrade

 RUN apt-get install -y git \
    libboost-dev \
    libboost-system-dev \
    libboost-thread-dev \
    libboost-filesystem-dev \
    libboost-test-dev \
    libboost-coroutine-dev \
    libboost-context-dev \
    libboost-program-options-dev \
    libboost-regex-dev \
    libapr1-dev \
    libaprutil1-dev \
    libapr1-dev \
    libaprutil1-dev \
    libaprutil1-dbd-sqlite3 \
    libmicrohttpd-dev \
    liblog4cxx10-dev \
    liblog4cxx10 \
    libjemalloc-dev \
    libsqlite3-dev \
    cmake \
    build-essential

RUN git clone https://github.com/akumuli/Akumuli.git ; \
    cd Akumuli; \
    cmake -DCMAKE_INSTALL_PREFIX=/usr .; \
    make -j2 ; make install ; \
    cd .. ; \
    rm -R /Akumuli

EXPOSE 8282
EXPOSE 8383/UDP
EXPOSE 8181

VOLUME /Akumuli

# Add Tini
ENV TINI_VERSION v0.9.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--"]

ADD akumulid /root/.akumulid
ADD startup.sh /root/akumuli.sh

CMD ["/root/akumuli.sh"]