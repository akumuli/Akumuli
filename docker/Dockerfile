FROM ubuntu:14.04
RUN apt-get update; \
    apt-get -y upgrade

RUN apt-get -y install gdebi-core
	

COPY akumuli_*_amd64.deb /tmp/akumuli.deb
RUN gdebi -n /tmp/akumuli.deb

EXPOSE 8282
EXPOSE 8383/UDP
EXPOSE 8181
EXPOSE 4242

VOLUME /Akumuli

# Environment
ENV volume_size 4GB
ENV nvolumes 4

# Add Tini
ENV TINI_VERSION v0.9.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--"]

ADD akumulid /root/akumulid_template
ADD startup.sh /root/akumuli.sh

CMD ["/root/akumuli.sh"]
