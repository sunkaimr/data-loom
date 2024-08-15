FROM ubuntu:22.04

LABEL maintainer="sunkai"

ENV TZ=Asia/Shanghai
RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone  \
    && apt-get update \
    && apt-get install -y tzdata dumb-init bash perl libdbi-perl libdbd-mysql-perl --no-install-recommends ca-certificates curl \
    && apt-get clean \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY config.yaml /opt/config.yaml
COPY data-loom /opt/data-loom
WORKDIR /opt/
EXPOSE 8080

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/opt/data-loom", "server"]
