FROM perconalab/percona-toolkit:3.2.0

LABEL maintainer="sunkai"

USER root
ENV TZ=Asia/Shanghai
RUN cd /etc/yum.repos.d \
    && sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* \
    && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* \
    && curl -s -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-vault-8.5.2111.repo \
    && yum clean all && yum makecache && yum install -y mysql jq \
    && yum clean all

# USER perconatoolkit

