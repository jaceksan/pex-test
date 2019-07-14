FROM alpine:3.9

ENV TZ Europe/Prague
ENV DB_USER vertica
ENV DB_GROUP verticadba
ENV DB_NAME verticadb
ENV PYTHON_EGG_CACHE /tmp/.python-eggs
ENV IN_DOCKER=True

RUN echo "export TZ=${TZ}" > /etc/profile.d/vertica-tz.sh

RUN /usr/sbin/groupadd -r $DB_GROUP \
  && /usr/sbin/useradd -r -m -s /bin/bash -g $DB_GROUP $DB_USER \
  && su - $DB_USER -c "mkdir $PYTHON_EGG_CACHE"

RUN yum -q -y makecache \
  && yum install -y mcelog gdb sysstat iproute wget openssl

ARG v_ver=9.2.1
ARG v_rel=0
ARG vertica_package=vertica-${v_ver}-${v_rel}.x86_64.RHEL6.rpm

ADD ./rpm/${vertica_package} /tmp

RUN set -x \
  && yum install -y /tmp/${vertica_package} \
  && /opt/vertica/sbin/install_vertica --license CE --accept-eula --hosts 127.0.0.1 --dba-user-password-disabled --failure-threshold NONE \
    --no-system-configuration --point-to-point --ignore-aws-instance-type --ignore-install-config --no-ssh-key-install --dba-user $DB_USER --dba-group $DB_GROUP

# Cleanup to reduce image size
RUN set -x \
  && rm -f /tmp/${vertica_package} \
  && rm -rf /opt/vertica/lib64 \
  && rm -rf /opt/vertica/oss/python/lib/python2.7/test \
  && rm -rf /opt/vertica/oss/python3 \
  && yum clean all \
  && rm -rf /var/cache/yum

ADD ./docker-entrypoint.sh /opt/vertica/bin/

VOLUME /home/$DB_USER/$DB_NAME

ARG GIT_COMMIT=unspecified

LABEL image_name="Vertica database"
LABEL maintainer="jacek <zupabusta@gmail.com>"
LABEL git_repository_url="https://github.com/jaceksan/pex-test"
LABEL parent_image="centos:7"
LABEL git_commit=$GIT_COMMIT

ENTRYPOINT ["/opt/vertica/bin/docker-entrypoint.sh"]
EXPOSE 5433
