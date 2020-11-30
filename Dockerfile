#
# dataCOllector agent
#
# This application grabs raw weather data from the Toulouse
# metropole weather station, process them and republish in
# proper topics.
#
# F.Thiebolt    apr.20  bash as default shell
# F.Thiebolt    jan.20  massive update
# Hamdi         2017    initial release
#

# Fedora 31 at the time of writing
FROM fedora:latest
MAINTAINER "Francois <thiebolt@irit.fr>"

# Switch to bash as default shell
SHELL [ "/bin/bash", "-c" ]

# Build ARGS
ARG APP="/app"

# Runtime & Build-time args
# Location of files to add to the container
# Note this <relative_path> to the build env @ host
ENV DOCKYARD="/dockyard" \
    DESTDIR="/opt/app"

# Switch to root user to enable installation (default)
#USER root

# Copy configuration directory
COPY ${DOCKYARD} ${DOCKYARD}

# Copy application directory
COPY ${APP} ${DESTDIR}

# passwd generated with 'openssl passwd' on CentOS 7.7

#
# Set-up ssh environnement / password / authorized-key + file copy
RUN echo -e "Starting setup ..." \
    # root passwd
    && echo 'root:$1$a00htwZE$G3wZbj4kezOIqXPfozuYV/' | chpasswd -e \
    # SSH authorized keys
    && cp -af ${DOCKYARD}/root / \
    && mkdir -p /root/.ssh \
    && cp -af ${DOCKYARD}/authorized_keys /root/.ssh/ \
    && chmod 600 /root/.ssh/authorized_keys \
    && chmod g-w /root \
    # custom motd message
    && cp -af ${DOCKYARD}/myMotd /etc/motd.d/ \
    && chmod 644 /etc/motd.d/myMotd \
    # Supervisor setup
    && cp -af ${DOCKYARD}/supervisord.d /etc \
    # system stuff
    && dnf -y --nogpgcheck install \
        git \
        gcc \
        procps \
        tmux \
        bind-utils \
        findutils \
        openssh-server \
        openssh-clients \
        supervisor \
        python3-pip \
        uwsgi \
        uwsgi-plugin-python3 \
        mailcap \
        python3-devel \
        python3-ipython \
#   && dnf -y --nogpgcheck --allowerasing upgrade \
    && dnf -y --nogpgcheck install vim \
    && dnf clean all \
    # Supervisor setup
    && cp -af /${DOCKYARD}/supervisord.d /etc \
    # [oct.20] fix for 'unable to hopen HTTP server'
    && mkdir -p /run/supervisor \
    # SSH server setup
    && mkdir -p /var/run/sshd \
    && ssh-keygen -A \
    # app. specific requirements
    && pip3 install -r ${DOCKYARD}/requirements.txt

#
# Ports for sshd and uwsgi application
EXPOSE 22 5000

# CMD
CMD [ "/usr/bin/supervisord", "-n" ]
#, "--loglevel=debug"]

