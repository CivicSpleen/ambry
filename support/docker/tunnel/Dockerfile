
FROM phusion/baseimage:0.9.17

MAINTAINER Eric Busboom "eric@civicknowledge.org"

EXPOSE 22

# Enable SSH. This is primarily for tunnels
RUN rm -f /etc/service/sshd/down
RUN /etc/my_init.d/00_regen_ssh_host_keys.sh

# Exec /bin/load key with the key as a parameter to load it in th /root/.ssh/authorized_keys
ADD loadkey /bin/loadkey
RUN chmod 775 /bin/loadkey

CMD /sbin/my_init