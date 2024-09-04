FROM quay.io/fedora/python-312
# Add application sources to a directory that the assemble script expects them
# and set permissions so that the container runs without root access
USER 0
ADD app-src /tmp/src
RUN /usr/bin/fix-permissions /tmp/src
USER 161

EXPOSE 8080

# Install the dependencies
RUN /usr/libexec/s2i/assemble #;\
#    patch /opt/app-root/lib/python3.9/site-packages/thrift/transport/THttpClient.py /opt/app-root/src/trace.patch

ENV APP_MODULE app:app

# Set the default command for the resulting image
CMD /usr/libexec/s2i/run
